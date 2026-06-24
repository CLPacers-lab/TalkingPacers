const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(process.cwd(), 'instant-gm', 'data');
const ROSTER_PATH = path.join(DATA_DIR, 'pacers-roster.json');
const CONTRACTS_PATH = path.join(DATA_DIR, 'pacers-contracts.json');
const CAP_SHEET_PATH = path.join(DATA_DIR, 'pacers-cap-sheet.json');
const CBA_RULES_PATH = path.join(DATA_DIR, 'cba-rules.json');

const OPENAI_MODEL = process.env.OPENAI_INSTANT_GM_MODEL || 'gpt-4.1-mini';
const LEGAL_LIMITATION_MESSAGE =
  'I can calculate the payroll impact, but I do not have the CBA rule engine needed to make that legal conclusion yet.';
const EXCEPTION_RULE_CONFIG = {
  non_taxpayer_mid_level_exception: {
    aliases: ['non-taxpayer mle', 'non taxpayer mle', 'full mle', 'non-taxpayer mid-level exception', 'mid-level exception'],
    amount_type: 'percent_of_cap',
    percent_of_cap: 0.0912,
    threshold: 'first apron',
  },
  bi_annual_exception: {
    aliases: ['bi-annual exception', 'bi annual exception', 'bae'],
    amount_type: 'percent_of_cap',
    percent_of_cap: 0.0332,
    threshold: 'first apron',
  },
  room_exception: {
    aliases: ['room exception', 'room mle', 'mid-level exception for room teams'],
    amount_type: 'percent_of_cap',
    percent_of_cap: 0.05678,
    threshold: 'salary cap',
  },
};

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/'/g, '')
    .replace(/\./g, '')
    .replace(/-/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function formatMoney(amount) {
  if (amount === null || amount === undefined || Number.isNaN(Number(amount))) {
    return 'unknown';
  }
  return `$${Number(amount).toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
}

function parseMoneyPhrase(text) {
  const match = String(text).match(/\$?\s*([0-9]+(?:\.[0-9]+)?)\s*([mk]|million|thousand)?\b/i);
  if (!match) {
    return null;
  }

  let value = Number(match[1]);
  const suffix = String(match[2] || '').toLowerCase();
  if (suffix === 'm' || suffix === 'million') {
    value *= 1_000_000;
  } else if (suffix === 'k' || suffix === 'thousand') {
    value *= 1_000;
  }
  return Math.round(value);
}

function similarity(a, b) {
  const left = normalizeText(a);
  const right = normalizeText(b);
  if (!left || !right) {
    return 0;
  }
  if (left === right) {
    return 1;
  }

  const leftTokens = new Set(left.split(' '));
  const rightTokens = new Set(right.split(' '));
  let overlap = 0;
  for (const token of leftTokens) {
    if (rightTokens.has(token)) {
      overlap += 1;
    }
  }

  return overlap / Math.max(leftTokens.size, rightTokens.size);
}

function buildPlayerAliases(players) {
  const aliases = new Map();
  for (const player of players) {
    const normalized = normalizeText(player);
    const parts = normalized.split(' ').filter(Boolean);
    const values = new Set([normalized]);
    if (parts.length) {
      values.add(parts[0]);
      values.add(parts[parts.length - 1]);
      if (parts.length >= 2) {
        values.add(`${parts[0]} ${parts[parts.length - 1]}`);
        values.add(parts.slice(-2).join(' '));
      }
    }
    aliases.set(player, values);
  }
  return aliases;
}

function resolvePlayer(question, players, aliases) {
  const normalizedQuestion = normalizeText(question);
  const exact = [];

  for (const player of players) {
    const playerAliases = aliases.get(player) || new Set();
    for (const alias of playerAliases) {
      if (alias && (` ${normalizedQuestion} `.includes(` ${alias} `) || normalizedQuestion === alias)) {
        exact.push(player);
        break;
      }
    }
  }

  if (exact.length === 1) {
    return { player: exact[0], options: [] };
  }
  if (exact.length > 1) {
    return { player: null, options: exact.sort() };
  }

  const fuzzy = [];
  for (const player of players) {
    let best = 0;
    for (const alias of aliases.get(player) || []) {
      best = Math.max(best, similarity(normalizedQuestion, alias));
      for (const token of normalizedQuestion.split(' ')) {
        best = Math.max(best, similarity(token, alias));
      }
    }
    if (best >= 0.72) {
      fuzzy.push({ player, score: best });
    }
  }

  fuzzy.sort((a, b) => b.score - a.score);
  if (fuzzy.length === 1) {
    return { player: fuzzy[0].player, options: [] };
  }
  if (fuzzy.length > 1) {
    const top = fuzzy[0].score;
    const options = fuzzy.filter((item) => top - item.score <= 0.06).slice(0, 5).map((item) => item.player);
    return { player: options.length === 1 ? options[0] : null, options: options.length === 1 ? [] : options };
  }

  return { player: null, options: [] };
}

function createEngine() {
  const roster = loadJson(ROSTER_PATH);
  const contracts = loadJson(CONTRACTS_PATH);
  const capSheet = loadJson(CAP_SHEET_PATH);
  const cbaRules = loadJson(CBA_RULES_PATH);

  const players = roster.records.map((record) => record.player);
  const aliases = buildPlayerAliases(players);
  const contractByPlayer = new Map(contracts.records.map((record) => [record.player, record]));

  return {
    roster,
    contracts,
    capSheet,
    cbaRules,
    players,
    aliases,
    contractByPlayer,
  };
}

function makeSource(title, url = null, extra = {}) {
  return { title, url, ...extra };
}

function dedupeSources(sources) {
  const seen = new Set();
  const result = [];
  for (const source of sources || []) {
    if (!source) continue;
    const key = source.url ? `url:${source.url}` : `title:${source.title}`;
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(source);
  }
  return result;
}

function stateSources(engine) {
  const sources = [makeSource('Pacers cap page', engine.capSheet.metadata.source_url)];
  for (const url of engine.capSheet.metadata.threshold_sources || []) {
    if (!sources.find((source) => source.url === url)) {
      sources.push(makeSource('Threshold source', url));
    }
  }
  return dedupeSources(sources);
}

function buildPayrollSnapshot(baseTeamSalary, engine) {
  const snapshot = engine.capSheet.snapshot;
  return {
    team_salary: baseTeamSalary,
    luxury_tax_line: snapshot.luxury_tax_line,
    first_apron: snapshot.first_apron,
    second_apron: snapshot.second_apron,
    distance_to_tax: snapshot.luxury_tax_line - baseTeamSalary,
    distance_to_first_apron: snapshot.first_apron - baseTeamSalary,
    distance_to_second_apron: snapshot.second_apron - baseTeamSalary,
  };
}

function thresholdValueByName(name, engine) {
  const snapshot = engine.capSheet.snapshot;
  const normalized = normalizeText(name || '');
  if (!normalized) return null;
  if (normalized === 'salary cap' || normalized === 'cap') return snapshot.salary_cap;
  if (normalized === 'luxury tax' || normalized === 'tax' || normalized === 'luxury tax line') return snapshot.luxury_tax_line;
  if (normalized === 'first apron') return snapshot.first_apron;
  if (normalized === 'second apron') return snapshot.second_apron;
  return null;
}

function findCbaRule(query, engine) {
  const lowered = normalizeText(query);
  const scored = [];

  for (const record of engine.cbaRules.records) {
    let score = 0;
    const terms = [
      record.title,
      record.plain_english_summary,
      record.notes,
      ...(record.tags || []),
      ...(record.match_terms || []),
    ];

    for (const term of terms) {
      const normalized = normalizeText(term);
      if (!normalized) continue;
      if (lowered.includes(normalized)) {
        score += normalized.split(' ').length > 1 ? 6 : 2;
      } else {
        const overlap = normalized.split(' ').filter((token) => lowered.split(' ').includes(token)).length;
        score += overlap;
      }
    }

    if (score > 0) {
      scored.push({ record, score });
    }
  }

  scored.sort((a, b) => b.score - a.score);
  if (!scored.length) {
    return {
      ok: false,
      kind: 'lookup',
      answer: 'I could not find a vetted rule for that question in the current Instant GM CBA library yet.',
      sources: [makeSource(engine.cbaRules.metadata.source_label)],
      toolPayload: {
        tool: 'lookup_cba_rule',
        found: false,
        query,
      },
    };
  }

  const best = scored[0].record;
  if (best.confidence === 'needs_manual_review') {
    return {
      ok: false,
      kind: 'lookup',
      answer: `I could not find a vetted ${best.title} record in the current Instant GM CBA library yet. This topic needs manual review before I should treat it as reliable.`,
      sources: [makeSource(best.source_label || engine.cbaRules.metadata.source_label)],
      toolPayload: {
        tool: 'lookup_cba_rule',
        found: false,
        query,
        matched_rule_id: best.rule_id,
        confidence: best.confidence,
      },
    };
  }

  const citationBits = [];
  if (best.article) citationBits.push(best.article);
  if (best.section) citationBits.push(best.section);
  if (best.page !== null && best.page !== undefined) citationBits.push(`page ${best.page}`);

  return {
    ok: true,
    kind: 'lookup',
    answer: `${best.title}: ${best.plain_english_summary}`,
    sources: [makeSource(best.source_label || engine.cbaRules.metadata.source_label)],
    toolPayload: {
      tool: 'lookup_cba_rule',
      found: true,
      query,
      rule_id: best.rule_id,
      title: best.title,
      plain_english_summary: best.plain_english_summary,
      rule_text_excerpt: best.rule_text_excerpt || null,
      article: best.article || null,
      section: best.section || null,
      page: best.page ?? null,
      confidence: best.confidence,
      notes: best.notes || null,
      source_label: best.source_label || engine.cbaRules.metadata.source_label,
      citation: citationBits.join(', ') || null,
    },
  };
}

function findExceptionRule(query, engine) {
  const lowered = normalizeText(query);
  let bestConfig = null;
  let bestScore = 0;

  for (const [ruleId, config] of Object.entries(EXCEPTION_RULE_CONFIG)) {
    let score = 0;
    for (const alias of config.aliases) {
      const normalized = normalizeText(alias);
      if (lowered.includes(normalized)) {
        score += normalized.split(' ').length > 1 ? 6 : 2;
      } else {
        score += normalized.split(' ').filter((token) => lowered.split(' ').includes(token)).length;
      }
    }
    if (score > bestScore) {
      bestScore = score;
      bestConfig = { ruleId, config };
    }
  }

  if (!bestConfig) {
    return null;
  }

  const matchedRule = engine.cbaRules.records.find((record) => record.rule_id === bestConfig.ruleId);
  if (!matchedRule) {
    return null;
  }

  return { rule: matchedRule, config: bestConfig.config };
}

function getExceptionAmount(exceptionQuery, engine) {
  const matched = findExceptionRule(exceptionQuery, engine);
  if (!matched) {
    return {
      ok: false,
      kind: 'clarification',
      answer: 'I could not match that exception to a structured exception amount in the current Instant GM rules layer.',
      sources: [makeSource(engine.cbaRules.metadata.source_label)],
      toolPayload: {
        tool: 'get_exception_amount',
        found: false,
        exception_query: exceptionQuery,
      },
    };
  }

  const { rule, config } = matched;
  const salaryCap = engine.capSheet.snapshot.salary_cap;
  let firstYearAmount = null;

  if (config.amount_type === 'percent_of_cap') {
    firstYearAmount = Math.round(salaryCap * config.percent_of_cap);
  }

  if (!Number.isFinite(firstYearAmount)) {
    return {
      ok: false,
      kind: 'lookup',
      answer: `I matched ${rule.title}, but I do not have a structured amount formula for it yet.`,
      sources: [makeSource(rule.source_label || engine.cbaRules.metadata.source_label)],
      toolPayload: {
        tool: 'get_exception_amount',
        found: false,
        exception_query: exceptionQuery,
        rule_id: rule.rule_id,
      },
    };
  }

  return {
    ok: true,
    kind: 'lookup',
    answer: `${rule.title} projects to ${formatMoney(firstYearAmount)} for ${engine.capSheet.metadata.season} based on ${config.percent_of_cap * 100}% of the ${formatMoney(salaryCap)} salary cap.`,
    sources: [
      makeSource(rule.source_label || engine.cbaRules.metadata.source_label),
      ...stateSources(engine),
    ],
    toolPayload: {
      tool: 'get_exception_amount',
      found: true,
      exception_query: exceptionQuery,
      rule_id: rule.rule_id,
      title: rule.title,
      amount_type: config.amount_type,
      percent_of_cap: config.percent_of_cap ?? null,
      first_year_amount: firstYearAmount,
      threshold: config.threshold || null,
      season: engine.capSheet.metadata.season,
      salary_cap: salaryCap,
    },
  };
}

function lookupPlayerSalary(playerQuery, engine) {
  const resolved = resolvePlayer(playerQuery, engine.players, engine.aliases);
  if (!resolved.player) {
    return {
      ok: false,
      kind: 'clarification',
      answer: resolved.options.length
        ? `I need clarification on which player you mean: ${resolved.options.join(', ')}.`
        : 'I could not match that player to the current Pacers roster.',
      sources: [makeSource('Pacers contracts source', engine.contracts.metadata.source_url)],
      toolPayload: {
        tool: 'get_player_salary',
        found: false,
        player_query: playerQuery,
        options: resolved.options,
      },
    };
  }

  const record = engine.contractByPlayer.get(resolved.player);
  if (!record) {
    return {
      ok: false,
      kind: 'unsupported',
      answer: `I do not have a contract record for ${resolved.player} in the current Pacers State Engine.`,
      sources: [makeSource('Pacers contracts source', engine.contracts.metadata.source_url)],
      toolPayload: {
        tool: 'get_player_salary',
        found: false,
        player_query: playerQuery,
        resolved_player: resolved.player,
      },
    };
  }

  if (record.salary === null || record.salary === undefined) {
    return {
      ok: false,
      kind: 'lookup',
      answer: `I do not have a verified current-season salary for ${resolved.player} yet. The contract row exists, but the salary is still unverified.`,
      sources: [makeSource(resolved.player, record.source_url)],
      toolPayload: {
        tool: 'get_player_salary',
        found: false,
        player_query: playerQuery,
        resolved_player: resolved.player,
        season: record.season,
      },
    };
  }

  return {
    ok: true,
    kind: 'lookup',
    answer: `${resolved.player} is making ${formatMoney(record.salary)} in ${record.season}.`,
    sources: [makeSource(resolved.player, record.source_url)],
    toolPayload: {
      tool: 'get_player_salary',
      found: true,
      player_query: playerQuery,
      player: resolved.player,
      season: record.season,
      salary: record.salary,
      guaranteed_salary: record.guaranteed_salary ?? null,
      nonguaranteed_salary: record.nonguaranteed_salary ?? null,
      option_type: record.option_type ?? null,
      option_holder: record.option_holder ?? null,
      notes: record.notes || null,
    },
  };
}

function lookupTopPaidPlayers(limit, engine) {
  const ranked = engine.contracts.records
    .filter((record) => typeof record.salary === 'number')
    .sort((a, b) => b.salary - a.salary)
    .slice(0, limit);

  return {
    ok: true,
    kind: 'lookup',
    answer: [
      `Here are the ${limit} highest-paid Pacers by current-season salary:`,
      ...ranked.map((record, index) => `${index + 1}. ${record.player}: ${formatMoney(record.salary)}`),
    ].join('\n'),
    sources: ranked.map((record) => makeSource(record.player, record.source_url)),
    toolPayload: {
      tool: 'get_top_paid_players',
      count: limit,
      players: ranked.map((record) => ({
        player: record.player,
        salary: record.salary,
        season: record.season,
        source_url: record.source_url,
      })),
    },
  };
}

function getCapSnapshot(engine) {
  const snapshot = engine.capSheet.snapshot;
  return {
    ok: true,
    kind: 'lookup',
    answer: `The Pacers are ${formatMoney(snapshot.distance_to_first_apron)} below the first apron. Current team salary: ${formatMoney(snapshot.team_salary)}. First apron: ${formatMoney(snapshot.first_apron)}.`,
    sources: stateSources(engine),
    toolPayload: {
      tool: 'get_cap_snapshot',
      team_salary: snapshot.team_salary,
      salary_cap: snapshot.salary_cap,
      luxury_tax_line: snapshot.luxury_tax_line,
      first_apron: snapshot.first_apron,
      second_apron: snapshot.second_apron,
      distance_to_tax: snapshot.distance_to_tax,
      distance_to_first_apron: snapshot.distance_to_first_apron,
      distance_to_second_apron: snapshot.distance_to_second_apron,
    },
  };
}

function calculatePayrollScenario({ additions = [], fixedRemovals = [], removePlayers = [] }, engine) {
  const snapshot = engine.capSheet.snapshot;
  const sources = [...stateSources(engine)];
  const steps = [];
  let delta = 0;

  for (const amount of additions) {
    if (typeof amount !== 'number' || Number.isNaN(amount)) continue;
    delta += amount;
    steps.push({ type: 'add_salary', amount, description: `Added ${formatMoney(amount)}.` });
  }

  for (const amount of fixedRemovals) {
    if (typeof amount !== 'number' || Number.isNaN(amount)) continue;
    delta -= amount;
    steps.push({ type: 'remove_salary_amount', amount, description: `Removed ${formatMoney(amount)}.` });
  }

  for (const playerQuery of removePlayers) {
    const resolved = resolvePlayer(playerQuery, engine.players, engine.aliases);
    if (!resolved.player) {
      return {
        ok: false,
        kind: 'clarification',
        answer: resolved.options.length
          ? `I need clarification on which player you mean: ${resolved.options.join(', ')}.`
          : 'I could not match that player to the current Pacers roster.',
        sources,
        toolPayload: {
          tool: 'payroll_scenario',
          found: false,
          player_query: playerQuery,
          options: resolved.options,
        },
      };
    }

    const record = engine.contractByPlayer.get(resolved.player);
    if (!record || record.salary === null || record.salary === undefined) {
      return {
        ok: false,
        kind: 'clarification',
        answer: `I found ${resolved.player}, but I do not have a verified current-season salary for that player yet, so I cannot run the payroll what-if.`,
        sources: record ? [makeSource(resolved.player, record.source_url), ...sources] : sources,
        toolPayload: {
          tool: 'payroll_scenario',
          found: false,
          player_query: playerQuery,
          resolved_player: resolved.player,
        },
      };
    }

    delta -= record.salary;
    steps.push({
      type: 'remove_player_salary',
      player: resolved.player,
      amount: record.salary,
      description: `Removed ${resolved.player}'s salary of ${formatMoney(record.salary)}.`,
    });
    sources.unshift(makeSource(resolved.player, record.source_url));
  }

  const newTeamSalary = snapshot.team_salary + delta;
  const payroll = buildPayrollSnapshot(newTeamSalary, engine);
  return {
    ok: true,
    kind: 'what_if',
    answer: [
      ...steps.map((step) => step.description),
      `New team salary: ${formatMoney(payroll.team_salary)}`,
      `Distance to luxury tax: ${formatMoney(payroll.distance_to_tax)}`,
      `Distance to first apron: ${formatMoney(payroll.distance_to_first_apron)}`,
      `Distance to second apron: ${formatMoney(payroll.distance_to_second_apron)}`,
    ].join('\n'),
    sources: dedupeSources(sources),
    toolPayload: {
      tool: 'payroll_scenario',
      steps,
      base_team_salary: snapshot.team_salary,
      new_team_salary: payroll.team_salary,
      luxury_tax_line: payroll.luxury_tax_line,
      first_apron: payroll.first_apron,
      second_apron: payroll.second_apron,
      distance_to_tax: payroll.distance_to_tax,
      distance_to_first_apron: payroll.distance_to_first_apron,
      distance_to_second_apron: payroll.distance_to_second_apron,
      total_delta: delta,
    },
  };
}

function simulateAddSalary(amount, engine, options = {}) {
  if (!Number.isFinite(Number(amount))) {
    return {
      ok: false,
      kind: 'clarification',
      answer: 'What salary amount should I add for that scenario?',
      sources: stateSources(engine),
      toolPayload: {
        tool: 'simulate_add_salary',
        found: false,
      },
    };
  }

  const baseTeamSalary = Number.isFinite(Number(options.base_team_salary))
    ? Number(options.base_team_salary)
    : engine.capSheet.snapshot.team_salary;
  const payroll = buildPayrollSnapshot(baseTeamSalary + Number(amount), engine);

  return {
    ok: true,
    kind: 'what_if',
    answer: [
      `Added ${formatMoney(Number(amount))}.`,
      `New team salary: ${formatMoney(payroll.team_salary)}`,
      `Distance to luxury tax: ${formatMoney(payroll.distance_to_tax)}`,
      `Distance to first apron: ${formatMoney(payroll.distance_to_first_apron)}`,
      `Distance to second apron: ${formatMoney(payroll.distance_to_second_apron)}`,
    ].join('\n'),
    sources: stateSources(engine),
    toolPayload: {
      tool: 'simulate_add_salary',
      amount: Number(amount),
      base_team_salary: baseTeamSalary,
      ...payroll,
    },
  };
}

function simulateRemovePlayerSalary(playerQuery, engine, options = {}) {
  const resolved = resolvePlayer(playerQuery, engine.players, engine.aliases);
  if (!resolved.player) {
    return {
      ok: false,
      kind: 'clarification',
      answer: resolved.options.length
        ? `I need clarification on which player you mean: ${resolved.options.join(', ')}.`
        : 'I could not match that player to the current Pacers roster.',
      sources: [makeSource('Pacers contracts source', engine.contracts.metadata.source_url)],
      toolPayload: {
        tool: 'simulate_remove_player_salary',
        found: false,
        player_query: playerQuery,
        options: resolved.options,
      },
    };
  }

  const record = engine.contractByPlayer.get(resolved.player);
  if (!record || record.salary === null || record.salary === undefined) {
    return {
      ok: false,
      kind: 'clarification',
      answer: `I found ${resolved.player}, but I do not have a verified current-season salary for that player yet, so I cannot run the payroll what-if.`,
      sources: record ? [makeSource(resolved.player, record.source_url)] : [makeSource('Pacers contracts source', engine.contracts.metadata.source_url)],
      toolPayload: {
        tool: 'simulate_remove_player_salary',
        found: false,
        player_query: playerQuery,
        resolved_player: resolved.player,
      },
    };
  }

  const baseTeamSalary = Number.isFinite(Number(options.base_team_salary))
    ? Number(options.base_team_salary)
    : engine.capSheet.snapshot.team_salary;
  const payroll = buildPayrollSnapshot(baseTeamSalary - Number(record.salary), engine);

  return {
    ok: true,
    kind: 'what_if',
    answer: [
      `Removed ${resolved.player}'s salary of ${formatMoney(record.salary)}.`,
      `New team salary: ${formatMoney(payroll.team_salary)}`,
      `Distance to luxury tax: ${formatMoney(payroll.distance_to_tax)}`,
      `Distance to first apron: ${formatMoney(payroll.distance_to_first_apron)}`,
      `Distance to second apron: ${formatMoney(payroll.distance_to_second_apron)}`,
    ].join('\n'),
    sources: dedupeSources([makeSource(resolved.player, record.source_url), ...stateSources(engine)]),
    toolPayload: {
      tool: 'simulate_remove_player_salary',
      player_query: playerQuery,
      player: resolved.player,
      removed_salary: record.salary,
      base_team_salary: baseTeamSalary,
      ...payroll,
    },
  };
}

function simulateRemoveEachPlayer(targetThreshold, engine) {
  const ranked = engine.contracts.records
    .filter((record) => typeof record.salary === 'number')
    .map((record) => {
      const payroll = buildPayrollSnapshot(engine.capSheet.snapshot.team_salary - record.salary, engine);
      const thresholdValue = thresholdValueByName(targetThreshold, engine);
      return {
        player: record.player,
        removed_salary: record.salary,
        new_team_salary: payroll.team_salary,
        distance_to_tax: payroll.distance_to_tax,
        distance_to_first_apron: payroll.distance_to_first_apron,
        distance_to_second_apron: payroll.distance_to_second_apron,
        gets_below_target: thresholdValue === null ? null : payroll.team_salary <= thresholdValue,
        source_url: record.source_url,
      };
    })
    .sort((a, b) => b.removed_salary - a.removed_salary);

  return {
    ok: true,
    kind: 'lookup',
    answer: `Simulated removing each single player salary from the current Pacers payroll${targetThreshold ? ` against the ${targetThreshold}` : ''}.`,
    sources: ranked.slice(0, 10).map((record) => makeSource(record.player, record.source_url)),
    toolPayload: {
      tool: 'simulate_remove_each_player',
      target_threshold: targetThreshold || null,
      base_team_salary: engine.capSheet.snapshot.team_salary,
      candidates: ranked,
    },
  };
}

function simulateRoomCreationOptions({ target_threshold, required_room = 0, mode = 'single_player_removal', limit = 10 }, engine) {
  const thresholdValue = thresholdValueByName(target_threshold, engine);
  if (!Number.isFinite(thresholdValue)) {
    return {
      ok: false,
      kind: 'clarification',
      answer: 'I need a supported threshold such as salary cap, luxury tax, first apron, or second apron.',
      sources: stateSources(engine),
      toolPayload: {
        tool: 'simulate_room_creation_options',
        found: false,
        target_threshold,
      },
    };
  }

  if (mode !== 'single_player_removal') {
    return {
      ok: false,
      kind: 'unsupported',
      answer: 'I only support single-player removal room scans right now.',
      sources: stateSources(engine),
      toolPayload: {
        tool: 'simulate_room_creation_options',
        found: false,
        target_threshold,
        mode,
      },
    };
  }

  const currentTeamSalary = engine.capSheet.snapshot.team_salary;
  const requiredMaxTeamSalary = thresholdValue - Number(required_room || 0);
  const currentSlack = thresholdValue - currentTeamSalary;

  const candidates = engine.contracts.records
    .filter((record) => typeof record.salary === 'number')
    .map((record) => {
      const newTeamSalary = currentTeamSalary - record.salary;
      const roomAfterMove = thresholdValue - newTeamSalary;
      return {
        player: record.player,
        removed_salary: record.salary,
        new_team_salary: newTeamSalary,
        target_threshold,
        threshold_value: thresholdValue,
        required_room: Number(required_room || 0),
        required_max_team_salary: requiredMaxTeamSalary,
        qualifies: newTeamSalary <= requiredMaxTeamSalary,
        remaining_buffer_after_required_room: requiredMaxTeamSalary - newTeamSalary,
        room_after_move: roomAfterMove,
        source_url: record.source_url,
      };
    })
    .sort((a, b) => {
      if (a.qualifies !== b.qualifies) {
        return a.qualifies ? -1 : 1;
      }
      return a.removed_salary - b.removed_salary;
    });

  const qualifying = candidates.filter((candidate) => candidate.qualifies);
  const displayCandidates = (qualifying.length ? qualifying : candidates).slice(0, Math.max(1, Math.min(Number(limit) || 10, 20)));

  return {
    ok: true,
    kind: 'lookup',
    answer: qualifying.length
      ? `Found ${qualifying.length} single-player removal options that would create at least ${formatMoney(required_room)} of room below the ${target_threshold}.`
      : `No single-player removal creates ${formatMoney(required_room)} of room below the ${target_threshold} from the current snapshot.`,
    sources: displayCandidates.map((record) => makeSource(record.player, record.source_url)),
    toolPayload: {
      tool: 'simulate_room_creation_options',
      mode,
      target_threshold,
      threshold_value: thresholdValue,
      required_room: Number(required_room || 0),
      current_team_salary: currentTeamSalary,
      current_slack_to_threshold: currentSlack,
      required_max_team_salary: requiredMaxTeamSalary,
      qualifies_without_move: currentTeamSalary <= requiredMaxTeamSalary,
      qualifying_candidates: qualifying,
      displayed_candidates: displayCandidates,
    },
  };
}

function hasLegalConclusionLanguage(text) {
  return /\b(can we|can i|legally|legal|allowed|eligible|could we use)\b/.test(normalizeText(text));
}

function getFallbackIntent(question) {
  const lowered = normalizeText(question);
  if (lowered.includes('first apron') && /\b(far|distance|how far)\b/.test(lowered)) {
    return { intent: 'apron_distance' };
  }
  if (lowered.startsWith('what if') || lowered.includes('added') || lowered.includes('removed') || lowered.includes('remove ') || lowered.includes('cut ')) {
    if (lowered.includes('added')) {
      return { intent: 'payroll_addition', amount: parseMoneyPhrase(question) };
    }
    if (lowered.includes('cut ')) {
      return { intent: 'payroll_removal', amount: parseMoneyPhrase(question) };
    }
    return { intent: 'payroll_removal', remove_player_query: question };
  }
  if (/\b(highest paid|top paid|highest-paid|five highest paid)\b/.test(lowered)) {
    return { intent: 'top_paid_players', top_n: 5 };
  }
  if (/\b(making|salary|paid|earn|earns)\b/.test(lowered)) {
    return { intent: 'player_salary_lookup', player_query: question };
  }
  if (/\b(exception|mle|apron|aggregate|aggregation|stepien|trade exception|traded player exception|two way|two-way|roster limit|hard cap|hard capped|newly signed|recently traded)\b/.test(lowered)) {
    return { intent: 'cba_rule_lookup', cba_rule_query: question, legal_question: hasLegalConclusionLanguage(question) };
  }
  if (/\b(can we trade|trade this pick|legally|legal|sign and trade|non taxpayer mle|taxpayer mle|aggregate salaries|trade exception|bi annual exception|stepien)\b/.test(lowered)) {
    return { intent: 'unsupported_or_needs_clarification', legal_question: true };
  }
  return { intent: 'unsupported_or_needs_clarification' };
}

function answerStateQuestion(question, engine) {
  const fallback = getFallbackIntent(question);
  return runIntent(fallback, question, engine);
}

function runIntent(classification, originalQuestion, engine) {
  const intent = classification.intent;

  if (intent === 'player_salary_lookup') {
    return lookupPlayerSalary(classification.player_query || originalQuestion, engine);
  }

  if (intent === 'top_paid_players') {
    return lookupTopPaidPlayers(Math.max(1, Math.min(Number(classification.top_n) || 5, 10)), engine);
  }

  if (intent === 'apron_distance') {
    return getCapSnapshot(engine);
  }

  if (intent === 'payroll_addition') {
    return simulateAddSalary(Number(classification.amount), engine);
  }

  if (intent === 'payroll_removal') {
    if (classification.remove_player_query) {
      return simulateRemovePlayerSalary(classification.remove_player_query, engine);
    }
    const amount = Number(classification.amount);
    if (!Number.isFinite(amount)) {
      return {
        ok: false,
        kind: 'clarification',
        answer: 'What player or salary amount should I remove for that scenario?',
        sources: stateSources(engine),
      };
    }
    return calculatePayrollScenario({ fixedRemovals: [amount] }, engine);
  }

  if (intent === 'mixed_payroll_and_rule_question') {
    const additions = [];
    const fixedRemovals = [];
    const removePlayers = [];

    if (Number.isFinite(Number(classification.add_amount))) {
      additions.push(Number(classification.add_amount));
    }
    if (Number.isFinite(Number(classification.remove_amount))) {
      fixedRemovals.push(Number(classification.remove_amount));
    }
    if (classification.remove_player_query) {
      removePlayers.push(classification.remove_player_query);
    }

    const payrollResult = additions.length || fixedRemovals.length || removePlayers.length
      ? calculatePayrollScenario({ additions, fixedRemovals, removePlayers }, engine)
      : null;
    const ruleResult = classification.cba_rule_query
      ? findCbaRule(classification.cba_rule_query, engine)
      : null;

    if (payrollResult && !payrollResult.ok) {
      return payrollResult;
    }
    if (ruleResult && !ruleResult.ok && !payrollResult) {
      return ruleResult;
    }

    return {
      ok: true,
      kind: 'mixed',
      answer: [payrollResult?.answer, ruleResult?.answer, LEGAL_LIMITATION_MESSAGE].filter(Boolean).join('\n\n'),
      sources: dedupeSources([...(payrollResult?.sources || []), ...(ruleResult?.sources || [])]),
      toolPayload: {
        tool: 'mixed_payroll_and_rule_question',
        payroll: payrollResult ? payrollResult.toolPayload : null,
        rule: ruleResult ? ruleResult.toolPayload : null,
        legal_question: Boolean(classification.legal_question),
      },
    };
  }

  if (intent === 'cba_rule_lookup') {
    const ruleResult = findCbaRule(classification.cba_rule_query || originalQuestion, engine);
    if (classification.legal_question && ruleResult.ok) {
      ruleResult.toolPayload.legal_question = true;
      ruleResult.toolPayload.legal_limitations = LEGAL_LIMITATION_MESSAGE;
    }
    return ruleResult;
  }

  if (intent === 'unsupported_or_needs_clarification') {
    if (classification.clarification_question) {
      return {
        ok: false,
        kind: 'clarification',
        answer: classification.clarification_question.trim(),
        sources: [],
      };
    }
    if (classification.legal_question) {
      return {
        ok: false,
        kind: 'unsupported',
        answer: LEGAL_LIMITATION_MESSAGE,
        sources: stateSources(engine),
      };
    }
    return {
      ok: false,
      kind: 'unsupported',
      answer: 'I can answer Pacers roster, salary, top-payroll, simple payroll what-if, and curated CBA rule questions from the current Instant GM data. I do not support that question yet.',
      sources: stateSources(engine),
    };
  }

  return answerStateQuestion(originalQuestion, engine);
}

async function openAIChatCompletion(messages, options = {}) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY is not set.');
  }

  const body = {
    model: OPENAI_MODEL,
    messages,
    temperature: 0.1,
    ...options,
  };

  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify(body),
  });

  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    const message = payload?.error?.message || `OpenAI request failed with status ${response.status}.`;
    throw new Error(message);
  }

  return payload;
}

const OPENAI_TOOLS = [
  {
    type: 'function',
    function: {
      name: 'get_cap_snapshot',
      description: 'Get the current Pacers payroll snapshot including team salary, salary cap, tax line, first apron, second apron, and distances to each threshold.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {},
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'get_player_salary',
      description: 'Get the current-season Pacers salary record for one player.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          player_query: { type: 'string' },
        },
        required: ['player_query'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'get_top_paid_players',
      description: 'Get the highest-paid Pacers players by current-season salary.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          limit: { type: 'integer' },
        },
        required: ['limit'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'simulate_add_salary',
      description: 'Add salary to the current or provided Pacers team salary and return the updated payroll distances.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          amount: { type: 'number' },
          base_team_salary: { type: ['number', 'null'] },
        },
        required: ['amount', 'base_team_salary'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'simulate_remove_player_salary',
      description: 'Remove one Pacers player salary from the current or provided Pacers team salary and return the updated payroll distances.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          player_query: { type: 'string' },
          base_team_salary: { type: ['number', 'null'] },
        },
        required: ['player_query', 'base_team_salary'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'simulate_remove_each_player',
      description: 'Simulate removing each single Pacers player salary from the current payroll. Useful for questions like who could be removed to get below a threshold.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          target_threshold: {
            type: ['string', 'null'],
            description: 'Optional threshold name such as first apron, second apron, luxury tax, or salary cap.',
          },
        },
        required: ['target_threshold'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'lookup_cba_rule',
      description: 'Look up a curated CBA rule summary and citation from the local rules library.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          query: { type: 'string' },
        },
        required: ['query'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'get_exception_amount',
      description: 'Get the projected first-year amount of a supported cap exception from the current salary cap and the local CBA rules layer.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          exception_query: { type: 'string' },
        },
        required: ['exception_query'],
      },
    },
  },
  {
    type: 'function',
    function: {
      name: 'simulate_room_creation_options',
      description: 'Given a target threshold and required amount of room below that threshold, scan single-player salary removals and return which options create enough room.',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          target_threshold: { type: 'string' },
          required_room: { type: 'number' },
          mode: { type: 'string', enum: ['single_player_removal'] },
          limit: { type: 'integer' },
        },
        required: ['target_threshold', 'required_room', 'mode', 'limit'],
      },
    },
  },
];

function executeToolCall(name, args, engine) {
  if (name === 'get_cap_snapshot') {
    return getCapSnapshot(engine);
  }
  if (name === 'get_player_salary') {
    return lookupPlayerSalary(args.player_query, engine);
  }
  if (name === 'get_top_paid_players') {
    return lookupTopPaidPlayers(Math.max(1, Math.min(Number(args.limit) || 5, 15)), engine);
  }
  if (name === 'simulate_add_salary') {
    return simulateAddSalary(args.amount, engine, { base_team_salary: args.base_team_salary });
  }
  if (name === 'simulate_remove_player_salary') {
    return simulateRemovePlayerSalary(args.player_query, engine, { base_team_salary: args.base_team_salary });
  }
  if (name === 'simulate_remove_each_player') {
    return simulateRemoveEachPlayer(args.target_threshold, engine);
  }
  if (name === 'lookup_cba_rule') {
    const result = findCbaRule(args.query, engine);
    if (hasLegalConclusionLanguage(args.query || '')) {
      result.toolPayload.legal_limitations = LEGAL_LIMITATION_MESSAGE;
    }
    return result;
  }
  if (name === 'get_exception_amount') {
    return getExceptionAmount(args.exception_query, engine);
  }
  if (name === 'simulate_room_creation_options') {
    return simulateRoomCreationOptions(args, engine);
  }
  return {
    ok: false,
    kind: 'unsupported',
    answer: 'I do not have a trusted tool for that request yet.',
    sources: [],
    toolPayload: {
      tool: name,
      found: false,
    },
  };
}

async function answerWithOpenAI(question, engine) {
  const messages = [
    {
      role: 'system',
      content: [
        'You are Instant GM, a Pacers-only front office assistant.',
        'Interpret "we" and "our" as the Indiana Pacers.',
        'Use tools for facts, calculations, payroll changes, and CBA lookups.',
        'Do not invent salaries, roster facts, thresholds, or legal conclusions.',
        'If a question is ambiguous, ask one concise clarification question instead of guessing.',
        'If a question asks for a legal conclusion beyond the available tools, you may still call tools for payroll impact and rule summaries, but you must clearly say the legal conclusion is not yet supported.',
        'If a tool you would need does not exist, say so plainly.',
        'For room-creation questions, prefer get_exception_amount plus simulate_room_creation_options.',
        'For questions like who could we get rid of to use the full MLE, use get_exception_amount for the non-taxpayer MLE, then simulate_room_creation_options against the first apron, and optionally lookup_cba_rule for the governing rule.',
      ].join(' '),
    },
    { role: 'user', content: question },
  ];

  const toolResults = [];

  for (let round = 0; round < 4; round += 1) {
    const payload = await openAIChatCompletion(messages, {
      tools: OPENAI_TOOLS,
      tool_choice: 'auto',
    });

    const message = payload?.choices?.[0]?.message;
    if (!message) {
      throw new Error('OpenAI tool flow returned no message.');
    }

    const toolCalls = Array.isArray(message.tool_calls) ? message.tool_calls : [];
    if (!toolCalls.length) {
      const answer = typeof message.content === 'string' ? message.content.trim() : '';
      if (!answer) {
        throw new Error('OpenAI final response returned no content.');
      }
      return {
        answer,
        sources: dedupeSources(toolResults.flatMap((result) => result.sources || [])),
      };
    }

    messages.push({
      role: 'assistant',
      content: message.content || '',
      tool_calls: toolCalls,
    });

    for (const call of toolCalls) {
      const name = call?.function?.name;
      let args = {};

      try {
        args = call?.function?.arguments ? JSON.parse(call.function.arguments) : {};
      } catch (_error) {
        args = {};
      }

      const result = executeToolCall(name, args, engine);
      toolResults.push(result);

      messages.push({
        role: 'tool',
        tool_call_id: call.id,
        content: JSON.stringify({
          ok: result.ok,
          kind: result.kind,
          answer: result.answer,
          sources: result.sources,
          tool_result: result.toolPayload || null,
        }),
      });
    }
  }

  throw new Error('OpenAI tool flow exceeded the maximum number of rounds.');
}

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method not allowed.' });
  }

  try {
    const question = typeof req.body?.question === 'string' ? req.body.question.trim() : '';
    if (!question) {
      return res.status(400).json({ error: 'Question is required.' });
    }

    const engine = createEngine();
    const apiKey = process.env.OPENAI_API_KEY;
    let result;
    let mode = 'state-engine-fallback';
    let warning = null;

    if (apiKey) {
      try {
        result = await answerWithOpenAI(question, engine);
        mode = 'openai-assisted';
      } catch (error) {
        result = answerStateQuestion(question, engine);
        warning = `OpenAI assist unavailable. Fell back to deterministic state-engine mode: ${error.message || 'Unknown OpenAI error.'}`;
      }
    } else {
      result = answerStateQuestion(question, engine);
    }

    return res.status(200).json({
      answer: result.answer,
      sources: dedupeSources(result.sources || []),
      mode,
      warning,
    });
  } catch (error) {
    return res.status(500).json({
      error: 'Instant GM server error.',
      detail: error && error.message ? error.message : 'Unknown error.',
    });
  }
};
