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
        tool: 'cba_rule_lookup',
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
        tool: 'cba_rule_lookup',
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
      tool: 'cba_rule_lookup',
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
        tool: 'player_salary_lookup',
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
        tool: 'player_salary_lookup',
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
        tool: 'player_salary_lookup',
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
      tool: 'player_salary_lookup',
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
      tool: 'top_paid_players',
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

function lookupApronDistance(engine) {
  const snapshot = engine.capSheet.snapshot;
  return {
    ok: true,
    kind: 'lookup',
    answer: `The Pacers are ${formatMoney(snapshot.distance_to_first_apron)} below the first apron. Current team salary: ${formatMoney(snapshot.team_salary)}. First apron: ${formatMoney(snapshot.first_apron)}.`,
    sources: stateSources(engine),
    toolPayload: {
      tool: 'apron_distance',
      team_salary: snapshot.team_salary,
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
  return {
    ok: true,
    kind: 'what_if',
    answer: [
      ...steps.map((step) => step.description),
      `New team salary: ${formatMoney(newTeamSalary)}`,
      `Distance to luxury tax: ${formatMoney(snapshot.luxury_tax_line - newTeamSalary)}`,
      `Distance to first apron: ${formatMoney(snapshot.first_apron - newTeamSalary)}`,
      `Distance to second apron: ${formatMoney(snapshot.second_apron - newTeamSalary)}`,
    ].join('\n'),
    sources: dedupeSources(sources),
    toolPayload: {
      tool: 'payroll_scenario',
      steps,
      base_team_salary: snapshot.team_salary,
      new_team_salary: newTeamSalary,
      luxury_tax_line: snapshot.luxury_tax_line,
      first_apron: snapshot.first_apron,
      second_apron: snapshot.second_apron,
      distance_to_tax: snapshot.luxury_tax_line - newTeamSalary,
      distance_to_first_apron: snapshot.first_apron - newTeamSalary,
      distance_to_second_apron: snapshot.second_apron - newTeamSalary,
      total_delta: delta,
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
    return lookupApronDistance(engine);
  }

  if (intent === 'payroll_addition') {
    const amount = Number(classification.amount);
    if (!Number.isFinite(amount)) {
      return {
        ok: false,
        kind: 'clarification',
        answer: 'What salary amount should I add for that scenario?',
        sources: stateSources(engine),
      };
    }
    return calculatePayrollScenario({ additions: [amount] }, engine);
  }

  if (intent === 'payroll_removal') {
    const removePlayers = [];
    if (classification.remove_player_query) {
      removePlayers.push(classification.remove_player_query);
    }
    const fixedRemovals = [];
    if (Number.isFinite(Number(classification.amount))) {
      fixedRemovals.push(Number(classification.amount));
    }
    if (!removePlayers.length && !fixedRemovals.length) {
      return {
        ok: false,
        kind: 'clarification',
        answer: 'What player or salary amount should I remove for that scenario?',
        sources: stateSources(engine),
      };
    }
    return calculatePayrollScenario({ fixedRemovals, removePlayers }, engine);
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

async function classifyIntentWithOpenAI(question) {
  const schema = {
    name: 'instant_gm_intent',
    strict: true,
    schema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        intent: {
          type: 'string',
          enum: [
            'player_salary_lookup',
            'top_paid_players',
            'apron_distance',
            'payroll_addition',
            'payroll_removal',
            'cba_rule_lookup',
            'mixed_payroll_and_rule_question',
            'unsupported_or_needs_clarification',
          ],
        },
        player_query: { type: ['string', 'null'] },
        top_n: { type: ['integer', 'null'] },
        amount: { type: ['number', 'null'] },
        add_amount: { type: ['number', 'null'] },
        remove_amount: { type: ['number', 'null'] },
        remove_player_query: { type: ['string', 'null'] },
        cba_rule_query: { type: ['string', 'null'] },
        legal_question: { type: 'boolean' },
        clarification_question: { type: ['string', 'null'] },
      },
      required: [
        'intent',
        'player_query',
        'top_n',
        'amount',
        'add_amount',
        'remove_amount',
        'remove_player_query',
        'cba_rule_query',
        'legal_question',
        'clarification_question',
      ],
    },
  };

  const messages = [
    {
      role: 'system',
      content: [
        'You classify Instant GM questions for a Pacers-only state engine.',
        'Interpret "we" and "our" as the Indiana Pacers.',
        'Choose exactly one intent.',
        'Use payroll_addition for adding salary only.',
        'Use payroll_removal for removing a player salary or removing a fixed salary amount.',
        'Use mixed_payroll_and_rule_question when a question combines payroll impact and a CBA rule or legal-style conclusion.',
        'Use cba_rule_lookup for general rule explanations.',
        'Use unsupported_or_needs_clarification when the question is outside current support or too vague to act on.',
        'Do not invent data. If the user is asking for a legal conclusion, set legal_question=true.',
        'If clarification is needed, return one short clarification question. Otherwise clarification_question should be null.',
      ].join(' '),
    },
    { role: 'user', content: question },
  ];

  const payload = await openAIChatCompletion(messages, {
    response_format: {
      type: 'json_schema',
      json_schema: schema,
    },
  });

  const content = payload?.choices?.[0]?.message?.content;
  if (typeof content !== 'string' || !content.trim()) {
    throw new Error('OpenAI classification returned no content.');
  }

  return JSON.parse(content);
}

async function explainWithOpenAI(question, classification, toolResults) {
  const sources = dedupeSources(toolResults.flatMap((result) => result.sources || []));
  const toolPayloads = toolResults.map((result) => result.toolPayload).filter(Boolean);

  const messages = [
    {
      role: 'system',
      content: [
        'You are Instant GM.',
        'Use only the supplied tool outputs.',
        'Do not invent salaries, rules, roster facts, thresholds, or legal conclusions.',
        'If tool outputs do not support a conclusion, say so plainly.',
        'Write a concise answer with these sections when applicable:',
        'Answer:',
        'Assumptions:',
        'Calculation:',
        'What I cannot conclude yet:',
        'Do not mention tools or hidden reasoning.',
      ].join(' '),
    },
    {
      role: 'user',
      content: JSON.stringify({
        question,
        classification,
        tool_results: toolPayloads,
      }),
    },
  ];

  const payload = await openAIChatCompletion(messages);
  const answer = payload?.choices?.[0]?.message?.content;
  if (typeof answer !== 'string' || !answer.trim()) {
    throw new Error('OpenAI explanation returned no content.');
  }

  return {
    answer: answer.trim(),
    sources,
  };
}

async function answerWithOpenAI(question, engine) {
  const classification = await classifyIntentWithOpenAI(question);

  if (classification.intent === 'unsupported_or_needs_clarification') {
    return runIntent(classification, question, engine);
  }

  const primaryResult = runIntent(classification, question, engine);
  if (!primaryResult.ok || primaryResult.kind === 'clarification') {
    return primaryResult;
  }

  const explainableIntents = new Set([
    'player_salary_lookup',
    'top_paid_players',
    'apron_distance',
    'payroll_addition',
    'payroll_removal',
    'mixed_payroll_and_rule_question',
    'cba_rule_lookup',
  ]);

  if (!explainableIntents.has(classification.intent)) {
    return primaryResult;
  }

  return explainWithOpenAI(question, classification, [primaryResult]);
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
