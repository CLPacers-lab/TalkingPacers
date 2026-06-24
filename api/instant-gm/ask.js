const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(process.cwd(), 'instant-gm', 'data');
const ROSTER_PATH = path.join(DATA_DIR, 'pacers-roster.json');
const CONTRACTS_PATH = path.join(DATA_DIR, 'pacers-contracts.json');
const CAP_SHEET_PATH = path.join(DATA_DIR, 'pacers-cap-sheet.json');
const CBA_RULES_PATH = path.join(DATA_DIR, 'cba-rules.json');

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

function stateSources(engine) {
  const sources = [makeSource('Pacers cap page', engine.capSheet.metadata.source_url)];
  for (const url of engine.capSheet.metadata.threshold_sources || []) {
    if (!sources.find((source) => source.url === url)) {
      sources.push(makeSource('Threshold source', url));
    }
  }
  return sources;
}

function answerCbaRule(question, engine) {
  const lowered = normalizeText(question);
  const ruleTriggers = [
    'exception', 'mle', 'apron', 'aggregate', 'aggregation', 'stepien',
    'trade exception', 'traded player exception', 'two way', 'two-way',
    'roster limit', 'hard cap', 'hard capped', 'newly signed', 'recently signed',
    'december 15', 'january 15'
  ];

  if (!ruleTriggers.some((term) => lowered.includes(term)) && !(lowered.includes('rule') && lowered.includes('trade'))) {
    return null;
  }

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
      answer: 'I could not find a vetted rule for that question in the current Instant GM CBA library yet.',
      sources: [makeSource(engine.cbaRules.metadata.source_label)],
    };
  }

  const best = scored[0].record;
  if (best.confidence === 'needs_manual_review') {
    return {
      answer: `I could not find a vetted ${best.title} record in the current Instant GM CBA library yet. This topic needs manual review before I should treat it as reliable.`,
      sources: [makeSource(best.source_label || engine.cbaRules.metadata.source_label)],
    };
  }

  const citationBits = [];
  if (best.article) citationBits.push(best.article);
  if (best.section) citationBits.push(best.section);
  if (best.page !== null && best.page !== undefined) citationBits.push(`page ${best.page}`);

  const lines = [
    `${best.title}: ${best.plain_english_summary}`,
    citationBits.length ? `Citation: ${citationBits.join(', ')}.` : null,
    best.rule_text_excerpt ? `Excerpt: ${best.rule_text_excerpt}` : null,
    /\b(can we|can i|legally|allowed|is it legal)\b/.test(lowered)
      ? 'I can calculate the payroll impact, but I do not have the CBA rule engine needed to make that legal conclusion yet.'
      : 'This is a general rule explanation only. I am not applying it to a full transaction yet.',
  ].filter(Boolean);

  return {
    answer: lines.join('\n'),
    sources: [makeSource(best.source_label || engine.cbaRules.metadata.source_label)],
  };
}

function answerStateQuestion(question, engine) {
  const lowered = normalizeText(question);

  if (lowered.includes('first apron') && /\b(far|distance|how far)\b/.test(lowered)) {
    const snapshot = engine.capSheet.snapshot;
    return {
      answer: `The Pacers are ${formatMoney(snapshot.distance_to_first_apron)} below the first apron. Current team salary: ${formatMoney(snapshot.team_salary)}. First apron: ${formatMoney(snapshot.first_apron)}.`,
      sources: stateSources(engine),
    };
  }

  if (lowered.startsWith('what if') || lowered.includes('added') || lowered.includes('removed') || lowered.includes('remove ') || lowered.includes('cut ')) {
    let delta = null;
    let explanation = null;
    let sources = stateSources(engine);

    if (lowered.includes('added')) {
      const amount = parseMoneyPhrase(question);
      if (amount === null) {
        return { answer: 'I need the salary amount to run that add-salary scenario.', sources };
      }
      delta = amount;
      explanation = `Added ${formatMoney(amount)} to team salary.`;
    } else if (lowered.includes('cut ')) {
      const amount = parseMoneyPhrase(question);
      if (amount === null) {
        return { answer: 'I need the salary amount to run that cut-salary scenario.', sources };
      }
      delta = -amount;
      explanation = `Removed ${formatMoney(amount)} from team salary.`;
    } else {
      const resolved = resolvePlayer(question, engine.players, engine.aliases);
      if (!resolved.player) {
        return {
          answer: resolved.options.length
            ? `I need clarification on which player you mean: ${resolved.options.join(', ')}.`
            : 'I could not match that player to the current Pacers roster.',
          sources,
        };
      }
      const record = engine.contractByPlayer.get(resolved.player);
      if (!record || record.salary === null || record.salary === undefined) {
        return {
          answer: `I found ${resolved.player}, but I do not have a verified current-season salary for that player yet, so I cannot run the payroll what-if.`,
          sources: record ? [makeSource(resolved.player, record.source_url)] : sources,
        };
      }
      delta = -record.salary;
      explanation = `Removed ${resolved.player}'s salary of ${formatMoney(record.salary)}.`;
      sources = [makeSource(resolved.player, record.source_url), ...sources];
    }

    const snapshot = engine.capSheet.snapshot;
    const teamSalary = snapshot.team_salary + delta;
    return {
      answer: [
        explanation,
        `New team salary: ${formatMoney(teamSalary)}`,
        `Distance to luxury tax: ${formatMoney(snapshot.luxury_tax_line - teamSalary)}`,
        `Distance to first apron: ${formatMoney(snapshot.first_apron - teamSalary)}`,
        `Distance to second apron: ${formatMoney(snapshot.second_apron - teamSalary)}`,
      ].join('\n'),
      sources,
    };
  }

  if (/\b(highest paid|top paid|highest-paid|five highest paid)\b/.test(lowered)) {
    const ranked = engine.contracts.records
      .filter((record) => typeof record.salary === 'number')
      .sort((a, b) => b.salary - a.salary)
      .slice(0, 5);

    return {
      answer: [
        'Here are the 5 highest-paid Pacers by current-season salary:',
        ...ranked.map((record, index) => `${index + 1}. ${record.player}: ${formatMoney(record.salary)}`),
      ].join('\n'),
      sources: ranked.map((record) => makeSource(record.player, record.source_url)),
    };
  }

  if (/\b(making|salary|paid|earn|earns)\b/.test(lowered)) {
    const resolved = resolvePlayer(question, engine.players, engine.aliases);
    if (!resolved.player) {
      return {
        answer: resolved.options.length
          ? `I need clarification on which player you mean: ${resolved.options.join(', ')}.`
          : 'I could not match that player to the current Pacers roster.',
        sources: [makeSource('Pacers contracts source', engine.contracts.metadata.source_url)],
      };
    }

    const record = engine.contractByPlayer.get(resolved.player);
    if (!record) {
      return {
        answer: `I do not have a contract record for ${resolved.player} in the current Pacers State Engine.`,
        sources: [makeSource('Pacers contracts source', engine.contracts.metadata.source_url)],
      };
    }
    if (record.salary === null || record.salary === undefined) {
      return {
        answer: `I do not have a verified current-season salary for ${resolved.player} yet. The contract row exists, but the salary is still unverified.`,
        sources: [makeSource(resolved.player, record.source_url)],
      };
    }

    return {
      answer: `${resolved.player} is making ${formatMoney(record.salary)} in ${record.season}.`,
      sources: [makeSource(resolved.player, record.source_url)],
    };
  }

  const cbaAnswer = answerCbaRule(question, engine);
  if (cbaAnswer) {
    return cbaAnswer;
  }

  if (/\b(can we trade|trade this pick|legally|legal|sign and trade|non taxpayer mle|taxpayer mle|aggregate salaries|trade exception|bi annual exception|stepien)\b/.test(lowered)) {
    return {
      answer: 'I can calculate the payroll impact, but I do not have the CBA rule engine needed to make that legal conclusion yet.',
      sources: stateSources(engine),
    };
  }

  return {
    answer: 'I can answer Pacers roster, salary, top-payroll, simple payroll what-if, and curated CBA rule questions from the current Instant GM data. I do not support that question yet.',
    sources: stateSources(engine),
  };
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
    const result = answerStateQuestion(question, engine);
    return res.status(200).json(result);
  } catch (error) {
    return res.status(500).json({
      error: 'Instant GM server error.',
      detail: error && error.message ? error.message : 'Unknown error.',
    });
  }
};
