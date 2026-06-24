const assert = require('assert');

const askHandler = require('../api/ask.js');
const { findRelevantRecords, NO_DATA_ANSWER } = askHandler._internals;

function summarize(matches) {
  return matches.map(({ record, score }) => ({
    type: record.type,
    title: record.title,
    game_id: record.game_id,
    date: record.date,
    opponent: record.opponent,
    player: record.player || null,
    score,
    source_url: record.source_url || null
  }));
}

function expectTopMatch(result, expected) {
  assert(result.matches.length > 0, 'Expected at least one retrieved record.');
  const top = result.matches[0].record;
  for (const [key, value] of Object.entries(expected)) {
    assert.strictEqual(top[key], value, `Expected top match ${key}=${value}, got ${top[key]}`);
  }
}

const tests = [];

tests.push(() => {
  const result = findRelevantRecords('How many rebounds did Dale Davis have against the Nets on 1995-01-04?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'player-game',
    player: 'Dale Davis',
    date: '1995-01-04',
    opponent: 'NJ'
  });
  assert.strictEqual(result.matches[0].record.stats.REB, '8');
  assert(result.matches[0].record.source_url, 'Expected source URL.');
  return {
    name: 'Dale Davis rebounds vs Nets on 1995-01-04',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('What happened in the Pacers game on 2026-02-22?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'game',
    date: '2026-02-22',
    opponent: 'DAL'
  });
  assert.strictEqual(result.matches[0].record.result, 'L 130-134');
  return {
    name: 'Pacers game on 2026-02-22',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('What happened in the Pacers playoff game against Atlanta on 1995-04-27?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'game',
    date: '1995-04-27',
    opponent: 'ATL'
  });
  assert.strictEqual(result.matches[0].record.playoffs, true);
  return {
    name: 'Playoff game vs Atlanta on 1995-04-27',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const firstTurn = findRelevantRecords('How many rebounds did Dale Davis have against the Nets on 1995-01-04?', [], true);
  const history = [
    { role: 'user', content: 'How many rebounds did Dale Davis have against the Nets on 1995-01-04?' },
    { role: 'assistant', content: 'He had 8 rebounds.', resolvedContext: firstTurn.resolvedContext }
  ];
  const result = findRelevantRecords('What about his points in that game?', history, true);
  assert.strictEqual(result.classification, 'likely_supported_followup');
  expectTopMatch(result, {
    type: 'player-game',
    player: 'Dale Davis',
    date: '1995-01-04',
    opponent: 'NJ'
  });
  assert.strictEqual(result.matches[0].record.stats.PTS, '7');
  return {
    name: 'Follow-up: What about his points in that game?',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('What did the Pacers front office think about the draft in 1984?', [], true);
  assert.strictEqual(result.classification, 'unsupported');
  assert.strictEqual(result.matches.length, 0);
  return {
    name: 'Front office / draft opinion must refuse',
    result: 'pass',
    answer: NO_DATA_ANSWER,
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('What was the Pacers salary cap situation in 2014?', [], true);
  assert.strictEqual(result.classification, 'unsupported');
  assert.strictEqual(result.matches.length, 0);
  return {
    name: 'Salary cap question must refuse',
    result: 'pass',
    answer: NO_DATA_ANSWER,
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('What transaction brought T.J. McConnell to Indiana?', [], true);
  assert.strictEqual(result.classification, 'unsupported');
  assert.strictEqual(result.matches.length, 0);
  return {
    name: 'Transaction question must refuse',
    result: 'pass',
    answer: NO_DATA_ANSWER,
    retrieved: summarize(result.matches)
  };
});

const results = [];
for (const test of tests) {
  results.push(test());
}

console.log(JSON.stringify({ status: 'ok', results }, null, 2));
