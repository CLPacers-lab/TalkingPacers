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

tests.push(() => {
  const result = findRelevantRecords("What is TJ McConnell's highest steals game?", [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'player-best-performance',
    player: 'T.J. McConnell',
    date: '2021-03-04',
    opponent: 'CLE'
  });
  assert.strictEqual(result.matches[0].record.stats.STL, 10);
  return {
    name: "TJ McConnell's highest steals game",
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords("What is T.J. McConnell's highest assists game?", [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'player-best-performance',
    player: 'T.J. McConnell',
    date: '2021-05-16',
    opponent: 'TOR'
  });
  assert.strictEqual(result.matches[0].record.stats.AST, 17);
  return {
    name: "T.J. McConnell's highest assists game",
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('Who is the best rebounder for the Pacers?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'franchise-leaderboard',
    player: 'Jeff Foster'
  });
  assert.strictEqual(result.matches[0].record.stats.REB, 5528);
  return {
    name: 'Best rebounder for the Pacers',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('Who has the most assists for the Pacers?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'franchise-leaderboard',
    player: 'Mark Jackson'
  });
  assert.strictEqual(result.matches[0].record.stats.AST, 3608);
  return {
    name: 'Most assists for the Pacers',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('What is Tyrese highest assists game?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'player-best-performance',
    player: 'Tyrese Haliburton'
  });
  return {
    name: 'Tyrese shorthand resolves to Tyrese Haliburton',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('What is Haliburten highest assists game?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'player-best-performance',
    player: 'Tyrese Haliburton'
  });
  return {
    name: 'Misspelled Haliburton resolves correctly',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords("What is Reggie Miller's highest scoring game?", [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'player-best-performance',
    player: 'Reggie Miller',
    date: '2001-04-25',
    opponent: 'PHI'
  });
  assert.strictEqual(result.matches[0].record.stats.PTS, 41);
  return {
    name: "Reggie Miller's highest scoring game",
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('What did Paul George average in 2013-14?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'player-season-summary',
    player: 'Paul George',
    season: '2013-14'
  });
  assert.strictEqual(result.matches[0].record.regular.averages.PTS, 20.02);
  assert.strictEqual(result.matches[0].record.playoffs_summary.averages.PTS, 22.58);
  return {
    name: 'Paul George 2013-14 season averages',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

tests.push(() => {
  const result = findRelevantRecords('Did Tyrese Haliburton average more assists in the playoffs or regular season as a Pacer?', [], true);
  assert.strictEqual(result.classification, 'box_score_supported');
  expectTopMatch(result, {
    type: 'player-career-summary',
    player: 'Tyrese Haliburton'
  });
  assert.strictEqual(result.matches[0].record.regular.averages.AST, 10.09);
  assert.strictEqual(result.matches[0].record.playoffs_summary.averages.AST, 8.42);
  assert(result.matches[0].record.regular.averages.AST > result.matches[0].record.playoffs_summary.averages.AST);
  return {
    name: 'Tyrese Haliburton playoff vs regular assists comparison',
    result: 'pass',
    retrieved: summarize(result.matches)
  };
});

const results = [];
for (const test of tests) {
  results.push(test());
}

console.log(JSON.stringify({ status: 'ok', results }, null, 2));
