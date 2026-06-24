const fs = require('fs');
const path = require('path');

const ROOT = process.cwd();
const BOX_SCORES_PATH = path.join(ROOT, 'data', 'pacers-boxscores.json');
const OUTPUTS = {
  career: path.join(ROOT, 'data', 'player-career-summaries.json'),
  season: path.join(ROOT, 'data', 'player-season-summaries.json'),
  leaderboards: path.join(ROOT, 'data', 'franchise-leaderboards.json'),
  performances: path.join(ROOT, 'data', 'best-performances.json'),
};

const NUMERIC_STATS = ['MIN', 'PTS', 'REB', 'AST', 'STL', 'BLK', 'TO', 'OREB', 'DREB', 'PF'];
const SHOOTING_STATS = ['FG', '3PT', 'FT'];
const LEADERBOARD_STATS = ['PTS', 'REB', 'AST', 'STL', 'BLK', '3PTM', 'FGM', 'MIN'];
const PERFORMANCE_STATS = ['PTS', 'REB', 'AST', 'STL', 'BLK', '3PTM', 'FGM', 'MIN'];
const PHASES = [
  { key: 'combined', label: 'combined' },
  { key: 'regular', label: 'regular season' },
  { key: 'playoffs', label: 'playoffs' }
];

function readGames() {
  return JSON.parse(fs.readFileSync(BOX_SCORES_PATH, 'utf8'));
}

function safeNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function parseShootingPair(value) {
  const match = String(value || '').trim().match(/^(-?\d+)-(-?\d+)$/);
  if (!match) {
    return { made: 0, attempted: 0 };
  }
  return {
    made: safeNumber(match[1]),
    attempted: safeNumber(match[2])
  };
}

function computeSeasonFromDate(date) {
  const [year, month] = String(date || '').split('-').map((part) => Number(part));
  if (!year || !month) {
    return '';
  }
  if (month >= 7) {
    return `${year}-${String((year + 1) % 100).padStart(2, '0')}`;
  }
  return `${year - 1}-${String(year % 100).padStart(2, '0')}`;
}

function createAccumulator() {
  return {
    games: 0,
    starts: 0,
    totals: Object.fromEntries(NUMERIC_STATS.map((stat) => [stat, 0])),
    shooting: {
      FG: { made: 0, attempted: 0 },
      '3PT': { made: 0, attempted: 0 },
      FT: { made: 0, attempted: 0 }
    },
    sourceUrls: new Set(),
    seasons: new Set(),
    gameDates: [],
    topGames: {},
  };
}

function cloneAccumulator(acc) {
  return JSON.parse(JSON.stringify({
    games: acc.games,
    starts: acc.starts,
    totals: acc.totals,
    shooting: acc.shooting,
    sourceUrls: [...acc.sourceUrls],
    seasons: [...acc.seasons],
    gameDates: acc.gameDates,
    topGames: acc.topGames
  }));
}

function ingestRecord(acc, game, playerRow) {
  if (playerRow.did_not_play) {
    return;
  }

  acc.games += 1;
  if (playerRow.starter) {
    acc.starts += 1;
  }
  acc.sourceUrls.add(game.recap_url);
  acc.seasons.add(game.season);
  acc.gameDates.push(game.date);

  for (const stat of NUMERIC_STATS) {
    acc.totals[stat] += safeNumber(playerRow.stats?.[stat]);
    if (!acc.topGames[stat] || safeNumber(playerRow.stats?.[stat]) > safeNumber(acc.topGames[stat].value)) {
      acc.topGames[stat] = {
        value: safeNumber(playerRow.stats?.[stat]),
        date: game.date,
        season: game.season,
        opponent: game.opponent,
        playoffs: game.playoffs === true,
        source_url: game.recap_url,
        game_id: game.game_id
      };
    }
  }

  for (const stat of SHOOTING_STATS) {
    const pair = parseShootingPair(playerRow.stats?.[stat]);
    acc.shooting[stat].made += pair.made;
    acc.shooting[stat].attempted += pair.attempted;
  }
}

function finalizeAccumulator(acc) {
  const games = acc.games || 0;
  const averages = {};
  for (const stat of NUMERIC_STATS) {
    averages[stat] = games > 0 ? Number((acc.totals[stat] / games).toFixed(2)) : 0;
  }

  const shooting = {
    FG: {
      made: acc.shooting.FG.made,
      attempted: acc.shooting.FG.attempted,
      pct: acc.shooting.FG.attempted > 0 ? Number((acc.shooting.FG.made / acc.shooting.FG.attempted).toFixed(3)) : 0
    },
    '3PT': {
      made: acc.shooting['3PT'].made,
      attempted: acc.shooting['3PT'].attempted,
      pct: acc.shooting['3PT'].attempted > 0 ? Number((acc.shooting['3PT'].made / acc.shooting['3PT'].attempted).toFixed(3)) : 0
    },
    FT: {
      made: acc.shooting.FT.made,
      attempted: acc.shooting.FT.attempted,
      pct: acc.shooting.FT.attempted > 0 ? Number((acc.shooting.FT.made / acc.shooting.FT.attempted).toFixed(3)) : 0
    }
  };

  return {
    games,
    starts: acc.starts,
    totals: acc.totals,
    averages,
    shooting,
    seasons: [...acc.seasons].sort(),
    first_game_date: acc.gameDates.slice().sort()[0] || null,
    last_game_date: acc.gameDates.slice().sort().slice(-1)[0] || null,
    source_urls: [...acc.sourceUrls].filter(Boolean).slice(0, 10),
    top_games: acc.topGames
  };
}

function createPhaseContainers() {
  return {
    combined: createAccumulator(),
    regular: createAccumulator(),
    playoffs: createAccumulator()
  };
}

function statSummaryLines(label, stats) {
  return [
    `${label}_games: ${stats.games}`,
    `${label}_starts: ${stats.starts}`,
    `${label}_points_total: ${stats.totals.PTS}`,
    `${label}_points_avg: ${stats.averages.PTS}`,
    `${label}_rebounds_total: ${stats.totals.REB}`,
    `${label}_rebounds_avg: ${stats.averages.REB}`,
    `${label}_assists_total: ${stats.totals.AST}`,
    `${label}_assists_avg: ${stats.averages.AST}`,
    `${label}_steals_total: ${stats.totals.STL}`,
    `${label}_steals_avg: ${stats.averages.STL}`,
    `${label}_blocks_total: ${stats.totals.BLK}`,
    `${label}_blocks_avg: ${stats.averages.BLK}`,
    `${label}_minutes_avg: ${stats.averages.MIN}`,
    `${label}_fg_pct: ${stats.shooting.FG.pct}`,
    `${label}_3pt_pct: ${stats.shooting['3PT'].pct}`,
    `${label}_ft_pct: ${stats.shooting.FT.pct}`
  ];
}

function buildCareerRecord(player, playerId, phases) {
  const combined = finalizeAccumulator(phases.combined);
  const regular = finalizeAccumulator(phases.regular);
  const playoffs = finalizeAccumulator(phases.playoffs);
  return {
    type: 'player-career-summary',
    title: `${player} career summary`,
    player,
    player_id: playerId,
    season: '',
    phase: 'career',
    playoffs: false,
    date: combined.last_game_date,
    opponent: '',
    source_url: combined.source_urls[0] || '',
    source_urls: combined.source_urls,
    stats: {
      PTS: combined.totals.PTS,
      REB: combined.totals.REB,
      AST: combined.totals.AST,
      STL: combined.totals.STL,
      BLK: combined.totals.BLK,
      MIN: combined.totals.MIN
    },
    combined,
    regular,
    playoffs_summary: playoffs,
    contextLines: [
      `player: ${player}`,
      `player_id: ${playerId}`,
      `seasons: ${combined.seasons.join(', ')}`,
      `first_game_date: ${combined.first_game_date || 'n/a'}`,
      `last_game_date: ${combined.last_game_date || 'n/a'}`,
      ...statSummaryLines('combined', combined),
      ...statSummaryLines('regular', regular),
      ...statSummaryLines('playoffs', playoffs),
      `sample_source_urls: ${combined.source_urls.join(', ') || 'unknown'}`
    ]
  };
}

function buildSeasonRecord(player, playerId, season, phases) {
  const combined = finalizeAccumulator(phases.combined);
  const regular = finalizeAccumulator(phases.regular);
  const playoffs = finalizeAccumulator(phases.playoffs);
  return {
    type: 'player-season-summary',
    title: `${player} ${season} season summary`,
    player,
    player_id: playerId,
    season,
    phase: 'season',
    playoffs: false,
    date: combined.last_game_date,
    opponent: '',
    source_url: combined.source_urls[0] || '',
    source_urls: combined.source_urls,
    stats: {
      PTS: combined.totals.PTS,
      REB: combined.totals.REB,
      AST: combined.totals.AST,
      STL: combined.totals.STL,
      BLK: combined.totals.BLK,
      MIN: combined.totals.MIN
    },
    combined,
    regular,
    playoffs_summary: playoffs,
    contextLines: [
      `player: ${player}`,
      `player_id: ${playerId}`,
      `season: ${season}`,
      ...statSummaryLines('combined', combined),
      ...statSummaryLines('regular', regular),
      ...statSummaryLines('playoffs', playoffs),
      `sample_source_urls: ${combined.source_urls.join(', ') || 'unknown'}`
    ]
  };
}

function statDisplayLabel(stat) {
  switch (stat) {
    case '3PTM':
      return 'three-pointers made';
    case 'FGM':
      return 'field goals made';
    default:
      return stat.toLowerCase();
  }
}

function getComputedStat(playerRow, stat) {
  if (stat === '3PTM') {
    return parseShootingPair(playerRow.stats?.['3PT']).made;
  }
  if (stat === 'FGM') {
    return parseShootingPair(playerRow.stats?.FG).made;
  }
  return safeNumber(playerRow.stats?.[stat]);
}

function getAccumulatorStat(summary, stat) {
  if (stat === '3PTM') {
    return summary.shooting['3PT'].made;
  }
  if (stat === 'FGM') {
    return summary.shooting.FG.made;
  }
  return summary.totals[stat] || 0;
}

function buildLeaderboardRecord(stat, phaseKey, leaders) {
  const phaseLabel = PHASES.find((phase) => phase.key === phaseKey)?.label || phaseKey;
  const top = leaders[0] || null;
  return {
    type: 'franchise-leaderboard',
    title: `${phaseLabel} ${statDisplayLabel(stat)} leaderboard`,
    player: top?.player || '',
    player_id: top?.player_id || '',
    season: '',
    phase: phaseKey,
    playoffs: phaseKey === 'playoffs',
    date: top?.last_game_date || null,
    opponent: '',
    stat,
    source_url: top?.source_urls?.[0] || '',
    source_urls: leaders.flatMap((leader) => leader.source_urls || []).filter(Boolean).slice(0, 10),
    stats: { [stat]: top?.value || 0 },
    leaders: leaders.slice(0, 10),
    contextLines: [
      `phase: ${phaseLabel}`,
      `stat: ${stat}`,
      ...leaders.slice(0, 10).map((leader) => `rank_${leader.rank}: ${leader.player} ${leader.value}`)
    ]
  };
}

function buildBestPerformanceRecord(mode, stat, phaseKey, player, performances) {
  const phaseLabel = PHASES.find((phase) => phase.key === phaseKey)?.label || phaseKey;
  const top = performances[0];
  return {
    type: mode === 'player' ? 'player-best-performance' : 'franchise-best-performance',
    title: player
      ? `${player} ${mode === 'player' ? 'best' : 'franchise'} ${statDisplayLabel(stat)} game`
      : `${mode === 'max' ? 'Best' : 'Worst'} ${statDisplayLabel(stat)} game`,
    player: player || top?.player || '',
    player_id: top?.player_id || '',
    season: '',
    phase: phaseKey,
    playoffs: phaseKey === 'playoffs',
    date: top?.date || null,
    opponent: top?.opponent || '',
    stat,
    order: 'max',
    source_url: top?.source_url || '',
    source_urls: performances.map((performance) => performance.source_url).filter(Boolean).slice(0, 10),
    stats: { [stat]: top?.value || 0 },
    performances,
    contextLines: [
      `phase: ${phaseLabel}`,
      `stat: ${stat}`,
      `player: ${player || top?.player || ''}`,
      ...performances.map((performance, index) => `performance_${index + 1}: ${performance.player} ${performance.value} on ${performance.date} vs ${performance.opponent}`)
    ]
  };
}

function main() {
  const startedAt = Date.now();
  const games = readGames();
  const playerCareer = new Map();
  const playerSeason = new Map();
  const allPerformances = [];

  for (const game of games) {
    for (const playerRow of game.pacers_players || []) {
      const player = playerRow.player;
      if (!player) {
        continue;
      }

      const playerId = playerRow.player_id || '';
      const careerKey = `${playerId}:${player}`;
      if (!playerCareer.has(careerKey)) {
        playerCareer.set(careerKey, {
          player,
          player_id: playerId,
          phases: createPhaseContainers()
        });
      }
      const careerEntry = playerCareer.get(careerKey);
      ingestRecord(careerEntry.phases.combined, game, playerRow);
      ingestRecord(careerEntry.phases[game.playoffs ? 'playoffs' : 'regular'], game, playerRow);

      const seasonKey = `${careerKey}:${game.season}`;
      if (!playerSeason.has(seasonKey)) {
        playerSeason.set(seasonKey, {
          player,
          player_id: playerId,
          season: game.season,
          phases: createPhaseContainers()
        });
      }
      const seasonEntry = playerSeason.get(seasonKey);
      ingestRecord(seasonEntry.phases.combined, game, playerRow);
      ingestRecord(seasonEntry.phases[game.playoffs ? 'playoffs' : 'regular'], game, playerRow);

      if (!playerRow.did_not_play) {
        for (const stat of PERFORMANCE_STATS) {
          allPerformances.push({
            player,
            player_id: playerId,
            stat,
            value: getComputedStat(playerRow, stat),
            date: game.date,
            season: game.season,
            opponent: game.opponent,
            playoffs: game.playoffs === true,
            source_url: game.recap_url,
            game_id: game.game_id
          });
        }
      }
    }
  }

  const careerRecords = [...playerCareer.values()]
    .map((entry) => buildCareerRecord(entry.player, entry.player_id, entry.phases))
    .sort((a, b) => a.player.localeCompare(b.player));

  const seasonRecords = [...playerSeason.values()]
    .map((entry) => buildSeasonRecord(entry.player, entry.player_id, entry.season, entry.phases))
    .sort((a, b) => a.player.localeCompare(b.player) || a.season.localeCompare(b.season));

  const leaderboardRecords = [];
  for (const stat of LEADERBOARD_STATS) {
    for (const phase of PHASES) {
      const leaders = careerRecords
        .map((record) => {
          const summary = phase.key === 'combined'
            ? record.combined
            : phase.key === 'regular'
              ? record.regular
              : record.playoffs_summary;
          return {
            player: record.player,
            player_id: record.player_id,
            value: getAccumulatorStat(summary, stat),
            games: summary.games,
            last_game_date: summary.last_game_date,
            source_urls: summary.source_urls
          };
        })
        .filter((leader) => leader.value > 0)
        .sort((a, b) => b.value - a.value || a.player.localeCompare(b.player))
        .slice(0, 10)
        .map((leader, index) => ({ ...leader, rank: index + 1 }));

      leaderboardRecords.push(buildLeaderboardRecord(stat, phase.key, leaders));
    }
  }

  const performanceRecords = [];
  for (const stat of PERFORMANCE_STATS) {
    for (const phase of PHASES) {
      const phaseMatches = allPerformances
        .filter((performance) => performance.stat === stat)
        .filter((performance) => phase.key === 'combined'
          ? true
          : phase.key === 'regular'
            ? performance.playoffs === false
            : performance.playoffs === true)
        .filter((performance) => performance.value > 0);

      const franchiseTop = phaseMatches
        .slice()
        .sort((a, b) => b.value - a.value || String(b.date).localeCompare(String(a.date)))
        .slice(0, 10);
      performanceRecords.push(buildBestPerformanceRecord('franchise', stat, phase.key, '', franchiseTop));

      const byPlayer = new Map();
      for (const performance of phaseMatches) {
        if (!byPlayer.has(performance.player)) {
          byPlayer.set(performance.player, []);
        }
        byPlayer.get(performance.player).push(performance);
      }

      for (const [player, performances] of byPlayer.entries()) {
        const top = performances
          .slice()
          .sort((a, b) => b.value - a.value || String(b.date).localeCompare(String(a.date)));
        const topValue = top[0]?.value ?? 0;
        const tied = top.filter((performance) => performance.value === topValue).slice(0, 5);
        performanceRecords.push(buildBestPerformanceRecord('player', stat, phase.key, player, tied));
      }
    }
  }

  const generatedAt = new Date().toISOString();
  const metadata = {
    generated_at: generatedAt,
    generation_time_ms: Date.now() - startedAt,
    source_game_count: games.length
  };

  const outputs = {
    career: { metadata: { ...metadata, dataset: 'player-career-summaries', record_count: careerRecords.length }, records: careerRecords },
    season: { metadata: { ...metadata, dataset: 'player-season-summaries', record_count: seasonRecords.length }, records: seasonRecords },
    leaderboards: { metadata: { ...metadata, dataset: 'franchise-leaderboards', record_count: leaderboardRecords.length }, records: leaderboardRecords },
    performances: { metadata: { ...metadata, dataset: 'best-performances', record_count: performanceRecords.length }, records: performanceRecords }
  };

  fs.writeFileSync(OUTPUTS.career, JSON.stringify(outputs.career, null, 2));
  fs.writeFileSync(OUTPUTS.season, JSON.stringify(outputs.season, null, 2));
  fs.writeFileSync(OUTPUTS.leaderboards, JSON.stringify(outputs.leaderboards, null, 2));
  fs.writeFileSync(OUTPUTS.performances, JSON.stringify(outputs.performances, null, 2));

  console.log(JSON.stringify({
    generated_at: generatedAt,
    generation_time_ms: metadata.generation_time_ms,
    source_game_count: games.length,
    dataset_sizes: {
      'player-career-summaries.json': careerRecords.length,
      'player-season-summaries.json': seasonRecords.length,
      'franchise-leaderboards.json': leaderboardRecords.length,
      'best-performances.json': performanceRecords.length
    }
  }, null, 2));
}

main();
