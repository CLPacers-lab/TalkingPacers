const fs = require('fs');
const path = require('path');

const BOX_SCORES_PATH = path.join(process.cwd(), 'data', 'pacers-boxscores.json');
const CAREER_SUMMARIES_PATH = path.join(process.cwd(), 'data', 'player-career-summaries.json');
const SEASON_SUMMARIES_PATH = path.join(process.cwd(), 'data', 'player-season-summaries.json');
const LEADERBOARDS_PATH = path.join(process.cwd(), 'data', 'franchise-leaderboards.json');
const BEST_PERFORMANCES_PATH = path.join(process.cwd(), 'data', 'best-performances.json');
const NO_DATA_ANSWER = "I don't have that in the TalkingPacers data yet.";
const BOX_SCORE_HINTS = [
  'points', 'point', 'pts', 'rebounds', 'rebound', 'boards', 'assists', 'assist',
  'steals', 'steal', 'blocks', 'block', 'turnovers', 'turnover', 'minutes', 'minute',
  'score', 'scored', 'result', 'won', 'lost', 'starter', 'starters', 'starting',
  'playoff', 'playoffs', 'postseason', 'box score', 'game', 'against', 'vs',
  'highest', 'most', 'fewest', 'lowest', 'career high', 'career low', 'best',
  'leader', 'leaderboard'
];
const FOLLOW_UP_HINTS = ['he', 'him', 'his', 'that game', 'that one', 'what about', 'what happened next'];
const UNSUPPORTED_HINTS = [
  'front office', 'draft', 'salary cap', 'cap space', 'luxury tax', 'contract',
  'contracts', 'extension', 'free agency', 'free agent', 'trade', 'traded',
  'transaction', 'transactions', 'waiver', 'waivers', 'roster history', 'roster move',
  'lineup construction', 'gm', 'general manager', 'president', 'owner',
  'coach think', 'what did they think', 'opinion', 'nba at large', 'league-wide'
];
const PLAYER_MATCH_BLOCKLIST = new Set([
  ...BOX_SCORE_HINTS.map((item) => normalizeText(item)),
  ...UNSUPPORTED_HINTS.flatMap((item) => normalizeText(item).split(/\s+/)),
  'pacers', 'indiana', 'player', 'players', 'team', 'who', 'best', 'most', 'highest',
  'lowest', 'fewest', 'game', 'season', 'playoff', 'playoffs'
]);
const STOPWORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'did', 'do', 'for', 'from', 'game',
  'games', 'had', 'has', 'have', 'he', 'her', 'him', 'how', 'in', 'is', 'it',
  'me', 'of', 'on', 'or', 'season', 'she', 'tell', 'that', 'the', 'their',
  'them', 'they', 'this', 'to', 'was', 'what', 'when', 'which', 'who', 'why',
  'with'
]);
const STAT_ALIASES = {
  MIN: ['min', 'mins', 'minute', 'minutes'],
  PTS: ['point', 'points', 'pts', 'score', 'scored', 'scoring'],
  FG: ['fg', 'field goal', 'field goals', 'shooting'],
  '3PT': ['3pt', '3pts', 'three', 'three pointers', 'threes'],
  FT: ['ft', 'free throw', 'free throws'],
  REB: ['reb', 'rebound', 'rebounds', 'rebounder', 'rebounding', 'boards'],
  AST: ['ast', 'assist', 'assists', 'assister', 'passing', 'playmaker'],
  TO: ['turnover', 'turnovers'],
  STL: ['steal', 'steals', 'stl'],
  BLK: ['block', 'blocks', 'blk'],
  OREB: ['offensive rebound', 'offensive rebounds', 'oreb'],
  DREB: ['defensive rebound', 'defensive rebounds', 'dreb'],
  PF: ['foul', 'fouls', 'personal foul', 'personal fouls'],
  '+/-': ['plus minus', 'plus-minus']
};
const OPPONENT_ALIASES = {
  ATL: ['atl', 'hawks', 'atlanta'],
  BOS: ['bos', 'celtics', 'boston'],
  BKN: ['bkn', 'nets', 'brooklyn', 'nj', 'new jersey'],
  CHA: ['cha', 'hornets', 'charlotte'],
  CHI: ['chi', 'bulls', 'chicago'],
  CLE: ['cle', 'cavs', 'cavaliers', 'cleveland'],
  DAL: ['dal', 'mavs', 'mavericks', 'dallas'],
  DEN: ['den', 'nuggets', 'denver'],
  DET: ['det', 'pistons', 'detroit'],
  GS: ['gs', 'gsw', 'warriors', 'golden state'],
  HOU: ['hou', 'rockets', 'houston'],
  IND: ['ind'],
  LAC: ['lac', 'clippers', 'la clippers'],
  LAL: ['lal', 'lakers', 'la lakers'],
  MEM: ['mem', 'grizzlies', 'memphis', 'vancouver'],
  MIA: ['mia', 'heat', 'miami'],
  MIL: ['mil', 'bucks', 'milwaukee'],
  MIN: ['min', 'timberwolves', 'wolves', 'minnesota'],
  NO: ['no', 'nop', 'pelicans', 'new orleans'],
  NY: ['ny', 'nyk', 'knicks', 'new york'],
  OKC: ['okc', 'thunder', 'oklahoma city', 'sea', 'supersonics', 'sonics'],
  ORL: ['orl', 'magic', 'orlando'],
  PHI: ['phi', 'sixers', '76ers', 'philadelphia'],
  PHX: ['phx', 'suns', 'phoenix'],
  POR: ['por', 'blazers', 'trail blazers', 'portland'],
  SAC: ['sac', 'kings', 'sacramento'],
  SA: ['sa', 'spurs', 'san antonio'],
  TOR: ['tor', 'raptors', 'toronto'],
  UTA: ['uta', 'utah', 'jazz'],
  WAS: ['was', 'wizards', 'washington']
};

let cachedIndex = null;
let cachedDerivedKnowledge = null;

function collapseInitials(text) {
  return normalizeText(text)
    .replace(/\b([a-z])\s+([a-z])\b/g, '$1$2')
    .replace(/\s+/g, ' ')
    .trim();
}

function editDistance(a, b) {
  const left = normalizeText(a);
  const right = normalizeText(b);
  const rows = left.length + 1;
  const cols = right.length + 1;
  const dp = Array.from({ length: rows }, () => Array(cols).fill(0));

  for (let i = 0; i < rows; i += 1) {
    dp[i][0] = i;
  }
  for (let j = 0; j < cols; j += 1) {
    dp[0][j] = j;
  }

  for (let i = 1; i < rows; i += 1) {
    for (let j = 1; j < cols; j += 1) {
      const cost = left[i - 1] === right[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost
      );
    }
  }

  return dp[rows - 1][cols - 1];
}

function extractAnswer(payload) {
  if (typeof payload?.output_text === 'string' && payload.output_text.trim()) {
    return payload.output_text.trim();
  }

  const outputItems = Array.isArray(payload?.output) ? payload.output : [];
  const textParts = [];

  for (const item of outputItems) {
    const contentItems = Array.isArray(item?.content) ? item.content : [];
    for (const content of contentItems) {
      if (typeof content?.text === 'string' && content.text.trim()) {
        textParts.push(content.text.trim());
      }
    }
  }

  const combined = textParts.join('\n\n').trim();
  return combined || null;
}

function normalizeText(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(text) {
  return normalizeText(text)
    .split(/\s+/)
    .filter((token) => token && !STOPWORDS.has(token) && token.length >= 2);
}

function uniqueTokens(text) {
  return [...new Set(tokenize(text))];
}

function extractSeasonTerms(text) {
  return text.match(/\b\d{4}-\d{2}\b(?!-\d{2})/g) || [];
}

function extractDateTerms(text) {
  return text.match(/\b\d{4}-\d{2}-\d{2}\b/g) || [];
}

function hasAverageLanguage(text) {
  return /\baverage|averaged|per game|ppg|rpg|apg\b/.test(normalizeText(text));
}

function hasCareerLanguage(text) {
  return /\bcareer|all time|all-time|as a pacer|with the pacers|for the pacers\b/.test(normalizeText(text));
}

function hasComparisonLanguage(text) {
  return /(playoff.*regular|regular.*playoff|vs regular|vs playoffs|compare)/.test(normalizeText(text));
}

function hasLeaderboardLanguage(text) {
  return /\b(franchise leader|leaderboard|who has the most|who is the best|top)\b/.test(normalizeText(text));
}

function sanitizeHistory(history) {
  if (!Array.isArray(history)) {
    return [];
  }

  return history
    .filter((item) => item && (item.role === 'user' || item.role === 'assistant') && typeof item.content === 'string')
    .map((item) => ({
      role: item.role,
      content: item.content.trim().slice(0, 1200),
      resolvedContext: sanitizeResolvedContext(item.resolvedContext)
    }))
    .filter((item) => item.content)
    .slice(-6);
}

function sanitizeResolvedContext(context) {
  if (!context || typeof context !== 'object') {
    return null;
  }

  const cleaned = {
    game_id: typeof context.game_id === 'string' ? context.game_id : '',
    date: typeof context.date === 'string' ? context.date : '',
    season: typeof context.season === 'string' ? context.season : '',
    opponent: typeof context.opponent === 'string' ? context.opponent : '',
    player: typeof context.player === 'string' ? context.player : '',
    phase: context.phase === 'playoffs' || context.phase === 'regular' ? context.phase : ''
  };

  return Object.values(cleaned).some(Boolean) ? cleaned : null;
}

function getPriorResolvedContext(history) {
  for (let index = history.length - 1; index >= 0; index -= 1) {
    const item = history[index];
    if (item.role === 'assistant' && item.resolvedContext) {
      return item.resolvedContext;
    }
  }
  return null;
}

function buildRetrievalText(question, resolvedContext) {
  const parts = [question];

  if (resolvedContext?.player) {
    parts.push(`player ${resolvedContext.player}`);
  }
  if (resolvedContext?.date) {
    parts.push(`date ${resolvedContext.date}`);
  }
  if (resolvedContext?.opponent) {
    parts.push(`opponent ${resolvedContext.opponent}`);
  }
  if (resolvedContext?.season) {
    parts.push(`season ${resolvedContext.season}`);
  }
  if (resolvedContext?.phase) {
    parts.push(resolvedContext.phase === 'playoffs' ? 'playoffs' : 'regular season');
  }

  return parts.join(' ');
}

function hasPhrase(text, phrase) {
  const normalizedText = ` ${normalizeText(text)} `;
  const normalizedPhrase = normalizeText(phrase);
  const escaped = normalizedPhrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\s+/g, '\\s+');
  const regex = new RegExp(`(^|\\s)${escaped}(?=\\s|$)`, 'i');
  return regex.test(normalizedText);
}

function classifyQuestion(question, history, index) {
  const normalized = normalizeText(question);
  const priorResolvedContext = getPriorResolvedContext(history);
  const matchedPlayers = detectPlayers(question, index);
  const matchedOpponents = detectOpponentCodes(question);
  const dates = extractDateTerms(question);
  const seasons = extractSeasonTerms(question);
  const phase = detectPhase(question);
  const requestedStats = detectRequestedStats(question);
  const aggregation = detectAggregation(question);
  const asksAverage = hasAverageLanguage(question);
  const asksCareer = hasCareerLanguage(question);
  const asksComparison = hasComparisonLanguage(question);
  const asksLeaderboard = hasLeaderboardLanguage(question);
  const hasDirectGameAnchor = dates.length > 0 || (matchedOpponents.length > 0 && /(against|vs)\b/.test(normalized));
  const hasUnsupportedHint = UNSUPPORTED_HINTS.some((hint) => hasPhrase(normalized, hint));
  const hasBoxScoreHint = BOX_SCORE_HINTS.some((hint) => hasPhrase(normalized, hint));
  const hasFollowUpHint = FOLLOW_UP_HINTS.some((hint) => hasPhrase(normalized, hint));
  const hasPlayerGameAnchor = matchedPlayers.length > 0 && (
    matchedOpponents.length > 0 ||
    dates.length > 0 ||
    seasons.length > 0 ||
    phase !== null
  );
  const hasStrongAnchor = (
    matchedPlayers.length > 0 ||
    matchedOpponents.length > 0 ||
    dates.length > 0 ||
    seasons.length > 0 ||
    phase !== null
  );

  let classification = 'unsupported';
  let route = 'unsupported';
  if (hasUnsupportedHint) {
    classification = 'unsupported';
    route = 'unsupported';
  } else if (hasFollowUpHint && priorResolvedContext) {
    classification = 'likely_supported_followup';
    route = 'box-score';
  } else if (aggregation?.type === 'single-game-extreme' && requestedStats.length > 0) {
    classification = 'box_score_supported';
    route = 'best-performances';
  } else if (aggregation?.type === 'player-total-leader' && requestedStats.length > 0) {
    classification = 'box_score_supported';
    route = 'franchise-leaderboards';
  } else if (matchedPlayers.length > 0 && seasons.length > 0 && (asksAverage || requestedStats.length > 0 || phase !== null)) {
    classification = 'box_score_supported';
    route = 'player-season-summaries';
  } else if (matchedPlayers.length > 0 && (asksCareer || asksComparison || asksAverage) && !hasDirectGameAnchor) {
    classification = 'box_score_supported';
    route = 'player-career-summaries';
  } else if (asksLeaderboard && requestedStats.length > 0 && !hasDirectGameAnchor) {
    classification = 'box_score_supported';
    route = 'franchise-leaderboards';
  } else if (hasDirectGameAnchor && matchedPlayers.length > 0) {
    classification = 'box_score_supported';
    route = 'box-score';
  } else if (hasBoxScoreHint && hasStrongAnchor) {
    classification = 'box_score_supported';
    route = 'box-score';
  } else if (hasStrongAnchor && dates.length > 0) {
    classification = 'box_score_supported';
    route = 'box-score';
  } else if (hasPlayerGameAnchor) {
    classification = 'box_score_supported';
    route = 'box-score';
  }

  return {
    classification,
    route,
    priorResolvedContext,
    matchedPlayers,
    matchedOpponents,
    dates,
    seasons,
    phase,
    requestedStats,
    aggregation
  };
}

function addToIndex(map, key, id) {
  if (!key) {
    return;
  }
  if (!map.has(key)) {
    map.set(key, []);
  }
  map.get(key).push(id);
}

function stringifyStats(stats) {
  return Object.entries(stats || {})
    .map(([label, value]) => `${label}:${value}`)
    .join(', ');
}

function buildRecordText(record) {
  const statTerms = Object.entries(record.stats || {}).flatMap(([label, value]) => {
    const aliases = STAT_ALIASES[label] || [label.toLowerCase()];
    return aliases.map((alias) => `${alias} ${value}`);
  });

  return normalizeText([
    record.date,
    record.season,
    record.opponent,
    record.playoffs ? 'playoffs postseason' : 'regular season',
    record.result,
    record.player || '',
    record.title,
    record.stat_line || '',
    ...statTerms
  ].join(' '));
}

function createGameRecord(game) {
  return {
    type: 'game',
    game_id: game.game_id,
    date: game.date,
    season: game.season,
    opponent: game.opponent,
    playoffs: game.playoffs === true,
    source_url: game.recap_url || '',
    title: `${game.date} vs ${game.opponent}`,
    result: game.result,
    stat_line: `Pacers ${game.pacers_score}, Opponent ${game.opponent_score}`,
    player: '',
    stats: {
      PTS: String(game.pacers_score),
      OPP_PTS: String(game.opponent_score)
    },
    contextLines: [
      `game_id: ${game.game_id}`,
      `date: ${game.date}`,
      `season: ${game.season}`,
      `opponent: ${game.opponent}`,
      `phase: ${game.playoffs ? 'playoffs' : 'regular season'}`,
      `result: ${game.result}`,
      `team_score: ${game.pacers_score}`,
      `opponent_score: ${game.opponent_score}`,
      `source_url: ${game.recap_url || 'unknown'}`
    ]
  };
}

function createPlayerGameRecord(game, playerRow) {
  const player = playerRow.player || 'Unknown';
  return {
    type: 'player-game',
    game_id: game.game_id,
    date: game.date,
    season: game.season,
    opponent: game.opponent,
    playoffs: game.playoffs === true,
    source_url: game.recap_url || '',
    title: `${player} on ${game.date} vs ${game.opponent}`,
    result: game.result,
    stat_line: stringifyStats(playerRow.stats || {}),
    player,
    player_id: playerRow.player_id || '',
    starter: playerRow.starter === true,
    did_not_play: playerRow.did_not_play === true,
    reason: playerRow.reason || '',
    stats: playerRow.stats || {},
    contextLines: [
      `game_id: ${game.game_id}`,
      `date: ${game.date}`,
      `season: ${game.season}`,
      `opponent: ${game.opponent}`,
      `phase: ${game.playoffs ? 'playoffs' : 'regular season'}`,
      `result: ${game.result}`,
      `player: ${player}`,
      `starter: ${playerRow.starter === true ? 'yes' : 'no'}`,
      `did_not_play: ${playerRow.did_not_play === true ? 'yes' : 'no'}`,
      `reason: ${playerRow.reason || 'n/a'}`,
      `stats: ${stringifyStats(playerRow.stats || {})}`,
      `source_url: ${game.recap_url || 'unknown'}`
    ]
  };
}

function buildIndex() {
  if (cachedIndex) {
    return cachedIndex;
  }

  const games = JSON.parse(fs.readFileSync(BOX_SCORES_PATH, 'utf8'));
  const records = [];
  const byPlayer = new Map();
  const byOpponent = new Map();
  const bySeason = new Map();
  const byDate = new Map();
  const byGameId = new Map();
  const byPhase = new Map([['playoffs', []], ['regular', []]]);
  const players = [];
  const playerFirstNames = new Map();
  const playerLastNames = new Map();

  for (const game of games) {
    const gameRecord = createGameRecord(game);
    gameRecord.text = buildRecordText(gameRecord);
    const gameIndex = records.push(gameRecord) - 1;

    addToIndex(byOpponent, gameRecord.opponent, gameIndex);
    addToIndex(bySeason, gameRecord.season, gameIndex);
    addToIndex(byDate, gameRecord.date, gameIndex);
    addToIndex(byGameId, gameRecord.game_id, gameIndex);
    byPhase.get(gameRecord.playoffs ? 'playoffs' : 'regular').push(gameIndex);

    for (const playerRow of game.pacers_players || []) {
      const playerRecord = createPlayerGameRecord(game, playerRow);
      playerRecord.text = buildRecordText(playerRecord);
      const playerIndex = records.push(playerRecord) - 1;

      addToIndex(byOpponent, playerRecord.opponent, playerIndex);
      addToIndex(bySeason, playerRecord.season, playerIndex);
      addToIndex(byDate, playerRecord.date, playerIndex);
      addToIndex(byGameId, playerRecord.game_id, playerIndex);
      addToIndex(byPhase, playerRecord.playoffs ? 'playoffs' : 'regular', playerIndex);

      const playerName = normalizeText(playerRecord.player);
      if (playerName) {
        addToIndex(byPlayer, playerName, playerIndex);
        addToIndex(byPlayer, collapseInitials(playerName), playerIndex);
        const nameParts = playerName.split(/\s+/);
        const firstName = nameParts[0];
        const lastName = nameParts.slice(-1)[0];
        addToIndex(playerFirstNames, firstName, playerName);
        addToIndex(playerLastNames, lastName, playerName);
        addToIndex(byPlayer, lastName, playerIndex);
        players.push(playerName);
      }
    }
  }

  cachedIndex = {
    records,
    byPlayer,
    byOpponent,
    bySeason,
    byDate,
    byGameId,
    byPhase,
    players: [...new Set(players)],
    playerFirstNames,
    playerLastNames
  };
  return cachedIndex;
}

function buildDerivedKnowledge() {
  if (cachedDerivedKnowledge) {
    return cachedDerivedKnowledge;
  }

  const datasets = {
    career: JSON.parse(fs.readFileSync(CAREER_SUMMARIES_PATH, 'utf8')),
    season: JSON.parse(fs.readFileSync(SEASON_SUMMARIES_PATH, 'utf8')),
    leaderboards: JSON.parse(fs.readFileSync(LEADERBOARDS_PATH, 'utf8')),
    performances: JSON.parse(fs.readFileSync(BEST_PERFORMANCES_PATH, 'utf8'))
  };

  const decorate = (records) => records.map((record) => ({
    ...record,
    text: normalizeText([
      record.title,
      record.player || '',
      record.season || '',
      record.phase || '',
      record.stat || '',
      ...(record.contextLines || [])
    ].join(' ')),
    player_normalized: normalizeText(record.player || '')
  }));

  cachedDerivedKnowledge = {
    career: {
      metadata: datasets.career.metadata,
      records: decorate(datasets.career.records)
    },
    season: {
      metadata: datasets.season.metadata,
      records: decorate(datasets.season.records)
    },
    leaderboards: {
      metadata: datasets.leaderboards.metadata,
      records: decorate(datasets.leaderboards.records)
    },
    performances: {
      metadata: datasets.performances.metadata,
      records: decorate(datasets.performances.records)
    }
  };
  return cachedDerivedKnowledge;
}

function detectPlayers(text, index) {
  const normalized = normalizeText(text);
  const collapsed = collapseInitials(text);
  const tokens = uniqueTokens(text);
  const matches = new Set();

  for (const name of index.players) {
    if (normalized.includes(name) || collapsed.includes(collapseInitials(name))) {
      matches.add(name);
    }
  }

  for (const token of tokens) {
    if (PLAYER_MATCH_BLOCKLIST.has(token)) {
      continue;
    }

    const firstNameMatches = index.playerFirstNames.get(token) || [];
    if (firstNameMatches.length === 1) {
      matches.add(firstNameMatches[0]);
    }

    const lastNameMatches = index.playerLastNames.get(token) || [];
    if (lastNameMatches.length === 1) {
      matches.add(lastNameMatches[0]);
    }
  }

  if (matches.size === 0) {
    for (const token of tokens) {
      if (token.length < 4 || PLAYER_MATCH_BLOCKLIST.has(token)) {
        continue;
      }

      let bestName = null;
      let bestDistance = Infinity;

      for (const name of index.players) {
        const parts = name.split(/\s+/);
        const firstName = parts[0];
        const lastName = parts.slice(-1)[0];
        const candidateParts = [firstName, lastName, collapseInitials(name)];

        for (const candidate of candidateParts) {
          const distance = editDistance(token, candidate);
          const lengthGap = Math.abs(token.length - candidate.length);
          if (distance <= 2 && lengthGap <= 2 && distance < bestDistance) {
            bestDistance = distance;
            bestName = name;
          }
        }
      }

      if (bestName) {
        matches.add(bestName);
      }
    }
  }

  return [...matches];
}

function detectOpponentCodes(text) {
  const normalized = normalizeText(text);
  return Object.entries(OPPONENT_ALIASES)
    .filter(([, aliases]) => aliases.some((alias) => hasPhrase(normalized, alias)))
    .map(([code]) => code);
}

function detectPhase(text) {
  const normalized = normalizeText(text);
  if (/(playoff|playoffs|postseason|series|game 7|game 6|game 5)/.test(normalized)) {
    return 'playoffs';
  }
  if (/(regular season|regular|season opener)/.test(normalized)) {
    return 'regular';
  }
  return null;
}

function detectRequestedStats(text) {
  const normalized = normalizeText(text);
  const labels = [];

  for (const [label, aliases] of Object.entries(STAT_ALIASES)) {
    if (aliases.some((alias) => hasPhrase(normalized, alias))) {
      labels.push(label);
    }
  }

  return labels;
}

function detectAggregation(text) {
  const normalized = normalizeText(text);
  const asksWho = /\bwho\b/.test(normalized);
  const hasLeaderLanguage = /(best|most|leader|top)/.test(normalized);
  const hasExtremeLanguage = /(highest|lowest|fewest|career high|career low|max|min)/.test(normalized);

  if (asksWho && hasLeaderLanguage) {
    return { type: 'player-total-leader', order: 'max' };
  }
  if (hasLeaderLanguage && /\b(for the pacers|as a pacer|on the pacers)\b/.test(normalized) && !/\bgame\b/.test(normalized)) {
    return { type: 'player-total-leader', order: 'max' };
  }
  if (/(highest|most|career high|best|max)/.test(normalized)) {
    return { type: 'single-game-extreme', order: 'max' };
  }
  if (/(lowest|fewest|career low|min)/.test(normalized)) {
    return { type: 'single-game-extreme', order: 'min' };
  }
  if (hasExtremeLanguage) {
    return { type: 'single-game-extreme', order: 'max' };
  }
  return null;
}

function parseStatValue(value) {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  const text = String(value || '').trim();
  if (!text) {
    return null;
  }

  if (/^-?\d+(\.\d+)?$/.test(text)) {
    return Number(text);
  }

  return null;
}

function scoreRecord(record, query, matchedPlayers, matchedOpponents, requestedStats) {
  let score = 0;

  for (const keyword of query.keywords) {
    if (record.text.includes(keyword)) {
      score += 1;
    }
  }

  if (matchedPlayers.some((player) => player === normalizeText(record.player))) {
    score += 10;
  } else if (matchedPlayers.length > 0 && record.type === 'player-game') {
    const lastName = normalizeText(record.player).split(/\s+/).slice(-1)[0];
    if (matchedPlayers.some((player) => player.endsWith(lastName))) {
      score += 6;
    }
  }

  if (matchedOpponents.includes(record.opponent)) {
    score += 7;
  }

  if (query.seasons.includes(record.season)) {
    score += 7;
  }

  if (query.dates.includes(record.date)) {
    score += 10;
  }

  if (query.phase && ((query.phase === 'playoffs') === record.playoffs)) {
    score += 5;
  }

  if (requestedStats.length > 0) {
    for (const label of requestedStats) {
      if (Object.prototype.hasOwnProperty.call(record.stats || {}, label)) {
        score += 4;
      }
    }
  }

  if (record.type === 'player-game' && matchedPlayers.length > 0) {
    score += 2;
  }

  return score;
}

function scoreDerivedRecord(record, query, matchedPlayers, requestedStats) {
  let score = 0;

  for (const keyword of query.keywords) {
    if (record.text.includes(keyword)) {
      score += 1;
    }
  }

  if (matchedPlayers.some((player) => player === record.player_normalized)) {
    score += 12;
  } else if (matchedPlayers.length > 0 && record.player_normalized) {
    const lastName = record.player_normalized.split(/\s+/).slice(-1)[0];
    if (matchedPlayers.some((player) => player.endsWith(lastName))) {
      score += 8;
    }
  }

  if (query.seasons.length > 0 && record.season && query.seasons.includes(record.season)) {
    score += 10;
  }

  if (query.phase && (record.phase === query.phase || (query.phase === 'playoffs' && record.playoffs === true))) {
    score += 5;
  }

  if (requestedStats.length > 0) {
    for (const label of requestedStats) {
      if (record.stat === label || Object.prototype.hasOwnProperty.call(record.stats || {}, label)) {
        score += 6;
      }
    }
  }

  if (record.type === 'franchise-leaderboard' && /(leader|most|best|top)/.test(query.text)) {
    score += 4;
  }

  return score;
}

function hasStrongSignals(query, matchedPlayers, matchedOpponents, resolvedContext) {
  return (
    matchedPlayers.length > 0 ||
    matchedOpponents.length > 0 ||
    query.seasons.length > 0 ||
    query.dates.length > 0 ||
    query.phase !== null ||
    Boolean(resolvedContext?.game_id || resolvedContext?.player)
  );
}

function gatherCandidateIds(query, index, matchedPlayers, matchedOpponents, resolvedContext) {
  const candidateIds = new Set();

  if (resolvedContext?.game_id) {
    for (const id of index.byGameId.get(resolvedContext.game_id) || []) {
      candidateIds.add(id);
    }
  }

  if (resolvedContext?.player) {
    const normalizedPlayer = normalizeText(resolvedContext.player);
    for (const id of index.byPlayer.get(normalizedPlayer) || []) {
      candidateIds.add(id);
    }
  }

  for (const player of matchedPlayers) {
    for (const id of index.byPlayer.get(player) || []) {
      candidateIds.add(id);
    }
  }

  for (const opponent of matchedOpponents) {
    for (const id of index.byOpponent.get(opponent) || []) {
      candidateIds.add(id);
    }
  }

  for (const season of query.seasons) {
    for (const id of index.bySeason.get(season) || []) {
      candidateIds.add(id);
    }
  }

  for (const date of query.dates) {
    for (const id of index.byDate.get(date) || []) {
      candidateIds.add(id);
    }
  }

  if (query.phase) {
    for (const id of index.byPhase.get(query.phase) || []) {
      candidateIds.add(id);
    }
  }

  return [...candidateIds];
}

function filterScoredMatches(scoredMatches, query, matchedPlayers, resolvedContext) {
  let filtered = scoredMatches;

  if (resolvedContext?.game_id) {
    filtered = filtered.filter((item) => item.record.game_id === resolvedContext.game_id);
  } else if (query.dates.length > 0) {
    const exactDateMatches = filtered.filter((item) => query.dates.includes(item.record.date));
    if (exactDateMatches.length > 0) {
      filtered = exactDateMatches;
    }
  }

  if (matchedPlayers.length > 0) {
    const exactPlayerMatches = filtered.filter((item) => {
      const normalizedPlayer = normalizeText(item.record.player || '');
      return matchedPlayers.some((player) => {
        if (player === normalizedPlayer) {
          return true;
        }
        const lastName = normalizedPlayer.split(/\s+/).slice(-1)[0];
        return player.endsWith(lastName);
      });
    });

    if (exactPlayerMatches.length > 0) {
      filtered = exactPlayerMatches;
    }
  }

  return filtered;
}

function filterDerivedRecords(records, route, classification, query, requestedStats) {
  if (route === 'player-career-summaries') {
    return records.filter((record) => classification.matchedPlayers.length === 0
      ? true
      : classification.matchedPlayers.some((player) => player === record.player_normalized || player.endsWith(record.player_normalized.split(/\s+/).slice(-1)[0])));
  }

  if (route === 'player-season-summaries') {
    return records
      .filter((record) => classification.matchedPlayers.some((player) => player === record.player_normalized || player.endsWith(record.player_normalized.split(/\s+/).slice(-1)[0])))
      .filter((record) => query.seasons.length === 0 || query.seasons.includes(record.season));
  }

  if (route === 'franchise-leaderboards') {
    return records.filter((record) => requestedStats.length === 0 || requestedStats.includes(record.stat));
  }

  if (route === 'best-performances') {
    let filtered = records.filter((record) => requestedStats.length === 0 || requestedStats.includes(record.stat));
    if (classification.matchedPlayers.length > 0) {
      filtered = filtered.filter((record) => record.type === 'player-best-performance')
        .filter((record) => classification.matchedPlayers.some((player) => player === record.player_normalized || player.endsWith(record.player_normalized.split(/\s+/).slice(-1)[0])));
    } else {
      filtered = filtered.filter((record) => record.type === 'franchise-best-performance');
    }
    if (query.phase) {
      filtered = filtered.filter((record) => record.phase === query.phase || record.phase === 'combined');
    } else {
      filtered = filtered.filter((record) => record.phase === 'combined');
    }
    return filtered;
  }

  return records;
}

function findDerivedRecords(question, history, debugMode, classification) {
  const knowledge = buildDerivedKnowledge();
  const retrievalQuestion = buildRetrievalText(question, classification.priorResolvedContext);
  const query = {
    text: normalizeText(retrievalQuestion),
    keywords: uniqueTokens(retrievalQuestion),
    seasons: extractSeasonTerms(retrievalQuestion),
    dates: extractDateTerms(retrievalQuestion),
    phase: detectPhase(retrievalQuestion)
  };
  const requestedStats = classification.requestedStats.length > 0
    ? classification.requestedStats
    : detectRequestedStats(retrievalQuestion);
  const routeKey = classification.route === 'player-career-summaries'
    ? 'career'
    : classification.route === 'player-season-summaries'
      ? 'season'
      : classification.route === 'franchise-leaderboards'
        ? 'leaderboards'
        : 'performances';
  const records = knowledge[routeKey].records;
  const filtered = filterDerivedRecords(records, classification.route, classification, query, requestedStats);
  const matches = filtered
    .map((record) => ({
      record,
      score: scoreDerivedRecord(record, query, classification.matchedPlayers, requestedStats)
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => {
      if (b.score !== a.score) {
        return b.score - a.score;
      }
      return String(b.record.date || '').localeCompare(String(a.record.date || ''));
    })
    .slice(0, 5);

  const debug = debugMode ? {
    classification: classification.classification,
    route: classification.route,
    dataset: knowledge[routeKey].metadata.dataset,
    dataset_size: knowledge[routeKey].metadata.record_count,
    matched_players: classification.matchedPlayers,
    matched_seasons: query.seasons,
    matched_phase: query.phase,
    requested_stats: requestedStats,
    records_retrieved: matches.map((item) => ({
      title: item.record.title,
      type: item.record.type,
      player: item.record.player || null,
      season: item.record.season || null,
      stat: item.record.stat || null,
      retrieval_score: item.score,
      source_url: item.record.source_url || null
    }))
  } : null;

  return {
    matches,
    debug,
    classification: classification.classification,
    resolvedContext: buildResolvedContext(matches, classification.priorResolvedContext)
  };
}

function runAggregateQuery(index, classification, query) {
  const requestedStat = classification.requestedStats[0];
  if (!requestedStat) {
    return [];
  }

  const candidateIds = gatherCandidateIds(
    query,
    index,
    classification.matchedPlayers,
    classification.matchedOpponents,
    classification.priorResolvedContext
  );

  let matches = candidateIds
    .map((id) => index.records[id])
    .filter((record) => record.type === 'player-game')
    .filter((record) => Object.prototype.hasOwnProperty.call(record.stats || {}, requestedStat))
    .filter((record) => {
      const value = parseStatValue(record.stats[requestedStat]);
      return value !== null;
    });

  matches = filterScoredMatches(
    matches.map((record) => ({
      record,
      score: scoreRecord(record, query, classification.matchedPlayers, classification.matchedOpponents, classification.requestedStats)
    })),
    query,
    classification.matchedPlayers,
    classification.priorResolvedContext
  );

  matches.sort((a, b) => {
    const valueA = parseStatValue(a.record.stats[requestedStat]);
    const valueB = parseStatValue(b.record.stats[requestedStat]);
    if (valueA !== valueB) {
      return classification.aggregation.order === 'min' ? valueA - valueB : valueB - valueA;
    }
    return String(b.record.date || '').localeCompare(String(a.record.date || ''));
  });

  if (matches.length === 0) {
    return [];
  }

  const bestValue = parseStatValue(matches[0].record.stats[requestedStat]);
  return matches
    .filter((item) => parseStatValue(item.record.stats[requestedStat]) === bestValue)
    .slice(0, 5);
}

function createAggregateLeaderRecord(player, statLabel, total, supportRecords, query) {
  const first = supportRecords[0]?.record || {};
  const sourceRecords = supportRecords.slice(0, 5).map((item) => item.record);
  const sourceUrls = sourceRecords.map((record) => record.source_url).filter(Boolean);
  const gamesCount = supportRecords.length;

  return {
    type: 'player-total-leader',
    game_id: '',
    date: '',
    season: query.seasons[0] || '',
    opponent: query.matchedOpponents?.[0] || '',
    playoffs: query.phase === 'playoffs',
    source_url: sourceUrls[0] || '',
    source_urls: sourceUrls,
    title: `${player} total ${statLabel}`,
    result: '',
    stat_line: `${statLabel}:${total}`,
    player,
    player_id: first.player_id || '',
    stats: { [statLabel]: String(total), GAMES: String(gamesCount) },
    contextLines: [
      `player: ${player}`,
      `aggregate_type: player total leader`,
      `stat: ${statLabel}`,
      `total: ${total}`,
      `games_counted: ${gamesCount}`,
      `season_filter: ${query.seasons.join(', ') || 'all seasons'}`,
      `opponent_filter: ${query.matchedOpponents?.join(', ') || 'all opponents'}`,
      `phase_filter: ${query.phase || 'all phases'}`,
      `sample_source_urls: ${sourceUrls.slice(0, 3).join(', ') || 'unknown'}`
    ]
  };
}

function runLeaderAggregateQuery(index, classification, query) {
  const requestedStat = classification.requestedStats[0];
  if (!requestedStat) {
    return [];
  }

  let candidateIds = gatherCandidateIds(
    query,
    index,
    classification.matchedPlayers,
    classification.matchedOpponents,
    classification.priorResolvedContext
  );

  if (candidateIds.length === 0) {
    candidateIds = index.records.map((_, id) => id);
  }

  const playerGames = candidateIds
    .map((id) => index.records[id])
    .filter((record) => record.type === 'player-game')
    .filter((record) => Object.prototype.hasOwnProperty.call(record.stats || {}, requestedStat))
    .filter((record) => parseStatValue(record.stats[requestedStat]) !== null);

  if (playerGames.length === 0) {
    return [];
  }

  const grouped = new Map();
  for (const record of playerGames) {
    if (!grouped.has(record.player)) {
      grouped.set(record.player, []);
    }
    grouped.get(record.player).push({
      record,
      score: scoreRecord(record, query, classification.matchedPlayers, classification.matchedOpponents, classification.requestedStats)
    });
  }

  const leaders = [...grouped.entries()]
    .map(([player, records]) => {
      const total = records.reduce((sum, item) => sum + parseStatValue(item.record.stats[requestedStat]), 0);
      return {
        record: createAggregateLeaderRecord(player, requestedStat, total, records, {
          seasons: query.seasons,
          phase: query.phase,
          matchedOpponents: classification.matchedOpponents
        }),
        score: records.reduce((sum, item) => sum + item.score, 0)
      };
    })
    .sort((a, b) => {
      const totalA = parseStatValue(a.record.stats[requestedStat]);
      const totalB = parseStatValue(b.record.stats[requestedStat]);
      if (totalA !== totalB) {
        return classification.aggregation.order === 'min' ? totalA - totalB : totalB - totalA;
      }
      return b.score - a.score;
    });

  if (leaders.length === 0) {
    return [];
  }

  const bestValue = parseStatValue(leaders[0].record.stats[requestedStat]);
  return leaders
    .filter((item) => parseStatValue(item.record.stats[requestedStat]) === bestValue)
    .slice(0, 5);
}

function findRelevantRecords(question, history, debugMode) {
  const index = buildIndex();
  const classification = classifyQuestion(question, history, index);
  if (classification.classification === 'unsupported') {
    return {
      matches: [],
      debug: debugMode ? {
        classification: classification.classification,
        matched_players: classification.matchedPlayers,
        matched_opponents: classification.matchedOpponents,
        matched_seasons: classification.seasons,
        matched_dates: classification.dates,
        matched_phase: classification.phase,
        requested_stats: classification.requestedStats,
        aggregation: classification.aggregation,
        prior_resolved_context: classification.priorResolvedContext,
        records_retrieved: []
      } : null,
      classification: classification.classification,
      resolvedContext: classification.priorResolvedContext
    };
  }

  if (classification.route && classification.route !== 'box-score') {
    return findDerivedRecords(question, history, debugMode, classification);
  }

  const retrievalQuestion = buildRetrievalText(question, classification.priorResolvedContext);
  const query = {
    text: normalizeText(retrievalQuestion),
    keywords: uniqueTokens(retrievalQuestion),
    seasons: extractSeasonTerms(retrievalQuestion),
    dates: extractDateTerms(retrievalQuestion),
    phase: detectPhase(retrievalQuestion)
  };
  const matchedPlayers = [...new Set([
    ...classification.matchedPlayers,
    ...detectPlayers(retrievalQuestion, index)
  ])];
  const matchedOpponents = [...new Set([
    ...classification.matchedOpponents,
    ...detectOpponentCodes(retrievalQuestion)
  ])];
  const requestedStats = classification.requestedStats.length > 0
    ? classification.requestedStats
    : detectRequestedStats(retrievalQuestion);
  if (classification.aggregation?.type === 'single-game-extreme') {
    const matches = runAggregateQuery(index, { ...classification, requestedStats }, query);
    const resolvedContext = buildResolvedContext(matches, classification.priorResolvedContext);
    const debug = debugMode ? {
      classification: classification.classification,
      query_type: 'single-game-extreme',
      candidate_count: matches.length,
      matched_players: matchedPlayers,
      matched_opponents: matchedOpponents,
      matched_seasons: query.seasons,
      matched_dates: query.dates,
      matched_phase: query.phase,
      requested_stats: requestedStats,
      aggregation: classification.aggregation,
      prior_resolved_context: classification.priorResolvedContext,
      records_retrieved: matches.map((item) => ({
        title: item.record.title,
        type: item.record.type,
        game_id: item.record.game_id,
        date: item.record.date,
        season: item.record.season,
        opponent: item.record.opponent,
        player: item.record.player || null,
        retrieval_score: item.score,
        stat_value: item.record.stats[requestedStats[0]],
        source_url: item.record.source_url || null
      }))
    } : null;
    return { matches, debug, classification: classification.classification, resolvedContext };
  }
  if (classification.aggregation?.type === 'player-total-leader') {
    const matches = runLeaderAggregateQuery(index, { ...classification, requestedStats }, query);
    const resolvedContext = buildResolvedContext(matches, classification.priorResolvedContext);
    const debug = debugMode ? {
      classification: classification.classification,
      query_type: 'player-total-leader',
      candidate_count: matches.length,
      matched_players: matchedPlayers,
      matched_opponents: matchedOpponents,
      matched_seasons: query.seasons,
      matched_dates: query.dates,
      matched_phase: query.phase,
      requested_stats: requestedStats,
      aggregation: classification.aggregation,
      prior_resolved_context: classification.priorResolvedContext,
      records_retrieved: matches.map((item) => ({
        title: item.record.title,
        type: item.record.type,
        player: item.record.player || null,
        total_value: item.record.stats[requestedStats[0]],
        games_counted: item.record.stats.GAMES,
        retrieval_score: item.score,
        source_url: item.record.source_url || null
      }))
    } : null;
    return { matches, debug, classification: classification.classification, resolvedContext };
  }
  const candidateIds = gatherCandidateIds(query, index, matchedPlayers, matchedOpponents, classification.priorResolvedContext);
  const requireStrongSupport = !hasStrongSignals(query, matchedPlayers, matchedOpponents, classification.priorResolvedContext);

  if (candidateIds.length === 0 || requireStrongSupport) {
    return {
      matches: [],
      debug: debugMode ? {
        classification: classification.classification,
        candidate_count: candidateIds.length,
        matched_players: matchedPlayers,
        matched_opponents: matchedOpponents,
        matched_seasons: query.seasons,
        matched_dates: query.dates,
        matched_phase: query.phase,
        requested_stats: requestedStats,
        prior_resolved_context: classification.priorResolvedContext,
        records_retrieved: []
      } : null,
      classification: classification.classification,
      resolvedContext: classification.priorResolvedContext
    };
  }

  const matches = filterScoredMatches(candidateIds
    .map((id) => {
      const record = index.records[id];
      return {
        record,
        score: scoreRecord(record, query, matchedPlayers, matchedOpponents, requestedStats)
      };
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => {
      if (b.score !== a.score) {
        return b.score - a.score;
      }
      return String(b.record.date || '').localeCompare(String(a.record.date || ''));
    }), query, matchedPlayers, classification.priorResolvedContext)
    .filter((item) => {
      return item.score >= 7;
    })
    .slice(0, 5);

  const resolvedContext = buildResolvedContext(matches, classification.priorResolvedContext);

  const debug = debugMode ? {
    classification: classification.classification,
    route: classification.route,
    candidate_count: candidateIds.length,
    matched_players: matchedPlayers,
    matched_opponents: matchedOpponents,
    matched_seasons: query.seasons,
    matched_dates: query.dates,
    matched_phase: query.phase,
    requested_stats: requestedStats,
    prior_resolved_context: classification.priorResolvedContext,
    records_retrieved: matches.map((item) => ({
      title: item.record.title,
      type: item.record.type,
      game_id: item.record.game_id,
      date: item.record.date,
      season: item.record.season,
      opponent: item.record.opponent,
      player: item.record.player || null,
      retrieval_score: item.score,
      source_url: item.record.source_url || null
    }))
  } : null;

  return { matches, debug, classification: classification.classification, resolvedContext };
}

function buildResolvedContext(matches, priorResolvedContext) {
  const top = matches[0]?.record;
  if (!top) {
    return priorResolvedContext || null;
  }

  return {
    game_id: top.game_id || priorResolvedContext?.game_id || '',
    date: top.date || priorResolvedContext?.date || '',
    season: top.season || priorResolvedContext?.season || '',
    opponent: top.opponent || priorResolvedContext?.opponent || '',
    player: top.player || priorResolvedContext?.player || '',
    phase: top.playoffs ? 'playoffs' : 'regular'
  };
}

function buildContextBlock(matches) {
  return matches.map(({ record }, index) => {
    return [
      `Record ${index + 1}`,
      `type: ${record.type}`,
      ...record.contextLines
    ].join('\n');
  }).join('\n\n');
}

function buildSources(matches) {
  return matches.flatMap(({ record }) => {
    if ((record.type === 'player-total-leader' || record.type === 'player-career-summary' || record.type === 'player-season-summary' || record.type === 'franchise-leaderboard') && Array.isArray(record.source_urls) && record.source_urls.length > 0) {
      return record.source_urls.slice(0, 5).map((url, index) => ({
        title: `${record.player || record.title} supporting game ${index + 1}`,
        date: null,
        opponent: null,
        url
      }));
    }

    if ((record.type === 'player-best-performance' || record.type === 'franchise-best-performance') && Array.isArray(record.performances)) {
      return record.performances.map((performance) => ({
        title: `${performance.player} ${record.stat} game`,
        date: performance.date || null,
        opponent: performance.opponent || null,
        url: performance.source_url || null
      }));
    }

    return [{
      title: record.title,
      date: record.date || null,
      opponent: record.opponent || null,
      url: record.source_url || null
    }];
  });
}

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method not allowed. Use POST.' });
  }

  try {
    const question = typeof req.body?.question === 'string' ? req.body.question.trim() : '';
    if (!question) {
      return res.status(400).json({ error: 'Question cannot be empty.' });
    }

    const debugMode = req.body?.debug === true;
    const history = sanitizeHistory(req.body?.history);
    const { matches, debug, resolvedContext } = findRelevantRecords(question, history, debugMode);
    const sources = buildSources(matches);

    if (matches.length === 0) {
      return res.status(200).json({ answer: NO_DATA_ANSWER, sources: [], debug, resolvedContext: null });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'OPENAI_API_KEY is missing on the server.' });
    }

    const historyBlock = history.length > 0
      ? history.map((item, index) => `${index + 1}. ${item.role}: ${item.content}`).join('\n')
      : 'None';

    const response = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: 'gpt-4.1-mini',
        input: [
          {
            role: 'system',
            content: [
              {
                type: 'input_text',
                text: [
                  'You are TalkingPacers.',
                  '',
                  'Answer only from the supplied Pacers knowledge records derived from Pacers box scores.',
                  'Use conversation history only to resolve references like "that game," "him," or "what happened next?"',
                  'Do not let conversation history add facts that are not in the supplied records.',
                  'Write naturally and conversationally.',
                  '',
                  'If the answer is not supported by the supplied records, respond exactly:',
                  '',
                  `"${NO_DATA_ANSWER}"`,
                  '',
                  'Do not use outside basketball knowledge.',
                  'Do not guess.',
                  'Include dates when available.',
                  'Do not list source links in the answer body.'
                ].join('\n'),
              },
            ],
          },
          {
            role: 'user',
            content: [
              {
                type: 'input_text',
                text: `Conversation history:\n${historyBlock}\n\nQuestion:\n${question}\n\nPacers knowledge records:\n${buildContextBlock(matches)}`,
              },
            ],
          },
        ],
      }),
    });

    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      const message = payload?.error?.message || 'OpenAI API error.';
      return res.status(response.status).json({ error: message });
    }

    const answer = extractAnswer(payload);
    if (!answer) {
      return res.status(502).json({ error: 'Malformed response from OpenAI.' });
    }

    return res.status(200).json({ answer, sources, debug, resolvedContext });
  } catch (error) {
    console.error('Ask endpoint failed:', error);
    return res.status(500).json({ error: 'Server error while answering question.' });
  }
};

module.exports._internals = {
  NO_DATA_ANSWER,
  buildIndex,
  buildDerivedKnowledge,
  classifyQuestion,
  findRelevantRecords,
  sanitizeHistory,
  sanitizeResolvedContext,
  getPriorResolvedContext
};
