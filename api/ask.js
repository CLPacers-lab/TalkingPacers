const fs = require('fs');
const path = require('path');

const BOX_SCORES_PATH = path.join(process.cwd(), 'data', 'pacers-boxscores.json');
const NO_DATA_ANSWER = "I don't have that in the TalkingPacers data yet.";
const BOX_SCORE_HINTS = [
  'points', 'point', 'pts', 'rebounds', 'rebound', 'boards', 'assists', 'assist',
  'steals', 'steal', 'blocks', 'block', 'turnovers', 'turnover', 'minutes', 'minute',
  'score', 'scored', 'result', 'won', 'lost', 'starter', 'starters', 'starting',
  'playoff', 'playoffs', 'postseason', 'box score', 'game', 'against', 'vs'
];
const FOLLOW_UP_HINTS = ['he', 'him', 'his', 'that game', 'that one', 'what about', 'what happened next'];
const UNSUPPORTED_HINTS = [
  'front office', 'draft', 'salary cap', 'cap space', 'luxury tax', 'contract',
  'contracts', 'extension', 'free agency', 'free agent', 'trade', 'traded',
  'transaction', 'transactions', 'waiver', 'waivers', 'roster history', 'roster move',
  'lineup construction', 'gm', 'general manager', 'president', 'owner',
  'coach think', 'what did they think', 'opinion', 'nba at large', 'league-wide'
];
const STOPWORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'did', 'do', 'for', 'from', 'game',
  'games', 'had', 'has', 'have', 'he', 'her', 'him', 'how', 'in', 'is', 'it',
  'me', 'of', 'on', 'or', 'season', 'she', 'tell', 'that', 'the', 'their',
  'them', 'they', 'this', 'to', 'was', 'what', 'when', 'which', 'who', 'why',
  'with'
]);
const STAT_ALIASES = {
  MIN: ['min', 'mins', 'minute', 'minutes'],
  PTS: ['point', 'points', 'pts', 'score', 'scored'],
  FG: ['fg', 'field goal', 'field goals', 'shooting'],
  '3PT': ['3pt', '3pts', 'three', 'three pointers', 'threes'],
  FT: ['ft', 'free throw', 'free throws'],
  REB: ['reb', 'rebound', 'rebounds', 'boards'],
  AST: ['ast', 'assist', 'assists'],
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
  if (hasUnsupportedHint) {
    classification = 'unsupported';
  } else if (hasFollowUpHint && priorResolvedContext) {
    classification = 'likely_supported_followup';
  } else if (hasBoxScoreHint && hasStrongAnchor) {
    classification = 'box_score_supported';
  } else if (hasStrongAnchor && dates.length > 0) {
    classification = 'box_score_supported';
  } else if (hasPlayerGameAnchor) {
    classification = 'box_score_supported';
  }

  return {
    classification,
    priorResolvedContext,
    matchedPlayers,
    matchedOpponents,
    dates,
    seasons,
    phase
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
        const lastName = playerName.split(/\s+/).slice(-1)[0];
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
    players: [...new Set(players)]
  };
  return cachedIndex;
}

function detectPlayers(text, index) {
  const normalized = normalizeText(text);
  const matches = [];

  for (const name of index.players) {
    if (normalized.includes(name)) {
      matches.push(name);
    }
  }

  return [...new Set(matches)];
}

function detectOpponentCodes(text) {
  const normalized = normalizeText(text);
  return Object.entries(OPPONENT_ALIASES)
    .filter(([, aliases]) => aliases.some((alias) => normalized.includes(normalizeText(alias))))
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
        prior_resolved_context: classification.priorResolvedContext,
        records_retrieved: []
      } : null,
      classification: classification.classification,
      resolvedContext: classification.priorResolvedContext
    };
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
  const requestedStats = detectRequestedStats(retrievalQuestion);
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
  return matches.map(({ record }) => ({
    title: record.title,
    date: record.date || null,
    opponent: record.opponent || null,
    url: record.source_url || null
  }));
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
                  'Answer only from the supplied Pacers box score records.',
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
                text: `Conversation history:\n${historyBlock}\n\nQuestion:\n${question}\n\nPacers box score records:\n${buildContextBlock(matches)}`,
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
  classifyQuestion,
  findRelevantRecords,
  sanitizeHistory,
  sanitizeResolvedContext,
  getPriorResolvedContext
};
