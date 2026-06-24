#!/usr/bin/env node
/* eslint-disable no-console */

const fs = require('fs');
const path = require('path');

const PACERS_TEAM_ID = '11';
const START_SEASON = 1995;
const CURRENT_YEAR = new Date().getUTCFullYear();
const ESPN_SCHEDULE = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/11/schedule';
const ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary';
const REQUEST_DELAY_MS = 45;

const RAW_OUTPUT_PATH = path.join(process.cwd(), 'data', 'pacers-boxscores-raw.json');
const NORMALIZED_OUTPUT_PATH = path.join(process.cwd(), 'data', 'pacers-boxscores.json');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, retries = 3) {
  let lastError = null;
  for (let i = 0; i < retries; i += 1) {
    try {
      const response = await fetch(url, {
        headers: {
          'User-Agent': 'Talking-Pacers-BoxScores/1.0',
          Accept: 'application/json'
        }
      });
      if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
      }
      return await response.json();
    } catch (error) {
      lastError = error;
      await sleep((i + 1) * 120);
    }
  }
  throw lastError;
}

function seasonLabelFromDate(dateStr) {
  const date = new Date(dateStr);
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  const start = month >= 7 ? year : year - 1;
  const end = String((start + 1) % 100).padStart(2, '0');
  return `${start}-${end}`;
}

function collectPacersTeamContext(summary) {
  const competition = (((summary || {}).header || {}).competitions || [])[0] || {};
  const competitors = competition.competitors || [];
  const pacersComp = competitors.find((team) => String((((team || {}).team || {}).id || '')) === PACERS_TEAM_ID);
  const oppComp = competitors.find((team) => String((((team || {}).team || {}).id || '')) !== PACERS_TEAM_ID);
  if (!pacersComp || !oppComp) {
    return null;
  }

  const pacersScore = Number(pacersComp.score || 0);
  const oppScore = Number(oppComp.score || 0);
  const won = pacersComp.winner === true;

  return {
    competition,
    pacersComp,
    oppComp,
    pacersScore,
    oppScore,
    won,
    homeAway: String(pacersComp.homeAway || '').toUpperCase() === 'HOME' ? 'HOME' : 'AWAY',
    opponent: (((oppComp || {}).team || {}).abbreviation || 'UNK').toUpperCase(),
    result: `${won ? 'W' : 'L'} ${pacersScore}-${oppScore}`,
    playoffs: Number((((summary || {}).header || {}).season || {}).type || 0) === 3
  };
}

function getRecapUrl(summary, context) {
  const competition = (((summary || {}).header || {}).competitions || [])[0] || {};
  const links = Array.isArray(competition.links) ? competition.links : [];
  const direct = links.find((link) => {
    const rel = Array.isArray(link.rel) ? link.rel : [];
    return rel.includes('summary') && rel.includes('desktop') && rel.includes('event') && typeof link.href === 'string';
  });
  if (direct && direct.href) {
    return direct.href;
  }
  if (competition.id) {
    const opp = String((context && context.opponent) || '').toLowerCase();
    return `https://www.espn.com/nba/game/_/gameId/${competition.id}/pacers-${opp || 'opponent'}`;
  }
  return '';
}

function parseStatGroup(statGroup) {
  const labels = Array.isArray(statGroup?.labels) ? statGroup.labels.map((label) => String(label || '').toUpperCase()) : [];
  const athletes = Array.isArray(statGroup?.athletes) ? statGroup.athletes : [];

  return athletes.map((row) => {
    const athlete = row.athlete || {};
    const statMap = {};
    const stats = Array.isArray(row.stats) ? row.stats : [];
    for (let i = 0; i < labels.length; i += 1) {
      statMap[labels[i]] = stats[i] ?? '';
    }
    return {
      player_id: athlete.id || '',
      player: athlete.displayName || 'Unknown',
      starter: row.starter === true,
      did_not_play: row.didNotPlay === true,
      reason: row.reason || '',
      stats: statMap
    };
  });
}

function normalizeSummary(summary) {
  const context = collectPacersTeamContext(summary);
  if (!context) {
    return null;
  }

  const competition = context.competition || {};
  const gameDate = String(competition.date || '').slice(0, 10);
  if (!gameDate || Number(gameDate.slice(0, 4)) < START_SEASON) {
    return null;
  }

  const season = seasonLabelFromDate(competition.date || '');
  const recapUrl = getRecapUrl(summary, context);
  const playersByTeam = ((summary.boxscore || {}).players || []).map((block) => ({
    team: {
      id: (((block || {}).team || {}).id || ''),
      abbreviation: (((block || {}).team || {}).abbreviation || ''),
      displayName: (((block || {}).team || {}).displayName || '')
    },
    player_rows: parseStatGroup((block.statistics || [])[0] || {})
  }));

  const pacersPlayersBlock = playersByTeam.find((block) => String((block.team || {}).id || '') === PACERS_TEAM_ID);
  const opponentPlayersBlock = playersByTeam.find((block) => String((block.team || {}).id || '') !== PACERS_TEAM_ID);

  return {
    game_id: String(competition.id || ''),
    date: gameDate,
    season,
    playoffs: context.playoffs,
    opponent: context.opponent,
    homeAway: context.homeAway,
    result: context.result,
    pacers_score: context.pacersScore,
    opponent_score: context.oppScore,
    recap_url: recapUrl,
    pacers_players: pacersPlayersBlock ? pacersPlayersBlock.player_rows : [],
    opponent_players: opponentPlayersBlock ? opponentPlayersBlock.player_rows : []
  };
}

async function fetchSeasonEventIds(year, seasonType) {
  const url = `${ESPN_SCHEDULE}?season=${year}&seasontype=${seasonType}`;
  const data = await fetchJson(url);
  const events = Array.isArray(data.events) ? data.events : [];
  return events.map((event) => event.id).filter(Boolean);
}

async function main() {
  const allIds = new Set();

  for (let season = START_SEASON; season <= CURRENT_YEAR; season += 1) {
    for (const seasonType of [2, 3]) {
      try {
        const ids = await fetchSeasonEventIds(season, seasonType);
        for (const id of ids) {
          allIds.add(String(id));
        }
      } catch (error) {
        console.warn(`Skipping season ${season}, type ${seasonType}: ${error.message}`);
      }
      await sleep(REQUEST_DELAY_MS);
    }
  }

  const orderedIds = Array.from(allIds).sort((a, b) => Number(a) - Number(b));
  fs.mkdirSync(path.dirname(RAW_OUTPUT_PATH), { recursive: true });

  const rawStream = fs.createWriteStream(RAW_OUTPUT_PATH, { encoding: 'utf8' });
  const normalizedStream = fs.createWriteStream(NORMALIZED_OUTPUT_PATH, { encoding: 'utf8' });
  rawStream.write('[\n');
  normalizedStream.write('[\n');

  let rawCount = 0;
  let normalizedCount = 0;

  for (let i = 0; i < orderedIds.length; i += 1) {
    const eventId = orderedIds[i];
    try {
      const summary = await fetchJson(`${ESPN_SUMMARY}?event=${eventId}`);
      const normalized = normalizeSummary(summary);
      if (!normalized) {
        continue;
      }

      const rawItem = {
        event_id: eventId,
        fetched_from: `${ESPN_SUMMARY}?event=${eventId}`,
        summary
      };

      rawStream.write(`${rawCount > 0 ? ',\n' : ''}${JSON.stringify(rawItem)}`);
      normalizedStream.write(`${normalizedCount > 0 ? ',\n' : ''}${JSON.stringify(normalized)}`);
      rawCount += 1;
      normalizedCount += 1;

      if ((i + 1) % 100 === 0) {
        console.log(`Processed ${i + 1}/${orderedIds.length} games; saved=${normalizedCount}`);
      }
    } catch (error) {
      console.warn(`Skipping event ${eventId}: ${error.message}`);
    }
    await sleep(REQUEST_DELAY_MS);
  }

  rawStream.write('\n]\n');
  normalizedStream.write('\n]\n');
  await Promise.all([
    new Promise((resolve) => rawStream.end(resolve)),
    new Promise((resolve) => normalizedStream.end(resolve)),
  ]);

  console.log(`Wrote ${rawCount} raw summaries to ${RAW_OUTPUT_PATH}`);
  console.log(`Wrote ${normalizedCount} normalized games to ${NORMALIZED_OUTPUT_PATH}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
