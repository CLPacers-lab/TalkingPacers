#!/usr/bin/env node
/* eslint-disable no-console */

const fs = require('fs');
const path = require('path');

const PACERS_TEAM_ID = '11';
const START_SEASON = 1995;
const CURRENT_YEAR = new Date().getUTCFullYear();
const ESPN_SCHEDULE = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/11/schedule';
const ESPN_SUMMARY = 'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary';
const PACERS_LOGO_URL = 'https://a.espncdn.com/i/teamlogos/nba/500/ind.png';
const TEAM_ID_BY_ABBR = {
  ATL: '1', BOS: '2', BKN: '17', CHA: '30', CHI: '4', CLE: '5', DAL: '6', DEN: '7', DET: '8',
  GS: '9', HOU: '10', IND: '11', LAC: '12', LAL: '13', MEM: '29', MIA: '14', MIL: '15', MIN: '16',
  NO: '3', NY: '18', OKC: '25', ORL: '19', PHI: '20', PHX: '21', POR: '22', SAC: '23', SA: '24',
  TOR: '28', UTA: '26', WAS: '27'
};

const OUTPUT_PATH = path.join(process.cwd(), 'data', 'remember-this.json');
const REQUEST_DELAY_MS = 45;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url, retries = 3) {
  let lastError = null;
  for (let i = 0; i < retries; i += 1) {
    try {
      const response = await fetch(url, {
        headers: {
          'User-Agent': 'Talking-Pacers-RememberThis/1.0',
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

function parseNumberFromStat(value) {
  if (value === undefined || value === null) {
    return 0;
  }
  const match = String(value).match(/-?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : 0;
}

function parseMinutes(value) {
  const text = String(value || '').trim();
  if (!text) {
    return 0;
  }
  if (/^\d+(?:\.\d+)?$/.test(text)) {
    return Number(text);
  }
  const colon = text.match(/^(\d+):(\d{1,2})(?:\.\d+)?$/);
  if (colon) {
    return Number(colon[1]) + (Number(colon[2]) / 60);
  }
  const iso = text.match(/^PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$/i);
  if (iso) {
    return Number(iso[1] || 0) + (Number(iso[2] || 0) / 60);
  }
  return parseNumberFromStat(text);
}

function parseClockToSeconds(play) {
  const display = String((((play || {}).clock || {}).displayValue || '')).trim();
  const colon = display.match(/^(\d+):(\d{1,2})(?:\.(\d+))?$/);
  if (colon) {
    return Number(colon[1]) * 60 + Number(colon[2]) + (colon[3] ? Number(`0.${colon[3]}`) : 0);
  }
  const text = String((play || {}).text || '').toLowerCase();
  const textClock = text.match(/(\d):(\d{2})(?:\.(\d+))?/);
  if (textClock) {
    return Number(textClock[1]) * 60 + Number(textClock[2]) + (textClock[3] ? Number(`0.${textClock[3]}`) : 0);
  }
  return Infinity;
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

function statMapForAthlete(athleteRow, labels) {
  const out = {};
  const stats = (athleteRow || {}).stats || [];
  for (let i = 0; i < labels.length; i += 1) {
    out[String(labels[i] || '').toUpperCase()] = stats[i];
  }
  return out;
}

function createBaseEntry(context, summary, player, statLine, category, why) {
  const hasPlayerHeadshot = Boolean(player && player.id && player.id !== '0');
  const opponentLogo = getOpponentLogo(context);
  return {
    date: String((((summary || {}).header || {}).competitions || [])[0]?.date || '').slice(0, 10),
    season: seasonLabelFromDate((((summary || {}).header || {}).competitions || [])[0]?.date || ''),
    opponent: context.opponent,
    homeAway: context.homeAway,
    result: context.result,
    playoffs: context.playoffs,
    player: player && player.name ? player.name : 'Unknown',
    stat_line: statLine,
    category,
    trauma: false,
    why,
    image: hasPlayerHeadshot ? `https://a.espncdn.com/i/headshots/nba/players/full/${player.id}.png` : opponentLogo
  };
}

function getOpponentLogo(context) {
  const oppTeam = (((context || {}).oppComp || {}).team || {});
  const logos = Array.isArray(oppTeam.logos) ? oppTeam.logos : [];
  const defaultLogo = logos.find((logo) => Array.isArray(logo.rel) && logo.rel.includes('default'));
  const bestLogo = defaultLogo || logos[0];
  if (bestLogo && bestLogo.href) {
    return bestLogo.href;
  }
  if (oppTeam.logo) {
    return oppTeam.logo;
  }
  const abbr = String((oppTeam.abbreviation || (context || {}).opponent || '')).toUpperCase();
  const id = TEAM_ID_BY_ABBR[abbr];
  if (id) {
    return `https://a.espncdn.com/i/teamlogos/nba/500/${abbr.toLowerCase()}.png`;
  }
  return PACERS_LOGO_URL;
}

function traumaify(entry, why) {
  return {
    ...entry,
    category: 'TRAUMA',
    trauma: true,
    trauma_level: 'HIGH',
    why
  };
}

function getPacersPeakLead(summary, context) {
  const winProb = summary.winprobability || [];
  let pacersPeakLead = 0;
  for (const wp of winProb) {
    const homeWin = Number(wp.homeWinPercentage || 0);
    const diff = Number(wp.homeScore || 0) - Number(wp.awayScore || 0);
    const pacersHome = String(context.pacersComp.homeAway || '').toLowerCase() === 'home';
    const pacersEdge = pacersHome ? diff : -diff;
    if (pacersEdge > pacersPeakLead) {
      pacersPeakLead = pacersEdge;
    }
    const impliedLead = pacersHome ? Math.round((homeWin - 0.5) * 40) : Math.round(((1 - homeWin) - 0.5) * 40);
    if (impliedLead > pacersPeakLead) {
      pacersPeakLead = impliedLead;
    }
  }
  return pacersPeakLead;
}

function detectPlayerMoments(summary, context, runningAverages, seasonKey) {
  const events = [];
  const boxscorePlayers = ((summary || {}).boxscore || {}).players || [];
  const pacersPlayerBlock = boxscorePlayers.find((block) => String(((block || {}).team || {}).id || '') === PACERS_TEAM_ID);
  if (!pacersPlayerBlock) {
    return events;
  }

  const statGroup = (pacersPlayerBlock.statistics || [])[0] || {};
  const labels = (statGroup.labels || []).map((label) => String(label || '').toUpperCase());
  const athletes = statGroup.athletes || [];

  for (const row of athletes) {
    const athlete = row.athlete || {};
    const statMap = statMapForAthlete(row, labels);
    const name = athlete.displayName || 'Unknown';
    const player = { name, id: athlete.id || '0' };
    const points = parseNumberFromStat(statMap.PTS);
    const rebounds = parseNumberFromStat(statMap.REB);
    const assists = parseNumberFromStat(statMap.AST);
    const turnovers = parseNumberFromStat(statMap.TO);
    const steals = parseNumberFromStat(statMap.STL);
    const blocks = parseNumberFromStat(statMap.BLK);
    const minutes = parseMinutes(statMap.MIN);
    const isBench = row.starter === false;

    const statLine = `${points} pts, ${rebounds} reb, ${assists} ast`;
    const avgKey = `${seasonKey}:${name}`;
    const avg = runningAverages[avgKey] || { games: 0, points: 0 };
    const preGameAverage = avg.games > 0 ? avg.points / avg.games : 0;

    let randomWhy = '';
    if (isBench && points >= 30) {
      randomWhy = `Bench detonation: ${name} dropped ${points} off the bench.`;
    } else if (points >= 45) {
      randomWhy = `${name} went for ${points}. Full orbital launch.`;
    } else if (points >= 35 && preGameAverage < 12 && avg.games >= 8) {
      randomWhy = `${name} averaged ${preGameAverage.toFixed(1)} then exploded for ${points}.`;
    } else if (points >= 25 && minutes > 0 && minutes < 25) {
      randomWhy = `${name} scored ${points} in ${minutes.toFixed(1)} minutes.`;
    }
    if (randomWhy) {
      events.push(createBaseEntry(context, summary, player, statLine, 'RANDOM_EXPLOSION', randomWhy));
    }

    const tripleDoubleBuckets = [points, rebounds, assists, steals, blocks].filter((value) => value >= 10).length;
    let chaosWhy = '';
    if (tripleDoubleBuckets >= 3 && turnovers >= 7) {
      chaosWhy = `${name} posted a triple-double and ${turnovers} turnovers. Maximum volatility.`;
    } else if (turnovers >= 10) {
      chaosWhy = `${name} turned it over ${turnovers} times and somehow it was still a game.`;
    } else if (steals >= 6) {
      chaosWhy = `${name} ripped ${steals} steals in one night.`;
    } else if (blocks >= 7) {
      chaosWhy = `${name} rejected ${blocks} shots like a bouncer.`;
    } else if (assists >= 15 && turnovers === 0) {
      chaosWhy = `${name} posted ${assists} assists with 0 turnovers. Surgical control.`;
    }
    if (chaosWhy) {
      events.push(createBaseEntry(context, summary, player, statLine, 'CHAOS', chaosWhy));
    }

    runningAverages[avgKey] = {
      games: avg.games + 1,
      points: avg.points + points
    };
  }

  return events;
}

function parseTeamStats(summary, context) {
  const teamRows = ((summary || {}).boxscore || {}).teams || [];
  const pacersRow = teamRows.find((row) => String(((row || {}).team || {}).id || '') === PACERS_TEAM_ID) || {};

  function findStat(row, key) {
    const stats = row.statistics || [];
    const stat = stats.find((item) => String(item.name || '').toUpperCase() === key || String(item.displayName || '').toUpperCase() === key);
    return stat ? String(stat.displayValue || stat.value || '') : '';
  }

  const fgPct = parseNumberFromStat(findStat(pacersRow, 'FIELD GOAL PCT')) || parseNumberFromStat(findStat(pacersRow, 'FG%'));
  return {
    fgPct,
    pacersScore: context.pacersScore,
    oppScore: context.oppScore,
    won: context.won
  };
}

function detectTeamWeirdness(summary, context) {
  const items = [];
  const team = parseTeamStats(summary, context);
  const statLine = `${context.pacersScore} pts, opp ${context.oppScore} pts`;
  const player = { name: 'Team Event', id: '0' };

  if (team.won && team.fgPct > 0 && team.fgPct < 35) {
    items.push(createBaseEntry(context, summary, player, statLine, 'TEAM_WEIRDNESS', `Pacers won while shooting ${team.fgPct.toFixed(1)}%. Basketball is fake.`));
  }
  if (!team.won && team.pacersScore >= 130) {
    items.push(createBaseEntry(context, summary, player, statLine, 'TEAM_WEIRDNESS', 'Dropped 130+ and still lost. Defense dissolved completely.'));
  }
  if (team.oppScore >= 150) {
    items.push(createBaseEntry(context, summary, player, statLine, 'TEAM_WEIRDNESS', 'Allowed 150+. Historical nonsense occurred.'));
  }

  const pacersPeakLead = getPacersPeakLead(summary, context);
  if (!team.won && pacersPeakLead >= 20) {
    items.push(createBaseEntry(context, summary, player, statLine, 'TEAM_WEIRDNESS', `Blew a ${Math.round(pacersPeakLead)}-point lead.`));
  }

  const detail = String((((context.competition || {}).status || {}).type || {}).detail || '').toLowerCase();
  const margin = Math.abs(team.pacersScore - team.oppScore);
  if (!team.won && /ot/.test(detail) && (margin === 1 || margin === 2)) {
    items.push(createBaseEntry(context, summary, player, statLine, 'TEAM_WEIRDNESS', `Overtime loss by ${margin}. Painfully precise.`));
  }

  return items;
}

function detectTrauma(summary, context) {
  const moments = [];
  if (context.won) {
    return moments;
  }

  const base = createBaseEntry(
    context,
    summary,
    { name: 'Team Event', id: '0' },
    `${context.pacersScore} pts, opp ${context.oppScore} pts`,
    'TRAUMA',
    'Hope briefly existed.'
  );

  const detailUpper = String((((context.competition || {}).status || {}).type || {}).detail || '').toUpperCase();
  const series = (context.competition.series || []).find((entry) => String(entry.type || '').toLowerCase() === 'playoff');
  const summaryText = String((series || {}).summary || '').toUpperCase();
  const seriesTitle = `${String((series || {}).title || '')} ${String((series || {}).description || '')}`.toUpperCase();

  const traumaReasons = [];
  if (context.playoffs && /WINS SERIES/.test(summaryText) && !summaryText.includes('IND')) {
    traumaReasons.push('Playoff elimination confirmed. Remote tossed.');
  }
  if (context.playoffs && /GAME 7/.test(detailUpper)) {
    traumaReasons.push('Game 7 loss. Historic pain tier unlocked.');
  }
  if (context.playoffs && (seriesTitle.includes('CONFERENCE FINALS') || seriesTitle.includes('NBA FINALS'))) {
    traumaReasons.push('ECF/Finals loss. Banner-adjacent heartbreak.');
  }
  if (context.playoffs && ['NY', 'MIA'].includes(context.opponent)) {
    traumaReasons.push(`${context.opponent} playoff loss. Annual emotional tax paid.`);
  }

  const pacersPeakLead = getPacersPeakLead(summary, context);
  if (pacersPeakLead >= 20) {
    traumaReasons.push(`Led by ${Math.round(pacersPeakLead)} and still lost. Hope briefly existed.`);
  }

  const plays = summary.plays || [];
  const lateOpponentGameWinner = plays.some((play) => {
    const text = String(play.text || '').toLowerCase();
    if (!/(makes|made|hits|hit)/.test(text)) {
      return false;
    }
    const secondsLeft = parseClockToSeconds(play);
    if (secondsLeft > 3) {
      return false;
    }
    return text.includes('pacers') ? false : true;
  });
  if (lateOpponentGameWinner) {
    traumaReasons.push('Buzzer-beater loss in the final 3 seconds.');
  }

  if (traumaReasons.length > 0) {
    moments.push(traumaify(base, traumaReasons[0]));
  }

  return moments;
}

function dedupeEntries(entries) {
  const seen = new Set();
  const out = [];
  for (const entry of entries) {
    const key = [entry.date, entry.opponent, entry.player, entry.category, entry.why].join('|');
    if (!seen.has(key)) {
      seen.add(key);
      out.push(entry);
    }
  }
  return out;
}

function entryQualityScore(entry) {
  let score = 0;
  if (entry.trauma === true) {
    score += 100;
  }
  if (entry.category === 'RANDOM_EXPLOSION' || entry.category === 'CHAOS') {
    score += 25;
  }
  const points = parseNumberFromStat(String(entry.stat_line || '').split(' pts')[0]);
  score += Math.min(points, 60);
  return score;
}

async function fetchSeasonEventIds(year, seasontype) {
  const url = `${ESPN_SCHEDULE}?season=${year}&seasontype=${seasontype}`;
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
  const runningAverages = {};
  const collected = [];

  for (let i = 0; i < orderedIds.length; i += 1) {
    const eventId = orderedIds[i];
    try {
      const summary = await fetchJson(`${ESPN_SUMMARY}?event=${eventId}`);
      const context = collectPacersTeamContext(summary);
      if (!context) {
        continue;
      }

      const gameDate = String((context.competition || {}).date || '').slice(0, 10);
      if (!gameDate || Number(gameDate.slice(0, 4)) < START_SEASON) {
        continue;
      }

      const seasonKey = seasonLabelFromDate((context.competition || {}).date || '');
      collected.push(...detectPlayerMoments(summary, context, runningAverages, seasonKey));
      collected.push(...detectTeamWeirdness(summary, context));
      collected.push(...detectTrauma(summary, context));

      if ((i + 1) % 100 === 0) {
        console.log(`Processed ${i + 1}/${orderedIds.length} games; entries=${collected.length}`);
      }
    } catch (error) {
      console.warn(`Skipping event ${eventId}: ${error.message}`);
    }
    await sleep(REQUEST_DELAY_MS);
  }

  let finalEntries = dedupeEntries(collected)
    .sort((a, b) => String(b.date).localeCompare(String(a.date)));

  if (finalEntries.length > 400) {
    finalEntries = finalEntries
      .slice()
      .sort((a, b) => entryQualityScore(b) - entryQualityScore(a) || String(b.date).localeCompare(String(a.date)))
      .slice(0, 400)
      .sort((a, b) => String(b.date).localeCompare(String(a.date)));
  }

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(finalEntries, null, 2)}\n`, 'utf-8');

  const traumaCount = finalEntries.filter((entry) => entry.trauma).length;
  console.log(`Wrote ${finalEntries.length} entries to ${OUTPUT_PATH}`);
  console.log(`Trauma entries: ${traumaCount}`);
  if (finalEntries.length < 250) {
    console.warn('Warning: dataset has fewer than 250 entries; consider relaxing one threshold.');
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
