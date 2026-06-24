const fs = require('fs');
const path = require('path');

const DATA_PATH = path.join(process.cwd(), 'data', 'remember-this.json');
const NO_DATA_ANSWER = "I don't have that in the TalkingPacers data yet.";
const STOPWORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'did', 'do', 'for', 'from', 'game',
  'games', 'has', 'have', 'how', 'in', 'is', 'it', 'me', 'of', 'on', 'or', 'season',
  'tell', 'that', 'the', 'their', 'them', 'they', 'this', 'to', 'was', 'what', 'when',
  'which', 'who', 'why', 'with'
]);

const OPPONENT_ALIASES = {
  ATL: ['atl', 'hawks', 'atlanta'],
  BOS: ['bos', 'celtics', 'boston'],
  BKN: ['bkn', 'nets', 'brooklyn'],
  CHA: ['cha', 'hornets', 'charlotte'],
  CHI: ['chi', 'bulls', 'chicago'],
  CLE: ['cle', 'cavs', 'cavaliers', 'cleveland'],
  DAL: ['dal', 'mavs', 'mavericks', 'dallas'],
  DEN: ['den', 'nuggets', 'denver'],
  DET: ['det', 'pistons', 'detroit'],
  GS: ['gs', 'gsw', 'warriors', 'golden state'],
  HOU: ['hou', 'rockets', 'houston'],
  IND: ['ind', 'pacers', 'indiana'],
  LAC: ['lac', 'clippers', 'la clippers'],
  LAL: ['lal', 'lakers', 'la lakers'],
  MEM: ['mem', 'grizzlies', 'memphis'],
  MIA: ['mia', 'heat', 'miami'],
  MIL: ['mil', 'bucks', 'milwaukee'],
  MIN: ['min', 'timberwolves', 'wolves', 'minnesota'],
  NO: ['no', 'nop', 'pelicans', 'new orleans'],
  NY: ['ny', 'nyk', 'knicks', 'new york'],
  OKC: ['okc', 'thunder', 'oklahoma city'],
  ORL: ['orl', 'magic', 'orlando'],
  PHI: ['phi', 'sixers', '76ers', 'philadelphia'],
  PHX: ['phx', 'suns', 'phoenix'],
  POR: ['por', 'blazers', 'trail blazers', 'portland'],
  SAC: ['sac', 'kings', 'sacramento'],
  SA: ['sa', 'spurs', 'san antonio'],
  TOR: ['tor', 'raptors', 'toronto'],
  UTAH: ['utah', 'jazz'],
  WAS: ['was', 'wizards', 'washington']
};

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

function loadRememberThis() {
  const raw = fs.readFileSync(DATA_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : [];
}

function tokenize(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, ' ')
    .split(/\s+/)
    .filter((token) => token && !STOPWORDS.has(token) && token.length >= 2);
}

function uniqueTokens(text) {
  return [...new Set(tokenize(text))];
}

function extractSeasonTerms(question) {
  return question.match(/\b\d{4}-\d{2}\b/g) || [];
}

function extractDateTerms(question) {
  return question.match(/\b\d{4}-\d{2}-\d{2}\b/g) || [];
}

function buildRecordText(record) {
  return [
    record.date,
    record.season,
    record.opponent,
    record.homeAway,
    record.result,
    record.player,
    record.stat_line,
    record.category,
    record.why
  ].join(' ').toLowerCase();
}

function scoreRecord(record, questionLower, keywords, seasons, dates) {
  let score = 0;
  const reasons = [];
  const recordText = buildRecordText(record);

  for (const keyword of keywords) {
    if (recordText.includes(keyword)) {
      score += 1;
      reasons.push(`keyword:${keyword}`);
    }
  }

  const playerName = String(record.player || '').trim();
  if (playerName && playerName !== 'Team Event') {
    const playerLower = playerName.toLowerCase();
    if (questionLower.includes(playerLower)) {
      score += 8;
      reasons.push(`player:${playerName}`);
    } else {
      const lastName = playerLower.split(/\s+/).slice(-1)[0];
      if (lastName && lastName.length >= 4 && questionLower.includes(lastName)) {
        score += 5;
        reasons.push(`player-last:${lastName}`);
      }
    }
  }

  const opponent = String(record.opponent || '').toUpperCase();
  const aliases = OPPONENT_ALIASES[opponent] || [opponent.toLowerCase()];
  if (aliases.some((alias) => questionLower.includes(alias))) {
    score += 6;
    reasons.push(`opponent:${opponent}`);
  }

  for (const season of seasons) {
    if (record.season === season) {
      score += 6;
      reasons.push(`season:${season}`);
    }
  }

  for (const date of dates) {
    if (record.date === date) {
      score += 6;
      reasons.push(`date:${date}`);
    }
  }

  return { score, reasons };
}

function findRelevantRecords(question, records) {
  const questionLower = question.toLowerCase();
  const keywords = uniqueTokens(question);
  const seasons = extractSeasonTerms(question);
  const dates = extractDateTerms(question);

  return records
    .map((record) => {
      const { score, reasons } = scoreRecord(record, questionLower, keywords, seasons, dates);
      return { record, score, reasons };
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => {
      if (b.score !== a.score) {
        return b.score - a.score;
      }
      return String(b.record.date || '').localeCompare(String(a.record.date || ''));
    })
    .slice(0, 5);
}

function buildContextBlock(matches) {
  return matches.map(({ record }, index) => {
    return [
      `Record ${index + 1}`,
      `date: ${record.date || 'unknown'}`,
      `season: ${record.season || 'unknown'}`,
      `opponent: ${record.opponent || 'unknown'}`,
      `homeAway: ${record.homeAway || 'unknown'}`,
      `result: ${record.result || 'unknown'}`,
      `player: ${record.player || 'unknown'}`,
      `category: ${record.category || 'unknown'}`,
      `stat_line: ${record.stat_line || 'unknown'}`,
      `why: ${record.why || 'unknown'}`,
      `recap_url: ${record.recap_url || 'unknown'}`
    ].join('\n');
  }).join('\n\n');
}

function formatUsedRecords(matches) {
  return matches.map(({ record }, index) => {
    const playerPart = record.player && record.player !== 'Team Event' ? `, ${record.player}` : '';
    return `${index + 1}. ${record.date || 'unknown date'} vs ${record.opponent || 'unknown'}${playerPart} (${record.category || 'unknown'})`;
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

    const records = loadRememberThis();
    const matches = findRelevantRecords(question, records);

    if (matches.length === 0) {
      return res.status(200).json({ answer: NO_DATA_ANSWER, recordsUsed: [] });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'OPENAI_API_KEY is missing on the server.' });
    }

    const contextBlock = buildContextBlock(matches);
    const usedRecords = formatUsedRecords(matches);

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
                  'Answer only from the supplied TalkingPacers records.',
                  '',
                  "If the answer is not supported by the records, respond:",
                  '',
                  "'I don't have that in the TalkingPacers data yet.'",
                  '',
                  'Do not use outside basketball knowledge.',
                  'Do not guess.',
                  'Mention which records were used.',
                  'Include dates when available.'
                ].join('\n'),
              },
            ],
          },
          {
            role: 'user',
            content: [
              {
                type: 'input_text',
                text: `Question:\n${question}\n\nTalkingPacers records:\n${contextBlock}`,
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

    const answerWithSources = `${answer}\n\nRecords used:\n${usedRecords.join('\n')}`;
    return res.status(200).json({ answer: answerWithSources, recordsUsed: usedRecords });
  } catch (error) {
    console.error('Ask endpoint failed:', error);
    return res.status(500).json({ error: 'Server error while answering question.' });
  }
};
