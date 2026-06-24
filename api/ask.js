const fs = require('fs');
const path = require('path');

const REMEMBER_THIS_PATH = path.join(process.cwd(), 'data', 'remember-this.json');
const CBA_PAGES_PATH = path.join(process.cwd(), 'data', 'cba_pages.json');
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

function extractSeasonTerms(question) {
  return question.match(/\b\d{4}-\d{2}\b/g) || [];
}

function extractDateTerms(question) {
  return question.match(/\b\d{4}-\d{2}-\d{2}\b/g) || [];
}

function loadRememberThis() {
  const raw = fs.readFileSync(REMEMBER_THIS_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    return [];
  }

  return parsed.map((record) => ({
    sourceType: 'remember-this',
    date: record.date || '',
    season: record.season || '',
    opponent: record.opponent || '',
    player: record.player || '',
    label: `${record.date || 'unknown date'} vs ${record.opponent || 'unknown'}`,
    text: [
      record.date,
      record.season,
      record.opponent,
      record.homeAway,
      record.result,
      record.player,
      record.category,
      record.stat_line,
      record.why
    ].join(' '),
    contextLines: [
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
    ]
  }));
}

function loadCbaPages() {
  const raw = fs.readFileSync(CBA_PAGES_PATH, 'utf8');
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    return [];
  }

  return parsed.map((page) => ({
    sourceType: 'cba',
    date: '',
    season: '',
    opponent: '',
    player: '',
    label: `CBA page ${page.page}`,
    text: page.text || '',
    contextLines: [
      `page: ${page.page}`,
      `text: ${page.text || ''}`
    ]
  }));
}

function scoreDocument(document, questionText, keywords, seasons, dates) {
  const docText = normalizeText(document.text);
  let score = 0;

  for (const keyword of keywords) {
    if (docText.includes(keyword)) {
      score += 1;
    }
  }

  const playerName = normalizeText(document.player);
  if (playerName && playerName !== 'team event' && questionText.includes(playerName)) {
    score += 8;
  } else if (playerName) {
    const lastName = playerName.split(/\s+/).slice(-1)[0];
    if (lastName && lastName.length >= 4 && questionText.includes(lastName)) {
      score += 5;
    }
  }

  const opponent = String(document.opponent || '').toUpperCase();
  const aliases = (OPPONENT_ALIASES[opponent] || []).map(normalizeText);
  if (aliases.some((alias) => questionText.includes(alias))) {
    score += 6;
  }

  for (const season of seasons) {
    if (document.season === season) {
      score += 6;
    }
  }

  for (const date of dates) {
    if (document.date === date) {
      score += 6;
    }
  }

  if (document.sourceType === 'cba') {
    const cbaTerms = ['contract', 'salary', 'cap', 'trade', 'waive', 'waiver', 'extension', 'rookie', 'free agent', 'two-way', '10-day', 'option'];
    for (const term of cbaTerms) {
      if (questionText.includes(term) && docText.includes(term)) {
        score += 3;
      }
    }
  }

  return score;
}

function findRelevantDocuments(question, documents) {
  const questionText = normalizeText(question);
  const keywords = uniqueTokens(question);
  const seasons = extractSeasonTerms(question);
  const dates = extractDateTerms(question);

  return documents
    .map((document) => ({
      document,
      score: scoreDocument(document, questionText, keywords, seasons, dates)
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => {
      if (b.score !== a.score) {
        return b.score - a.score;
      }
      return String(b.document.date || '').localeCompare(String(a.document.date || ''));
    })
    .slice(0, 5);
}

function buildContextBlock(matches) {
  return matches.map(({ document }, index) => {
    return [
      `Source ${index + 1}`,
      `source_type: ${document.sourceType}`,
      `label: ${document.label}`,
      ...document.contextLines
    ].join('\n');
  }).join('\n\n');
}

function formatUsedSources(matches) {
  return matches.map(({ document }, index) => `${index + 1}. ${document.label} (${document.sourceType})`);
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

    const documents = [
      ...loadRememberThis(),
      ...loadCbaPages(),
    ];
    const matches = findRelevantDocuments(question, documents);

    if (matches.length === 0) {
      return res.status(200).json({ answer: NO_DATA_ANSWER, recordsUsed: [] });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'OPENAI_API_KEY is missing on the server.' });
    }

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
                  'Answer only from the supplied TalkingPacers records and CBA pages.',
                  'Write like a real person: natural, conversational, and direct.',
                  'Keep it concise unless the question clearly needs more detail.',
                  '',
                  'If the answer is not supported by the supplied material, respond exactly:',
                  '',
                  "'I don't have that in the TalkingPacers data yet.'",
                  '',
                  'Do not use outside basketball knowledge.',
                  'Do not guess.',
                  'Mention which sources you used.',
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
                text: `Question:\n${question}\n\nTalkingPacers source material:\n${buildContextBlock(matches)}`,
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

    const usedSources = formatUsedSources(matches);
    return res.status(200).json({
      answer: `${answer}\n\nSources used:\n${usedSources.join('\n')}`,
      recordsUsed: usedSources,
    });
  } catch (error) {
    console.error('Ask endpoint failed:', error);
    return res.status(500).json({ error: 'Server error while answering question.' });
  }
};
