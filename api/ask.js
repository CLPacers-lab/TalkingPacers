module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: 'OPENAI_API_KEY is not configured.' });
  }

  try {
    const question = typeof req.body?.question === 'string' ? req.body.question.trim() : '';
    if (!question) {
      return res.status(400).json({ error: 'A question is required.' });
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
                text: 'You are Talking Pacers, a concise Pacers basketball assistant. Answer clearly and directly.',
              },
            ],
          },
          {
            role: 'user',
            content: [
              {
                type: 'input_text',
                text: question,
              },
            ],
          },
        ],
      }),
    });

    const payload = await response.json();
    if (!response.ok) {
      const message = payload?.error?.message || 'OpenAI request failed.';
      return res.status(response.status).json({ error: message });
    }

    const answer = typeof payload?.output_text === 'string' && payload.output_text.trim()
      ? payload.output_text.trim()
      : 'No response returned.';

    return res.status(200).json({ answer });
  } catch (error) {
    console.error('Ask endpoint failed:', error);
    return res.status(500).json({ error: 'Server error while answering question.' });
  }
};
