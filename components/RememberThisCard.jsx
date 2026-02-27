const FALLBACK_HEADSHOT = 'https://a.espncdn.com/i/headshots/nba/players/full/0.png';
const FALLBACK_PACERS_LOGO = 'https://a.espncdn.com/i/teamlogos/nba/500/ind.png';
const TEAM_LOGO_BY_ABBR = {
  ATL: 'https://a.espncdn.com/i/teamlogos/nba/500/atl.png',
  BOS: 'https://a.espncdn.com/i/teamlogos/nba/500/bos.png',
  BKN: 'https://a.espncdn.com/i/teamlogos/nba/500/bkn.png',
  CHA: 'https://a.espncdn.com/i/teamlogos/nba/500/cha.png',
  CHI: 'https://a.espncdn.com/i/teamlogos/nba/500/chi.png',
  CLE: 'https://a.espncdn.com/i/teamlogos/nba/500/cle.png',
  DAL: 'https://a.espncdn.com/i/teamlogos/nba/500/dal.png',
  DEN: 'https://a.espncdn.com/i/teamlogos/nba/500/den.png',
  DET: 'https://a.espncdn.com/i/teamlogos/nba/500/det.png',
  GS: 'https://a.espncdn.com/i/teamlogos/nba/500/gs.png',
  HOU: 'https://a.espncdn.com/i/teamlogos/nba/500/hou.png',
  IND: 'https://a.espncdn.com/i/teamlogos/nba/500/ind.png',
  LAC: 'https://a.espncdn.com/i/teamlogos/nba/500/lac.png',
  LAL: 'https://a.espncdn.com/i/teamlogos/nba/500/lal.png',
  MEM: 'https://a.espncdn.com/i/teamlogos/nba/500/mem.png',
  MIA: 'https://a.espncdn.com/i/teamlogos/nba/500/mia.png',
  MIL: 'https://a.espncdn.com/i/teamlogos/nba/500/mil.png',
  MIN: 'https://a.espncdn.com/i/teamlogos/nba/500/min.png',
  NO: 'https://a.espncdn.com/i/teamlogos/nba/500/no.png',
  NY: 'https://a.espncdn.com/i/teamlogos/nba/500/ny.png',
  OKC: 'https://a.espncdn.com/i/teamlogos/nba/500/okc.png',
  ORL: 'https://a.espncdn.com/i/teamlogos/nba/500/orl.png',
  PHI: 'https://a.espncdn.com/i/teamlogos/nba/500/phi.png',
  PHX: 'https://a.espncdn.com/i/teamlogos/nba/500/phx.png',
  POR: 'https://a.espncdn.com/i/teamlogos/nba/500/por.png',
  SAC: 'https://a.espncdn.com/i/teamlogos/nba/500/sac.png',
  SA: 'https://a.espncdn.com/i/teamlogos/nba/500/sa.png',
  TOR: 'https://a.espncdn.com/i/teamlogos/nba/500/tor.png',
  UTA: 'https://a.espncdn.com/i/teamlogos/nba/500/uta.png',
  WAS: 'https://a.espncdn.com/i/teamlogos/nba/500/was.png'
};

function opponentLogo(entry) {
  const abbr = String((entry && entry.opponent) || '').toUpperCase();
  return TEAM_LOGO_BY_ABBR[abbr] || FALLBACK_PACERS_LOGO;
}

function injectStyles() {
  if (document.getElementById('remember-this-card-styles')) {
    return;
  }

  const style = document.createElement('style');
  style.id = 'remember-this-card-styles';
  style.textContent = `
    .remember-this-root {
      font-family: 'Source Sans 3', sans-serif;
      border: 2px solid #fdbb30;
      background: linear-gradient(145deg, #1f3764 0%, #12284f 60%, #0b1b36 100%);
      color: #fff;
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 10px 24px rgba(0, 0, 0, 0.25);
      max-width: 440px;
      margin: 0 auto;
    }

    .remember-this-root * {
      box-sizing: border-box;
    }

    .remember-this-title {
      margin: 0;
      font-family: 'Oswald', sans-serif;
      font-size: 1.55rem;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: #fdbb30;
    }

    .remember-this-meta {
      margin: 8px 0 12px;
      font-size: 0.95rem;
      opacity: 0.9;
    }

    .remember-this-result {
      margin: 0 0 14px;
      font-weight: 700;
      font-size: 1.05rem;
    }

    .remember-this-body {
      display: grid;
      grid-template-columns: 88px minmax(0, 1fr);
      gap: 12px;
      align-items: start;
    }

    .remember-this-photo {
      width: 88px;
      height: 88px;
      border-radius: 10px;
      object-fit: cover;
      background: rgba(255, 255, 255, 0.12);
      border: 1px solid rgba(255, 255, 255, 0.25);
    }

    .remember-this-statline {
      margin: 0 0 8px;
      font-size: 0.98rem;
      line-height: 1.35;
    }

    .remember-this-why {
      margin: 0;
      font-style: italic;
      opacity: 0.95;
      line-height: 1.35;
    }

    .remember-this-footer {
      margin-top: 14px;
      display: flex;
      align-items: center;
      gap: 8px;
      justify-content: space-between;
    }

    .remember-this-chip {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 0.75rem;
      font-weight: 700;
      letter-spacing: 0.03em;
      background: rgba(253, 187, 48, 0.18);
      border: 1px solid rgba(253, 187, 48, 0.35);
      color: #fddb8a;
      text-transform: uppercase;
    }

    .remember-this-btn {
      border: 0;
      border-radius: 999px;
      padding: 8px 14px;
      font-weight: 700;
      background: #fdbb30;
      color: #1a2b4f;
      cursor: pointer;
    }

    .remember-this-btn:disabled {
      opacity: 0.7;
      cursor: wait;
    }

    .remember-this-status {
      margin: 8px 0 0;
      font-size: 0.88rem;
      opacity: 0.85;
    }

    @media (max-width: 540px) {
      .remember-this-body {
        grid-template-columns: 72px minmax(0, 1fr);
      }

      .remember-this-photo {
        width: 72px;
        height: 72px;
      }
    }
  `;

  document.head.appendChild(style);
}

function randomFrom(items) {
  if (!Array.isArray(items) || items.length === 0) {
    return null;
  }
  return items[Math.floor(Math.random() * items.length)] || null;
}

function chooseLocalEntry(dataset) {
  const trauma = dataset.filter((entry) => entry && entry.trauma === true);
  const normal = dataset.filter((entry) => entry && entry.trauma !== true);
  const roll = Math.random();
  if (roll < 0.2 && trauma.length) {
    return randomFrom(trauma);
  }
  return randomFrom(normal.length ? normal : dataset);
}

async function fetchEntry() {
  const response = await fetch('/api/remember-this', { cache: 'no-store' });
  if (response.ok) {
    const payload = await response.json();
    if (payload && payload.entry) {
      return payload.entry;
    }
    return payload || null;
  }

  // Local static fallback: keep runtime static and avoid hard dependency on serverless APIs.
  const localResponse = await fetch('/data/remember-this.json', { cache: 'no-store' });
  if (!localResponse.ok) {
    throw new Error('Could not load remember-this data.');
  }
  const dataset = await localResponse.json();
  return chooseLocalEntry(Array.isArray(dataset) ? dataset : []);
}

function renderEntry(root, entry) {
  if (!entry) {
    root.innerHTML = '<p class="remember-this-status">No moments available yet.</p>';
    return;
  }

  const chip = entry.trauma ? `TRAUMA: ${entry.trauma_level || 'HIGH'}` : String(entry.category || 'MOMENT');

  root.innerHTML = `
    <h3 class="remember-this-title">REMEMBER THIS?</h3>
    <p class="remember-this-meta">${entry.date || ''} vs ${entry.opponent || 'UNK'} (${entry.homeAway || '-'})</p>
    <p class="remember-this-result">${entry.result || ''}</p>
    <div class="remember-this-body">
      <img class="remember-this-photo" src="${entry.image || entry.headshot || opponentLogo(entry)}" alt="${entry.player || 'Pacers player'} headshot" />
      <div>
        <p class="remember-this-statline"><strong>${entry.player || 'Unknown'}</strong>: ${entry.stat_line || 'No stat line available.'}</p>
        <p class="remember-this-why">${entry.why || 'Basketball happened.'}</p>
      </div>
    </div>
    <div class="remember-this-footer">
      <span class="remember-this-chip">${chip}</span>
      <button type="button" class="remember-this-btn" data-remember-refresh>Another one</button>
    </div>
    <p class="remember-this-status" data-remember-status></p>
  `;

  const img = root.querySelector('.remember-this-photo');
  if (img) {
    img.addEventListener('error', () => {
      img.src = opponentLogo(entry) || FALLBACK_HEADSHOT;
    });
  }
}

export default function RememberThisCard() {
  injectStyles();

  const root = document.createElement('div');
  root.className = 'remember-this-root';
  root.innerHTML = '<p class="remember-this-status">Loading painful memory...</p>';

  async function refresh() {
    const btn = root.querySelector('[data-remember-refresh]');
    const status = root.querySelector('[data-remember-status]');
    if (btn) {
      btn.disabled = true;
      btn.textContent = 'Loading...';
    }
    if (status) {
      status.textContent = '';
    }

    try {
      const entry = await fetchEntry();
      renderEntry(root, entry);
    } catch (error) {
      root.innerHTML = `<p class="remember-this-status">${String(error && error.message ? error.message : error)}</p>`;
    }

    const newBtn = root.querySelector('[data-remember-refresh]');
    if (newBtn) {
      newBtn.disabled = false;
      newBtn.textContent = 'Another one';
      newBtn.addEventListener('click', refresh);
    }
  }

  refresh();
  return root;
}
