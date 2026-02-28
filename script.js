const fallbackData = {
  asOf: "February 20, 2026",
  whatYouMissed: {
    summary: "Latest completed game not available yet.",
    leaders: [],
    highlightVideoUrl: "https://youtu.be/nVnPeY3di08?si=-CGsEbxXmEJdrWuB"
  },
  lotteryStandings: [
    { slot: 1, team: "SAC", record: "12-45", top4: 52.1, first: 14.0 },
    { slot: 2, team: "IND", record: "15-41", top4: 52.1, first: 14.0 },
    { slot: 3, team: "NO", record: "15-41", top4: 52.1, first: 14.0 },
    { slot: 4, team: "BKN", record: "15-39", top4: 45.2, first: 11.5 },
    { slot: 5, team: "WAS", record: "15-39", top4: 45.2, first: 11.5 },
    { slot: 6, team: "UTA", record: "18-38", top4: 37.2, first: 9.0 },
    { slot: 7, team: "DAL", record: "19-35", top4: 31.9, first: 7.5 },
    { slot: 8, team: "MEM", record: "20-33", top4: 26.3, first: 6.0 },
    { slot: 9, team: "CHI", record: "24-32", top4: 20.3, first: 4.5 },
    { slot: 10, team: "MIL", record: "23-30", top4: 13.9, first: 3.0 },
    { slot: 11, team: "ATL", record: "27-30", top4: 8.0, first: 1.5 },
    { slot: 12, team: "CHA", record: "26-30", top4: 9.4, first: 2.0 },
    { slot: 13, team: "POR", record: "27-29", top4: 4.8, first: 1.0 },
    { slot: 14, team: "LAC", record: "27-28", top4: 2.4, first: 0.5 }
  ],
  pacersPickOdds: [
    { pick: 1, pct: 14.0 },
    { pick: 2, pct: 13.4 },
    { pick: 3, pct: 12.7 },
    { pick: 4, pct: 12.0 },
    { pick: 5, pct: 27.8 },
    { pick: 6, pct: 20.0 }
  ],
  pickProtection: {
    text: "Reported protection in the Ivica Zubac deal: Indiana's 2026 first-round pick is protected 1-4 and 10-30.",
    keepSlots: "1-4 and 10-30",
    sendSlots: "5-9"
  },
  keyGames: [
    {
      date: "Sun, Feb 22, 2026",
      opponent: "Dallas Mavericks",
      location: "Home",
      isKey: true,
      why: "Dallas is in the same lower-standings tier and currently just a few games ahead in lottery position."
    }
  ]
};

const FORCED_KEY_OPPONENTS = new Set([
  "sacramento kings",
  "kings",
  "new orleans pelicans",
  "pelicans",
  "brooklyn nets",
  "nets",
  "washington wizards",
  "wizards",
  "utah jazz",
  "jazz",
  "dallas mavericks",
  "mavericks",
  "mavs",
  "memphis grizzlies",
  "grizzlies"
]);

const ABBR_TO_TEAM_ID = {
  ATL: 1610612737, BOS: 1610612738, BKN: 1610612751, CHA: 1610612766, CHI: 1610612741,
  CLE: 1610612739, DAL: 1610612742, DEN: 1610612743, DET: 1610612765, GS: 1610612744,
  HOU: 1610612745, IND: 1610612754, LAC: 1610612746, LAL: 1610612747, MEM: 1610612763,
  MIA: 1610612748, MIL: 1610612749, MIN: 1610612750, NO: 1610612740, NY: 1610612752,
  OKC: 1610612760, ORL: 1610612753, PHI: 1610612755, PHX: 1610612756, POR: 1610612757,
  SAC: 1610612758, SA: 1610612759, TOR: 1610612761, UTA: 1610612762, WAS: 1610612764
};

const NAME_TO_ABBR = {
  "atlanta hawks": "ATL",
  "boston celtics": "BOS",
  "brooklyn nets": "BKN",
  "charlotte hornets": "CHA",
  "chicago bulls": "CHI",
  "cleveland cavaliers": "CLE",
  "dallas mavericks": "DAL",
  "denver nuggets": "DEN",
  "detroit pistons": "DET",
  "golden state warriors": "GS",
  "houston rockets": "HOU",
  "indiana pacers": "IND",
  "la clippers": "LAC",
  "los angeles clippers": "LAC",
  "los angeles lakers": "LAL",
  "memphis grizzlies": "MEM",
  "miami heat": "MIA",
  "milwaukee bucks": "MIL",
  "minnesota timberwolves": "MIN",
  "new orleans pelicans": "NO",
  "new york knicks": "NY",
  "oklahoma city thunder": "OKC",
  "orlando magic": "ORL",
  "philadelphia 76ers": "PHI",
  "phoenix suns": "PHX",
  "portland trail blazers": "POR",
  "sacramento kings": "SAC",
  "san antonio spurs": "SA",
  "toronto raptors": "TOR",
  "utah jazz": "UTA",
  "washington wizards": "WAS"
};

function pct(value) {
  return `${value.toFixed(1)}%`;
}

function normalizeMinuteDisplay(value) {
  const text = String(value || "");
  const iso = text.match(/^PT(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$/);
  if (!iso) {
    return text || "-";
  }
  const mins = Number(iso[1] || 0);
  const secsFloat = Number(iso[2] || 0);
  let secs = Math.round(secsFloat);
  let outMins = mins;
  if (secs === 60) {
    outMins += 1;
    secs = 0;
  }
  return `${outMins}:${String(secs).padStart(2, "0")}`;
}

function isKeepPick(pick) {
  return pick <= 4 || pick >= 10;
}

function logoUrlForAbbr(abbr) {
  const teamId = ABBR_TO_TEAM_ID[abbr];
  if (!teamId) {
    return "";
  }
  return `https://cdn.nba.com/logos/nba/${teamId}/primary/L/logo.svg`;
}

function abbrFromOpponentName(opponent) {
  if (!opponent) {
    return "";
  }
  const normalized = opponent.toLowerCase().trim();
  return NAME_TO_ABBR[normalized] || "";
}

function isForcedKeyOpponent(opponent) {
  const normalized = String(opponent || "").toLowerCase().trim();
  if (!normalized) {
    return false;
  }
  if (FORCED_KEY_OPPONENTS.has(normalized)) {
    return true;
  }
  for (const token of FORCED_KEY_OPPONENTS) {
    if (token.length >= 4 && normalized.includes(token)) {
      return true;
    }
  }
  return false;
}

function sectionMeta(data, key) {
  const sections = data && data.meta && data.meta.sections && typeof data.meta.sections === "object"
    ? data.meta.sections
    : {};
  const value = sections[key];
  return value && typeof value === "object" ? value : {};
}

function upsertStatusBanner(anchorEl, sectionKey, metaObj) {
  if (!anchorEl || !metaObj || typeof metaObj !== "object") {
    return;
  }
  const status = String(metaObj.status || "").toLowerCase();
  const reason = String(metaObj.reason || "").trim();
  const id = `status-banner-${sectionKey}`;
  let banner = document.getElementById(id);
  if (!banner) {
    banner = document.createElement("p");
    banner.id = id;
    banner.className = "status-banner";
    anchorEl.parentNode.insertBefore(banner, anchorEl);
  }
  if (status === "disabled") {
    banner.className = "status-banner status-banner-disabled";
    banner.textContent = `DISABLED / NEEDS SOURCE — ${reason || "no_compliant_source"}`;
    banner.style.display = "";
    return;
  }
  if (status === "stale") {
    banner.className = "status-banner status-banner-stale";
    const lastOk = metaObj.last_ok_at_utc ? ` (last ok: ${metaObj.last_ok_at_utc})` : "";
    banner.textContent = `STALE${lastOk}`;
    banner.style.display = "";
    return;
  }
  if (status === "ok" && reason === "game_in_progress") {
    banner.className = "status-banner status-banner-live";
    banner.textContent = "LIVE — game in progress. Section updates after final.";
    banner.style.display = "";
    return;
  }
  banner.style.display = "none";
}

function buildWhatYouMissed(data) {
  const meta = document.getElementById("missed-meta");
  const grid = document.getElementById("missed-grid");
  upsertStatusBanner(meta, "whatYouMissed", sectionMeta(data, "whatYouMissed"));
  const missed = data.whatYouMissed || {};
  let leaders = Array.isArray(missed.leaders) ? missed.leaders : [];
  const cacheKey = "talkingPacersWhatYouMissed";

  const summaryText = String(missed.summary || "").toLowerCase();
  const summaryUnavailable = summaryText.includes("not available") || summaryText.includes("unavailable");
  const rawSummary = !summaryUnavailable && missed.summary ? missed.summary : fallbackData.whatYouMissed.summary;
  meta.innerHTML = formatLastGameSummary(rawSummary);
  buildMissedHighlight(missed);

  if (leaders.length) {
    try {
      localStorage.setItem(cacheKey, JSON.stringify({ summary: missed.summary || "", leaders }));
    } catch (err) {
      console.warn("Could not cache section 1 data", err);
    }
  } else {
    try {
      const raw = localStorage.getItem(cacheKey);
      if (raw) {
        const cached = JSON.parse(raw);
        if (cached && Array.isArray(cached.leaders) && cached.leaders.length) {
          leaders = cached.leaders;
          if ((summaryUnavailable || !missed.summary) && cached.summary) {
            meta.innerHTML = formatLastGameSummary(cached.summary);
          }
        }
      }
    } catch (err) {
      console.warn("Could not read section 1 cache", err);
    }
  }

  if (!leaders.length) {
    grid.innerHTML = '<p class="small">Latest game leaders are updating. Run refresh again in a minute.</p>';
    return;
  }

  grid.innerHTML = leaders
    .map((leader) => `
      <article class="leader-card">
        <img class="leader-photo" src="${leader.headshot || "https://a.espncdn.com/i/headshots/nba/players/full/0.png"}" alt="${leader.player || "Player"} headshot" onerror="this.onerror=null;this.src='https://a.espncdn.com/i/headshots/nba/players/full/0.png';" />
        <div>
          <p class="leader-statline">${leader.stat || "STAT"}: ${leader.stat === "MIN" ? normalizeMinuteDisplay(leader.value) : (leader.value || "-")}</p>
          <p class="leader-name">${leader.player || "Unknown"}</p>
        </div>
      </article>
    `)
    .join("");

}

function formatLastGameSummary(summary) {
  const s = String(summary || "").trim();
  const m = s.match(/Last game:\s*vs\s+(.+?)\s+\((.+?)\)\s+\|\s+Result:\s+IND\s+(\d+)-(\d+)/i);
  if (!m) {
    return s;
  }
  const opponent = m[1].trim();
  const dateRaw = m[2].trim();
  const indScore = Number(m[3]);
  const oppScore = Number(m[4]);
  const dateMatch = dateRaw.match(/([A-Za-z]{3}),\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})/);
  let niceDate = dateRaw;
  if (dateMatch) {
    const monthMap = { Jan: "January", Feb: "February", Mar: "March", Apr: "April", May: "May", Jun: "June", Jul: "July", Aug: "August", Sep: "September", Oct: "October", Nov: "November", Dec: "December" };
    const month = monthMap[dateMatch[2]] || dateMatch[2];
    niceDate = `${month} ${Number(dateMatch[3])}, ${dateMatch[4]}`;
  }
  const isWin = indScore > oppScore;
  const resultWord = isWin ? "Win" : "Loss";
  const cls = isWin ? "result-win" : "result-loss";
  return `${niceDate} vs ${opponent}: <span class="${cls}">${resultWord}</span> (${indScore}-${oppScore}).`;
}

function toYouTubeEmbedUrl(url) {
  if (!url) {
    return "";
  }
  const text = String(url).trim();
  if (/^https:\/\/www\.youtube\.com\/embed\?/i.test(text)) {
    return text;
  }
  const short = text.match(/youtu\.be\/([A-Za-z0-9_-]{11})/);
  if (short) {
    return `https://www.youtube.com/embed/${short[1]}`;
  }
  const full = text.match(/[?&]v=([A-Za-z0-9_-]{11})/);
  if (full) {
    return `https://www.youtube.com/embed/${full[1]}`;
  }
  const embed = text.match(/youtube\.com\/embed\/([A-Za-z0-9_-]{11})/);
  if (embed) {
    return `https://www.youtube.com/embed/${embed[1]}`;
  }
  return "";
}

function buildMissedHighlight(missed) {
  const el = document.getElementById("missed-highlight");
  if (!el) {
    return;
  }
  const embedUrl = toYouTubeEmbedUrl(missed.highlightVideoUrl || "");
  if (!embedUrl) {
    el.innerHTML = `<p class="status-banner status-banner-disabled">VIDEOS DISABLED — missing uploads playlist id</p>`;
    return;
  }
  el.innerHTML = `
    <h3>Indiana Pacers (Official)</h3>
    <div class="highlight-frame">
      <iframe
        id="official-pacers-uploads"
        src="${embedUrl}"
        title="Indiana Pacers official uploads"
        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
        allowfullscreen
      ></iframe>
      <p id="video-unavailable" class="small">If playback fails, open the official channel directly.</p>
    </div>
  `;
}

function buildStandingsTable(data) {
  const container = document.getElementById("standings-table");
  upsertStatusBanner(container, "standings", sectionMeta(data, "standings"));
  const standings = Array.isArray(data.lotteryStandings) && data.lotteryStandings.length
    ? data.lotteryStandings
    : fallbackData.lotteryStandings;
  const rows = standings
    .map((row) => {
      const logo = logoUrlForAbbr(row.team);
      return `
      <tr class="${row.team === "IND" ? "pacers-row" : ""}">
        <td>${row.slot}</td>
        <td>
          <div class="team-cell">
            ${logo ? `<img class="team-logo" src="${logo}" alt="${row.team} logo" />` : ""}
            <span>${row.team}</span>
          </div>
        </td>
        <td>${row.record}</td>
        <td>${pct(row.top4)}</td>
        <td>${pct(row.first)}</td>
        <td>${(() => {
          const name = row.mockPlayerName || "";
          const url = row.mockPlayerUrl || "";
          if (!name) {
            return "No mock available";
          }
          if (!url || !String(url).startsWith("https://")) {
            return name;
          }
          return `<a href="${url}" target="_blank" rel="noopener noreferrer">${name}</a>`;
        })()}</td>
      </tr>`;
    })
    .join("");

  container.innerHTML = `
    <table class="draft-table" aria-label="Lottery standings and odds">
      <thead>
        <tr>
          <th>Pick</th>
          <th>Team</th>
          <th>Record</th>
          <th>Top-4 Odds</th>
          <th>No. 1 Odds</th>
          <th>Mock</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function buildProtectionSection(data) {
  const summaryEl = document.getElementById("protection-summary");
  upsertStatusBanner(summaryEl, "draft", sectionMeta(data, "draft"));
  const pickOdds = Array.isArray(data.pacersPickOdds) && data.pacersPickOdds.length
    ? data.pacersPickOdds
    : fallbackData.pacersPickOdds;
  const protection = data.pickProtection || fallbackData.pickProtection;
  const keepChance = pickOdds
    .filter((p) => isKeepPick(p.pick))
    .reduce((sum, p) => sum + p.pct, 0);
  const sendChance = pickOdds
    .filter((p) => p.pick >= 5 && p.pick <= 9)
    .reduce((sum, p) => sum + p.pct, 0);

  summaryEl.textContent = protection.text;
  document.getElementById("keep-pct").textContent = pct(keepChance);
  document.getElementById("keep-pct-ring").textContent = pct(keepChance);
  document.getElementById("send-pct").textContent = pct(sendChance);
  document.getElementById("keep-detail").textContent = `Current modeled keep outcomes: picks ${protection.keepSlots}.`;
  document.getElementById("send-detail").textContent = `Current modeled convey outcomes: picks ${protection.sendSlots}.`;

  const keepAngle = Math.max(0, Math.min(360, (keepChance / 100) * 360));
  document.getElementById("odds-ring").style.setProperty("--keep-angle", `${keepAngle.toFixed(1)}deg`);

  const breakdownContainer = document.getElementById("pick-odds-breakdown");
  const pickRows = pickOdds
    .map((row) => {
      const status = isKeepPick(row.pick) ? "Keep" : "Send to LAC";
      return `
        <tr>
          <td>No. ${row.pick}</td>
          <td>${pct(row.pct)}</td>
          <td>${status}</td>
        </tr>`;
    })
    .join("");

  breakdownContainer.innerHTML = `
    <table aria-label="Pacers pick-by-pick odds">
      <thead>
        <tr>
          <th>Pick</th>
          <th>Odds</th>
          <th>Zubac Trade Result</th>
        </tr>
      </thead>
      <tbody>${pickRows}</tbody>
    </table>`;
}

function buildGames(data) {
  const list = document.getElementById("games-list");
  upsertStatusBanner(list, "schedule", sectionMeta(data, "schedule"));
  const games = Array.isArray(data.upcomingGames) && data.upcomingGames.length
    ? data.upcomingGames
    : (Array.isArray(data.keyGames) && data.keyGames.length ? data.keyGames : fallbackData.keyGames);
  if (!games.length) {
    list.innerHTML = '<p class="small">No upcoming games available.</p>';
    return;
  }
  list.innerHTML = games
    .map((game) => {
      const abbr = abbrFromOpponentName(game.opponent);
      const logo = logoUrlForAbbr(abbr);
      const forcedKey = isForcedKeyOpponent(game.opponent);
      const isKey = forcedKey || Boolean(game.isKey) || /within 5 wins/i.test(String(game.why || ""));
      const keyClass = isKey ? "key-game" : "";
      const keyTag = isKey ? '<span class="badge key-badge">Key</span>' : "";
      return `
      <article class="game-item ${keyClass}">
        <h3 class="game-heading">
          ${logo ? `<img class="game-logo" src="${logo}" alt="${game.opponent} logo" />` : ""}
          <span>${game.opponent}</span>
        </h3>
        <p><strong>${game.date}</strong> <span class="badge">${game.location}</span> ${keyTag}</p>
      </article>`;
    })
    .join("");
}

async function loadData() {
  const storageKey = "pacers_dashboard_data_v1";
  try {
    const response = await fetch(`data.json?ts=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Fetch failed with status ${response.status}`);
    }
    const payload = await response.json();
    if (!payload || !Array.isArray(payload.lotteryStandings) || !Array.isArray(payload.pacersPickOdds)) {
      throw new Error("Invalid payload shape");
    }
    try {
      localStorage.setItem(storageKey, JSON.stringify(payload));
    } catch (err) {
      console.warn("Could not cache full payload", err);
    }
    return payload;
  } catch (error) {
    console.warn("Fetch failed; trying localStorage fallback:", error);
    try {
      const raw = localStorage.getItem(storageKey);
      if (raw) {
        const cached = JSON.parse(raw);
        if (cached && Array.isArray(cached.lotteryStandings) && Array.isArray(cached.pacersPickOdds)) {
          return cached;
        }
      }
    } catch (err) {
      console.warn("LocalStorage fallback failed:", err);
    }
    console.warn("Using built-in fallback data");
    return fallbackData;
  }
}

async function init() {
  const data = await loadData();
  const generated = data.meta && data.meta.generated_at_utc
    ? data.meta.generated_at_utc
    : (data.asOf || "");
  const header = document.querySelector(".hero__content");
  if (header) {
    let lastUpdatedEl = document.getElementById("last-updated");
    if (!lastUpdatedEl) {
      lastUpdatedEl = document.createElement("p");
      lastUpdatedEl.id = "last-updated";
      lastUpdatedEl.className = "as-of";
      header.appendChild(lastUpdatedEl);
    }
    lastUpdatedEl.textContent = generated ? `Last updated: ${generated}` : "Last updated: unavailable";
  }

  const sections = data.meta && data.meta.sections && typeof data.meta.sections === "object"
    ? data.meta.sections
    : {};
  const visibleSectionKeys = new Set([
    "whatYouMissed",
    "standings",
    "draft",
    "pickProtection",
    "videos"
  ]);

  const visibleSections = Object.entries(sections).filter(([name]) => visibleSectionKeys.has(name));

  const disabled = visibleSections
    .filter(([, value]) => value && typeof value === "object" && String(value.status || "").toLowerCase() === "disabled")
    .map(([name, value]) => `${name}: ${value.reason || "no_compliant_source"}`);
  const stale = visibleSections
    .filter(([, value]) => value && typeof value === "object" && String(value.status || "").toLowerCase() === "stale")
    .map(([name, value]) => `${name}${value && value.last_ok_at_utc ? ` (last ok: ${value.last_ok_at_utc})` : ""}`);
  if (header && (disabled.length || stale.length)) {
    let notice = document.getElementById("global-status-notice");
    if (!notice) {
      notice = document.createElement("p");
      notice.id = "global-status-notice";
      notice.className = "section-note";
      header.appendChild(notice);
    }
    const bits = [];
    if (disabled.length) {
      bits.push(`DISABLED / NEEDS SOURCE: ${disabled.join("; ")}`);
    }
    if (stale.length) {
      bits.push(`STALE: ${stale.join(", ")}`);
    }
    notice.textContent = bits.join(" | ");
  }
  buildWhatYouMissed(data);
  upsertStatusBanner(document.getElementById("missed-highlight"), "videos", sectionMeta(data, "videos"));
  buildStandingsTable(data);
  buildProtectionSection(data);
  buildGames(data);
}

init();
