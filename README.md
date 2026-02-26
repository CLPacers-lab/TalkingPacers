# Talking Pacers

Policy mode is enabled by default:
- `no_scraping=true`
- `require_compliant_sources=true`

Blocked:
- HTML scraping
- Embedded JSON scraped from HTML pages
- PDF text extraction scraping
- Tankathon / nbadraft.net / basketball-reference scraping paths

Allowed:
- Official structured APIs (NBA CDN/data.nba.net, ESPN structured APIs)
- Manual override JSON files
- Paid/licensed provider adapters

## Run updater

```bash
python3 scripts/update_data.py
```

Optional flags:

```bash
python3 scripts/update_data.py --fast
python3 scripts/update_data.py --safe
```

## Local web server (auto-refresh on open)

To ensure `data.json` is refreshed from network when you open the page locally:

```bash
python3 scripts/dev_server.py
```

Then open:

```text
http://localhost:8000
```

Notes:
- The server refreshes data on `/`, `/index.html`, and `/data.json` requests.
- It uses a short refresh throttle (default 60s). Override with `PACERS_REFRESH_MIN_SECONDS`.

## How to make everything green

1. Standings / Schedule / Scoreboard / Boxscores / Players
- Ensure NBA/ESPN structured endpoints are reachable from your network.
- Default sources are already configured in `scripts/update_data.py`.

2. Draft (`manual_or_paid_api`)
- Populate `mock_override.json`, or wire a paid draft API adapter with `PACERS_DRAFT_API_KEY`.
- If neither is configured, Draft section is `DISABLED / NEEDS SOURCE`.

3. Videos (`youtube_embed_or_youtube_api_or_manual`)
- Set `youtube.uploads_playlist_id` in `config.json` (copy/paste the official Pacers uploads playlist id, starts with `UU`).
- Example embed URL built by app: `https://www.youtube.com/embed?listType=playlist&list=<uploads_playlist_id>`.
- If `youtube.uploads_playlist_id` is empty, Videos section is `DISABLED / NEEDS SOURCE`.

5. Stats.NBA.com
- Blocked by default (`allow_stats_nba_com=false`).
- Only enable explicitly in `config.json` if you have a compliance reason.

## Disabled meaning

If a section is non-compliant or missing compliant inputs, `meta.sections.<section>` is set to:
- `status: "disabled"`
- `reason: "policy_no_scrape" | "missing_api_key" | "no_compliant_source" | "manual_override_missing"`

The UI renders visible:
- per-section `DISABLED / NEEDS SOURCE — <reason>`
- global disabled banner at the top

## Override templates

- `mock_override.json`: draft pick -> player mapping
- `videos_override.json`: YouTube video IDs

These files are templates by default and do not count as live data until you fill valid values.
