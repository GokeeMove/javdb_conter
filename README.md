## javdb scraper (Python 3)

Polite scraper for listing pages like `https://javdb.com/?vft=5&vst=3`, with pagination, retries, and CSV/JSON export.

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Usage

```bash
python scrape_javdb.py \
  --url "https://javdb.com/?vft=5&vst=3" \
  --max-pages 3 \
  --out data.json \
  --format json
```

Or CSV:

```bash
python scrape_javdb.py --url "https://javdb.com/?vft=5&vst=3" --max-pages 2 --out data.csv --format csv
```

### Options

- `--url`: Start listing URL (required).
- `--max-pages`: Maximum number of pages to crawl (default: 1).
- `--delay`: Seconds to sleep between page requests (default: 1.5).
- `--timeout`: HTTP timeout seconds (default: 20).
- `--user-agent`: Custom UA string.
- `--cookies`: Raw cookie string like `name=value; name2=value2`.
- `--proxy`: HTTP/HTTPS proxy, e.g. `http://127.0.0.1:7890`.
- `--format`: `json` or `csv`.
- `--out`: Output file path (default: `javdb_output.json`).
 - `--log-level`: Logging level `DEBUG|INFO|WARNING|ERROR` (default: `INFO`).
 - `--log-file`: Write logs to file path (default: stderr).
 - `--translate`: Enable free translation (Japanese -> Chinese) for fields.
 - `--translator`: `mymemory`, `libre`, or `google` (default: `google`).
 - `--source-lang` / `--target-lang`: Language codes, default `ja` -> `zh-CN`.
 - `--mt-delay`: Delay seconds between translation API calls (default: 1.0).
  - `--progress`: Show live translation progress in stderr (default: on).
  - `--translate-default`: Translate titles by default (default: on).
  - `--details`: Fetch each detail page and extract magnets (default: on).
  - `--detail-delay`: Delay seconds between detail requests (default: 1.0).
  - Defaults summary: details on, title translation on, translator `google`, list delay 1.5s, zh-CN target.
  - `--search`: Filter items by keyword. Can be repeated or comma-separated.
  - `--search-to-ja`: Translate each keyword to Japanese first (default: on), then match against original fields (title/actors/categories). Falls back to translated fields.
  - `--search-mode`: `or` (default) or `and` for multi-keyword matching.
  - `--show`: Display results in terminal with Chinese titles, covers, and magnets.

### Notes

- Site may employ anti-bot measures. Provide your own cookies or proxy if needed.
- The parser tries multiple selectors to adapt to layout changes, but you may need to tweak selectors over time.
 - Free translators are rate-limited and best-effort; results may vary and be throttled. Provide your own proxy if needed.

