# Gross Proceed Analysis Dashboard

The analysis dashboard hosted on GitHub Pages reads its data directly from the Excel workbook stored in the repository.

## Data source
- Excel file: `analysis/data/daily.xlsx`
- Parsed client-side in the browser using [SheetJS](https://sheetjs.com/) (`xlsx` library).
- The loader first requests the workbook from the same origin at `/GrossProceed/analysis/data/daily.xlsx` and automatically falls back to `https://raw.githubusercontent.com/chirag1969/GrossProceed/main/analysis/data/daily.xlsx` if the primary URL is unavailable.

## Freshness and cache busting
- Each request appends a timestamp cache-buster query parameter and uses `cache: "no-store"` to bypass intermediary caches.
- Any registered service workers are unregistered on load to avoid stale cached responses.
- The UI data banner shows which source responded ("same-origin" or "raw.githubusercontent.com") together with the fetch timestamp.

## Updating worksheets or columns
- The main dashboard logic lives in `analysis/js/analysis.js`.
- To switch worksheets or adjust column mappings, update the worksheet candidate arrays and column configuration objects near the top of `analysis/js/analysis.js`.
- The loader returns both the parsed workbook and raw row data; additional sheets can be accessed by updating the relevant candidate lists and column definitions.

No manual build step is required—pushing an updated `daily.xlsx` to `main` automatically refreshes the published dashboard once the page is reloaded.
