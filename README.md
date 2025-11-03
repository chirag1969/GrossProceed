# Gross Proceed Analysis Dashboard

The analysis dashboard hosted on GitHub Pages reads its data directly from the Excel workbook stored in the repository.

## Data source
- Excel file: `analysis/data/daily.xlsx`
- Parsed client-side in the browser using [SheetJS](https://sheetjs.com/) (`xlsx` library).
- The loader first requests the workbook from the same origin at `/GrossProceed/analysis/data/daily.xlsx` and automatically falls back to `https://raw.githubusercontent.com/chirag1969/GrossProceed/main/analysis/data/daily.xlsx` if the primary URL is unavailable.
- Every page load reads the entire used range of the active worksheet without imposing row caps; only rows that are completely empty are skipped during record construction.
- Regular tab visuals render numeric Excel dates as `dd-mm-yyyy` strings while preserving any textual date values verbatim to avoid timezone shifts or locale mutations.

## Freshness and cache busting
- Each request appends a timestamp cache-buster query parameter and uses `cache: "no-store"` to bypass intermediary caches.
- Any registered service workers are unregistered on load to avoid stale cached responses.
- The UI data banner shows which source responded ("same-origin" or "raw.githubusercontent.com") together with the fetch timestamp.

## Updating worksheets or columns
- The main dashboard logic lives in `analysis/js/analysis.js`.
- To switch worksheets or adjust column mappings, update the worksheet candidate arrays and column configuration objects near the top of `analysis/js/analysis.js`.
- The loader returns both the parsed workbook and raw row data; additional sheets can be accessed by updating the relevant candidate lists and column definitions.

No manual build step is required—pushing an updated `daily.xlsx` to `main` automatically refreshes the published dashboard once the page is reloaded.

## Regular tab data sync
- The Regular tab now reads from the same workbook instance that powers the Main tab, including when the page requests a forced refresh.
- When multiple worksheets contain "REGULAR" in their name, the loader prefers an exact "REGULAR" match, then names that start with `REGULAR`, and finally the last matching sheet in the workbook.
- If no worksheet name contains "REGULAR", the tab shows an inline notice while the rest of the dashboard continues to operate.
- To verify freshness, upload a new `analysis/data/daily.xlsx`, reload the dashboard, and confirm that the Regular tab rows mirror the updated workbook.
