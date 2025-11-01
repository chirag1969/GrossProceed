# Gross Proceed Analysis Dashboard

The analysis dashboard hosted on GitHub Pages reads its data directly from the Excel workbook stored in the repository.

## Data source
- Excel file: `analysis/data/daily.xlsx`
- Parsed client-side in the browser using [SheetJS](https://sheetjs.com/) (`xlsx` library).
- The loader automatically requests the most recent version of the workbook from the `main` branch.

## Versioning and cache busting
- `analysis/js/dataLoader.js` calls the GitHub commits API to determine the latest commit touching the workbook.
- The commit SHA and a timestamp are appended to the raw file request to prevent CDN or browser caches from serving stale content.
- The short SHA and commit date are surfaced in the UI (see the “Data version” text near the top of the dashboard).

## Updating worksheets or columns
- The main dashboard logic lives in `analysis/js/analysis.js`.
- To switch worksheets or adjust column mappings, update the worksheet candidate arrays and column configuration objects near the top of `analysis/js/analysis.js`.
- The loader returns both the parsed workbook and raw row data; additional sheets can be accessed by updating the relevant candidate lists and column definitions.

No manual build step is required—pushing an updated `daily.xlsx` to `main` automatically refreshes the published dashboard once the page is reloaded.
