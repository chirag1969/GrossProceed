(function (global) {
  if (typeof global !== 'object' || global === null) {
    return;
  }

  const WORKBOOK_PATH = '/GrossProceed/analysis/data/daily.xlsx';
  const RAW_BASE_URL = 'https://raw.githubusercontent.com/chirag1969/GrossProceed/main/analysis/data/daily.xlsx';

  let cachedPromise = null;
  let serviceWorkerUnregistered = false;

  function ensureServiceWorkerUnregistered() {
    if (serviceWorkerUnregistered) {
      return;
    }
    serviceWorkerUnregistered = true;
    if (typeof navigator !== 'undefined'
      && navigator
      && typeof navigator.serviceWorker !== 'undefined'
      && navigator.serviceWorker
      && typeof navigator.serviceWorker.getRegistrations === 'function') {
      navigator.serviceWorker.getRegistrations()
        .then((registrations) => {
          registrations.forEach((registration) => {
            if (registration && typeof registration.unregister === 'function') {
              registration.unregister().catch((error) => {
                console.warn('Failed to unregister service worker', error);
              });
            }
          });
        })
        .catch((error) => {
          console.warn('Unable to inspect service workers for unregister', error);
        });
    }
  }

  function normaliseOptions(options) {
    if (!options || typeof options !== 'object') {
      return {};
    }
    return options;
  }

  function pad(number) {
    return number < 10 ? `0${number}` : `${number}`;
  }

  function formatDate(value) {
    if (!value) {
      return '';
    }
    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      const year = value.getUTCFullYear();
      const month = pad(value.getUTCMonth() + 1);
      const day = pad(value.getUTCDate());
      return `${year}-${month}-${day}`;
    }
    return '';
  }

  function formatDateTime(value) {
    if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
      return '';
    }
    const year = value.getUTCFullYear();
    const month = pad(value.getUTCMonth() + 1);
    const day = pad(value.getUTCDate());
    const hours = pad(value.getUTCHours());
    const minutes = pad(value.getUTCMinutes());
    return `${year}-${month}-${day} ${hours}:${minutes} UTC`;
  }

  function normaliseCellValue(cell, workbookOptions) {
    if (cell === null || cell === undefined) {
      return '';
    }
    if (cell instanceof Date && !Number.isNaN(cell.getTime())) {
      return formatDate(cell);
    }
    if (typeof cell === 'number' && Number.isFinite(cell)) {
      const date = XLSX.SSF.parse_date_code(cell, { date1904: !!(workbookOptions && workbookOptions.date1904) });
      if (date && date.y && date.m && date.d) {
        return `${date.y}-${pad(date.m)}-${pad(date.d)}`;
      }
      return cell;
    }
    return cell;
  }

  function normaliseRow(row, workbookOptions) {
    return row.map((cell) => normaliseCellValue(cell, workbookOptions));
  }

  function buildRecords(rows) {
    if (!Array.isArray(rows) || rows.length === 0) {
      return [];
    }
    const header = Array.isArray(rows[0]) ? rows[0] : [];
    const records = [];
    for (let index = 1; index < rows.length; index += 1) {
      const sourceRow = Array.isArray(rows[index]) ? rows[index] : [];
      const record = {};
      header.forEach((key, keyIndex) => {
        const normalisedKey = key === null || key === undefined ? '' : String(key).trim();
        if (!normalisedKey) {
          return;
        }
        record[normalisedKey] = keyIndex < sourceRow.length ? sourceRow[keyIndex] : '';
      });
      records.push(record);
    }
    return records;
  }

  function resolveSheetName(workbook, options) {
    const sheetNames = Array.isArray(workbook.SheetNames) ? workbook.SheetNames : [];
    if (!sheetNames.length) {
      return null;
    }
    if (typeof options.sheetName === 'string' && options.sheetName.trim().length) {
      const target = options.sheetName.trim();
      if (sheetNames.includes(target)) {
        return target;
      }
    }
    if (Number.isInteger(options.sheetIndex)) {
      const safeIndex = Math.max(0, Math.min(sheetNames.length - 1, options.sheetIndex));
      return sheetNames[safeIndex];
    }
    return sheetNames[0];
  }

  function buildAttemptUrl(source, cacheBuster) {
    const defaultCacheBuster = cacheBuster || Date.now().toString();
    try {
      if (source.type === 'same-origin') {
        if (global && global.location && typeof global.location.origin === 'string') {
          const url = new URL(WORKBOOK_PATH, global.location.origin);
          url.searchParams.set('cb', defaultCacheBuster);
          return {
            href: url.toString(),
            displayUrl: WORKBOOK_PATH,
          };
        }
        const url = new URL(WORKBOOK_PATH, 'https://example.com');
        url.searchParams.set('cb', defaultCacheBuster);
        return {
          href: url.toString(),
          displayUrl: WORKBOOK_PATH,
        };
      }
      const url = new URL(RAW_BASE_URL);
      url.searchParams.set('cb', defaultCacheBuster);
      return {
        href: url.toString(),
        displayUrl: RAW_BASE_URL,
      };
    } catch (error) {
      return {
        href: `${source.type === 'same-origin' ? WORKBOOK_PATH : RAW_BASE_URL}?cb=${defaultCacheBuster}`,
        displayUrl: source.type === 'same-origin' ? WORKBOOK_PATH : RAW_BASE_URL,
      };
    }
  }

  async function fetchWorkbookFromSource(source, cacheBuster) {
    const { href, displayUrl } = buildAttemptUrl(source, cacheBuster);
    const response = await fetch(href, {
      cache: 'no-store',
      headers: {
        'Cache-Control': 'no-store',
        Pragma: 'no-cache',
      },
    });
    if (!response.ok) {
      const error = new Error(`Request to ${displayUrl} failed with status ${response.status} ${response.statusText || ''}`.trim());
      error.status = response.status;
      error.statusText = response.statusText;
      error.url = displayUrl;
      error.finalUrl = href;
      throw error;
    }
    const buffer = await response.arrayBuffer();
    const fetchCompletedAt = new Date();
    const lastModifiedHeader = response.headers ? response.headers.get('last-modified') : null;
    const lastModifiedDate = lastModifiedHeader ? new Date(lastModifiedHeader) : null;
    const metadata = {
      sourceType: source.type,
      sourceLabel: source.label,
      url: displayUrl,
      finalUrl: href,
      cacheBuster: cacheBuster,
      fetchTimestamp: fetchCompletedAt,
      fetchTimestampISO: fetchCompletedAt.toISOString(),
      fetchTimestampDisplay: formatDateTime(fetchCompletedAt),
      lastModifiedISO: lastModifiedDate && !Number.isNaN(lastModifiedDate.getTime()) ? lastModifiedDate.toISOString() : '',
      lastModifiedDisplay: lastModifiedDate && !Number.isNaN(lastModifiedDate.getTime()) ? formatDateTime(lastModifiedDate) : '',
    };
    return { buffer, metadata };
  }

  async function fetchWorkbookBuffer() {
    const cacheBuster = Date.now().toString();
    const sources = [
      { type: 'same-origin', label: 'same-origin' },
      { type: 'raw.githubusercontent.com', label: 'raw.githubusercontent.com' },
    ];

    const errors = [];

    // Try primary source first, then fallback.
    for (let index = 0; index < sources.length; index += 1) {
      const source = sources[index];
      try {
        const result = await fetchWorkbookFromSource(source, cacheBuster);
        return result;
      } catch (error) {
        const detail = {
          source: source.label,
          message: error && error.message ? error.message : 'Unknown error',
        };
        if (typeof error.status === 'number') {
          detail.status = error.status;
        }
        if (typeof error.statusText === 'string' && error.statusText.length) {
          detail.statusText = error.statusText;
        }
        if (error && typeof error.finalUrl === 'string') {
          detail.finalUrl = error.finalUrl;
        }
        errors.push(detail);
        console.error(`Failed to load workbook from ${source.label}:`, error);
      }
    }

    const summary = errors.length ? errors.map((error) => {
      const parts = [];
      parts.push(`${error.source}:`);
      if (error.message) {
        parts.push(error.message);
      }
      if (error.finalUrl) {
        parts.push(`(${error.finalUrl})`);
      }
      return parts.join(' ');
    }).join(' ') : 'No additional error details available.';

    const finalError = new Error(`Unable to fetch Excel workbook. ${summary} Please hard refresh the page and contact the maintainer if the issue persists.`);
    finalError.attempts = errors;
    throw finalError;
  }

  async function loadExcelData(options = {}) {
    const normalisedOptions = normaliseOptions(options);
    if (cachedPromise && normalisedOptions.forceReload !== true) {
      return cachedPromise;
    }
    if (typeof XLSX === 'undefined') {
      return Promise.reject(new Error('Excel parser library is not available'));
    }

    ensureServiceWorkerUnregistered();

    cachedPromise = (async () => {
      const { buffer, metadata } = await fetchWorkbookBuffer();
      const workbook = XLSX.read(buffer, {
        type: 'array',
        cellDates: true,
        cellNF: false,
        cellText: false,
      });
      const workbookOptions = {
        date1904: !!(workbook && workbook.Workbook && workbook.Workbook.WBProps && workbook.Workbook.WBProps.date1904),
      };
      const sheetName = resolveSheetName(workbook, normalisedOptions);
      const worksheet = sheetName ? workbook.Sheets[sheetName] : null;
      let rows = [];
      if (worksheet) {
        const rawRows = XLSX.utils.sheet_to_json(worksheet, {
          header: 1,
          blankrows: false,
          defval: '',
          raw: true,
        });
        rows = rawRows.map((row) => (Array.isArray(row) ? normaliseRow(row, workbookOptions) : []));
      }
      const records = buildRecords(rows);
      return {
        workbook,
        sheetName,
        rows,
        records,
        version: metadata,
      };
    })().catch((error) => {
      cachedPromise = null;
      throw error;
    });

    return cachedPromise;
  }

  global.loadExcelData = loadExcelData;
})(typeof window !== 'undefined' ? window : this);
