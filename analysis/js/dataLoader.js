(function (global) {
  if (typeof global !== 'object' || global === null) {
    return;
  }

  const REPO_OWNER = 'chirag1969';
  const REPO_NAME = 'GrossProceed';
  const DEFAULT_BRANCH = 'main';
  const WORKBOOK_PATH = 'analysis/data/daily.xlsx';
  const COMMITS_ENDPOINT = `https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/commits`;
  const RAW_BASE_URL = `https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${DEFAULT_BRANCH}/${WORKBOOK_PATH}`;

  let cachedPromise = null;

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

  async function fetchLatestCommit() {
    const url = new URL(COMMITS_ENDPOINT);
    url.searchParams.set('path', WORKBOOK_PATH);
    url.searchParams.set('per_page', '1');
    url.searchParams.set('page', '1');
    const response = await fetch(url.toString(), {
      cache: 'no-store',
      headers: {
        'Cache-Control': 'no-cache',
        Pragma: 'no-cache',
      },
    });
    if (!response.ok) {
      throw new Error(`Failed to resolve latest commit for ${WORKBOOK_PATH}`);
    }
    const body = await response.json();
    if (!Array.isArray(body) || !body.length) {
      throw new Error('No commit history available for workbook');
    }
    const commit = body[0];
    const sha = typeof commit.sha === 'string' ? commit.sha : '';
    const commitDate = commit && commit.commit && commit.commit.committer && commit.commit.committer.date
      ? commit.commit.committer.date
      : '';
    const committedDate = commitDate ? new Date(commitDate) : null;
    return {
      sha,
      shortSha: sha ? sha.slice(0, 7) : '',
      committedDate,
      committedDateISO: committedDate && !Number.isNaN(committedDate.getTime()) ? committedDate.toISOString() : '',
      committedDateDisplay: committedDate && !Number.isNaN(committedDate.getTime()) ? formatDate(committedDate) : '',
    };
  }

  function buildRawUrl(version) {
    const url = new URL(RAW_BASE_URL);
    if (version && version.sha) {
      url.searchParams.set('version', version.sha);
    }
    url.searchParams.set('t', Date.now().toString());
    return url.toString();
  }

  async function fetchWorkbook(url) {
    const response = await fetch(url, {
      cache: 'no-store',
      headers: {
        'Cache-Control': 'no-cache',
        Pragma: 'no-cache',
      },
    });
    if (!response.ok) {
      throw new Error(`Failed to load workbook from ${url}`);
    }
    const buffer = await response.arrayBuffer();
    return buffer;
  }

  async function loadExcelData(options = {}) {
    const normalisedOptions = normaliseOptions(options);
    if (cachedPromise && normalisedOptions.forceReload !== true) {
      return cachedPromise;
    }
    if (typeof XLSX === 'undefined') {
      return Promise.reject(new Error('Excel parser library is not available'));
    }
    cachedPromise = (async () => {
      const version = await fetchLatestCommit();
      const rawUrl = buildRawUrl(version);
      const buffer = await fetchWorkbook(rawUrl);
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
        rows = rawRows.map((row) => Array.isArray(row) ? normaliseRow(row, workbookOptions) : []);
      }
      const records = buildRecords(rows);
      return {
        workbook,
        sheetName,
        rows,
        records,
        version,
      };
    })().catch((error) => {
      cachedPromise = null;
      throw error;
    });
    return cachedPromise;
  }

  global.loadExcelData = loadExcelData;
})(typeof window !== 'undefined' ? window : this);
