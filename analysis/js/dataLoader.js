(function (global) {
  if (typeof global !== 'object' || global === null) {
    return;
  }

  const DEFAULT_PATH = './data/daily.xlsx';
  let workbookPromise = null;
  let lastRequestedPath = null;

  function toRelativeUrl(url) {
    if (!(url instanceof URL)) {
      return null;
    }
    try {
      const base = new URL(global.location.href);
      if (url.origin === base.origin) {
        return `${url.pathname}${url.search}${url.hash}`;
      }
      return url.toString();
    } catch (error) {
      return url.toString();
    }
  }

  function normalisePath(path) {
    if (typeof path === 'string') {
      const trimmed = path.trim();
      if (trimmed.length) {
        return trimmed;
      }
    }
    return DEFAULT_PATH;
  }

  function buildRequestUrl(path) {
    const resolvedPath = normalisePath(path);
    const baseUrl = new URL(resolvedPath, global.location.href);
    baseUrl.searchParams.set('t', Date.now().toString());
    const relative = toRelativeUrl(baseUrl);
    return relative || baseUrl.toString();
  }

  async function fetchWorkbook(path) {
    if (typeof XLSX === 'undefined') {
      throw new Error('Excel parser library is not available');
    }
    const requestUrl = buildRequestUrl(path);
    const response = await fetch(requestUrl, { cache: 'no-store' });
    if (!response.ok) {
      throw new Error(`Unable to load Excel workbook from ${requestUrl}`);
    }
    const buffer = await response.arrayBuffer();
    return XLSX.read(buffer, { type: 'array' });
  }

  function loadWorkbook(options = {}) {
    const requestedPath = normalisePath(options.path);
    if (workbookPromise && requestedPath === lastRequestedPath) {
      return workbookPromise;
    }
    lastRequestedPath = requestedPath;
    workbookPromise = fetchWorkbook(requestedPath).catch((error) => {
      if (requestedPath === lastRequestedPath) {
        workbookPromise = null;
        lastRequestedPath = null;
      }
      throw error;
    });
    return workbookPromise;
  }

  async function loadWorksheetRows(options = {}) {
    const workbook = await loadWorkbook(options);
    const sheetNames = Array.isArray(workbook.SheetNames) ? workbook.SheetNames : [];
    if (!sheetNames.length) {
      throw new Error('Workbook does not contain any sheets');
    }
    let targetSheetName = null;
    if (typeof options.sheetName === 'string' && options.sheetName.trim().length) {
      const candidate = options.sheetName.trim();
      if (Object.prototype.hasOwnProperty.call(workbook.Sheets, candidate)) {
        targetSheetName = candidate;
      }
    }
    if (!targetSheetName && Number.isInteger(options.sheetIndex) && options.sheetIndex >= 0 && options.sheetIndex < sheetNames.length) {
      targetSheetName = sheetNames[options.sheetIndex];
    }
    if (!targetSheetName) {
      targetSheetName = sheetNames[0];
    }
    const worksheet = workbook.Sheets[targetSheetName];
    if (!worksheet) {
      throw new Error('Worksheet not found in workbook');
    }
    const rows = XLSX.utils.sheet_to_json(worksheet, {
      header: 1,
      blankrows: false,
      defval: '',
      raw: true,
    });
    return { rows, sheetName: targetSheetName };
  }

  function resetCache() {
    workbookPromise = null;
    lastRequestedPath = null;
  }

  global.ExcelDataLoader = {
    DEFAULT_PATH,
    buildRequestUrl,
    loadWorkbook,
    loadWorksheetRows,
    resetCache,
  };
})(window);
