(function (global) {
  if (!global || typeof global !== 'object') {
    return;
  }

  // Accepts JS Date, Excel serial (number), ISO/string, or empty.
  function excelSerialToDate(n) {
    // Excel serial (1900-based)
    if (typeof n !== 'number' || Number.isNaN(n)) return null;
    const epoch = new Date(Date.UTC(1899, 11, 30)); // Excel’s day 0 correction
    const ms = n * 24 * 60 * 60 * 1000;
    return new Date(epoch.getTime() + ms);
  }

  function parseToDate(value) {
    if (value == null || value === '') return null;
    if (value instanceof Date && !Number.isNaN(value)) return value;
    if (typeof value === 'number') {
      const d = excelSerialToDate(value);
      return (d && !Number.isNaN(d)) ? d : null;
    }
    if (typeof value === 'string') {
      // Try ISO or common date strings
      const t = Date.parse(value);
      if (!Number.isNaN(t)) return new Date(t);
      // Try dd/mm/yyyy or dd-mm-yyyy typed input
      const m = value.match(/^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})$/);
      if (m) {
        const dd = parseInt(m[1], 10);
        const mm = parseInt(m[2], 10) - 1;
        const yyyy = parseInt(m[3], 10);
        const d = new Date(yyyy, mm, dd);
        return (!Number.isNaN(d)) ? d : null;
      }
    }
    return null;
  }

  function formatDDMMYYYY(value) {
    const d = parseToDate(value);
    if (!d) return '';
    const dd = String(d.getDate()).padStart(2, '0');
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const yyyy = String(d.getFullYear());
    return `${dd}-${mm}-${yyyy}`;
  }

  global.excelSerialToDate = excelSerialToDate;
  global.parseToDate = parseToDate;
  global.formatDDMMYYYY = formatDDMMYYYY;
})(typeof window !== 'undefined' ? window : this);
