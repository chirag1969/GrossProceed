(function (global) {
  if (!global || typeof global !== 'object') {
    return;
  }

  function excelSerialToYMD(n) {
    if (typeof XLSX === 'undefined' || !XLSX.SSF || typeof n !== 'number' || Number.isNaN(n)) {
      return null;
    }
    const parsed = XLSX.SSF.parse_date_code(n);
    if (!parsed || !parsed.y || !parsed.m || !parsed.d) {
      return null;
    }
    return { y: parsed.y, m: parsed.m, d: parsed.d };
  }

  function toYMD(value) {
    if (value === null || value === undefined || value === '') {
      return null;
    }

    if (typeof value === 'number') {
      return excelSerialToYMD(value);
    }

    if (typeof value === 'string') {
      const match = value.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
      if (match) {
        return { d: Number(match[1]), m: Number(match[2]), y: Number(match[3]) };
      }
      const timestamp = Date.parse(value);
      if (!Number.isNaN(timestamp)) {
        const date = new Date(timestamp);
        return {
          y: date.getUTCFullYear(),
          m: date.getUTCMonth() + 1,
          d: date.getUTCDate(),
        };
      }
    }

    if (value instanceof Date && !Number.isNaN(value)) {
      return {
        y: value.getUTCFullYear(),
        m: value.getUTCMonth() + 1,
        d: value.getUTCDate(),
      };
    }

    return null;
  }

  function formatDDMMYYYY(value) {
    const ymd = value && typeof value === 'object' && 'y' in value && 'm' in value && 'd' in value
      ? value
      : toYMD(value);
    if (!ymd) {
      return '';
    }
    const dd = String(ymd.d).padStart(2, '0');
    const mm = String(ymd.m).padStart(2, '0');
    const yyyy = String(ymd.y);
    return `${dd}-${mm}-${yyyy}`;
  }

  function sortKeyYYYYMMDD(value) {
    const ymd = toYMD(value);
    if (!ymd) {
      return '';
    }
    const mm = String(ymd.m).padStart(2, '0');
    const dd = String(ymd.d).padStart(2, '0');
    return `${ymd.y}-${mm}-${dd}`;
  }

  function excelSerialToDate(n) {
    const ymd = excelSerialToYMD(n);
    if (!ymd) {
      return null;
    }
    const date = new Date(Date.UTC(ymd.y, ymd.m - 1, ymd.d));
    return Number.isNaN(date.getTime()) ? null : date;
  }

  function parseToDate(value) {
    const ymd = toYMD(value);
    if (!ymd) {
      return null;
    }
    const date = new Date(Date.UTC(ymd.y, ymd.m - 1, ymd.d));
    return Number.isNaN(date.getTime()) ? null : date;
  }

  global.excelSerialToYMD = excelSerialToYMD;
  global.toYMD = toYMD;
  global.formatDDMMYYYY = formatDDMMYYYY;
  global.sortKeyYYYYMMDD = sortKeyYYYYMMDD;
  global.excelSerialToDate = excelSerialToDate;
  global.parseToDate = parseToDate;
})(typeof window !== 'undefined' ? window : this);
