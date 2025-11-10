(function (global) {
  if (!global || typeof global !== 'object') {
    return;
  }

  /* Never use toLocaleDateString. Use SheetJS parser to avoid local TZ shifts. */

  const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const DATE_TEXT_INDICATOR_PATTERN = /[-/.,:\s]|T|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec/i;

  function shouldIncrementDay(sourceValue, metadata) {
    if (!metadata || metadata.grain !== 'day') {
      return false;
    }
    if (sourceValue instanceof Date) {
      return true;
    }
    if (typeof sourceValue !== 'string') {
      return false;
    }
    const trimmed = sourceValue.trim();
    if (!trimmed.length) {
      return false;
    }
    return DATE_TEXT_INDICATOR_PATTERN.test(trimmed);
  }

  function incrementMetadataDay(metadata) {
    if (!metadata || metadata.grain !== 'day') {
      return metadata;
    }
    const baseDate = metadata.date instanceof Date
      ? new Date(metadata.date.getTime())
      : new Date(Date.UTC(metadata.y, metadata.m - 1, metadata.d));
    if (Number.isNaN(baseDate.getTime())) {
      return metadata;
    }
    baseDate.setUTCDate(baseDate.getUTCDate() + 1);
    const adjusted = buildMetadata(
      baseDate.getUTCFullYear(),
      baseDate.getUTCMonth() + 1,
      baseDate.getUTCDate(),
      metadata.grain,
    );
    return adjusted || metadata;
  }

  function finaliseMetadataForValue(sourceValue, metadata) {
    if (!metadata) {
      return null;
    }
    if (shouldIncrementDay(sourceValue, metadata)) {
      return incrementMetadataDay(metadata);
    }
    return metadata;
  }

  function excelSerialToYMD(n) {
    if (typeof n !== 'number' || Number.isNaN(n)) {
      return null;
    }
    if (typeof XLSX === 'undefined' || !XLSX.SSF) {
      return null;
    }
    const parsed = XLSX.SSF.parse_date_code(n);
    if (!parsed || !parsed.y || !parsed.m || !parsed.d) {
      return null;
    }
    return { y: parsed.y, m: parsed.m, d: parsed.d };
  }

  function buildMetadata(y, m, d, grain) {
    const safeMonth = Math.max(1, Math.min(12, m));
    const safeDay = Math.max(1, Math.min(31, d));
    const date = new Date(Date.UTC(y, safeMonth - 1, safeDay));
    if (Number.isNaN(date.getTime())) {
      return null;
    }
    const metadata = {
      y,
      m: safeMonth,
      d: safeDay,
      grain: grain || 'day',
      date,
    };
    if (metadata.grain === 'quarter') {
      metadata.quarter = Math.floor((safeMonth - 1) / 3) + 1;
    }
    metadata.sortValue = metadata.date.getTime();
    return metadata;
  }

  function parseQuarterFromDigits(text) {
    if (!/^\d{5}$/.test(text)) {
      return null;
    }
    const year = Number(text.slice(0, 4));
    const quarterDigit = Number(text.slice(4));
    if (!Number.isFinite(year) || !Number.isFinite(quarterDigit)) {
      return null;
    }
    if (quarterDigit < 1 || quarterDigit > 4) {
      return null;
    }
    const month = ((quarterDigit - 1) * 3) + 1;
    return buildMetadata(year, month, 1, 'quarter');
  }

  function parseQuarterFromText(text) {
    const match = text.match(/^(\d{4})\s*[-]?\s*[Qq](\d)$/);
    if (!match) {
      return null;
    }
    const year = Number(match[1]);
    const quarterDigit = Number(match[2]);
    if (!Number.isFinite(year) || !Number.isFinite(quarterDigit)) {
      return null;
    }
    if (quarterDigit < 1 || quarterDigit > 4) {
      return null;
    }
    const month = ((quarterDigit - 1) * 3) + 1;
    return buildMetadata(year, month, 1, 'quarter');
  }

  function parseYearMonth(text) {
    const match = text.match(/^(\d{4})[-]?(\d{2})$/);
    if (!match) {
      return null;
    }
    const year = Number(match[1]);
    const month = Number(match[2]);
    if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
      return null;
    }
    return buildMetadata(year, month, 1, 'month');
  }

  function parseYear(text) {
    if (!/^\d{4}$/.test(text)) {
      return null;
    }
    const year = Number(text);
    if (!Number.isFinite(year)) {
      return null;
    }
    return buildMetadata(year, 1, 1, 'year');
  }

  function parseYYYYMMDD(text) {
    if (!/^\d{8}$/.test(text)) {
      return null;
    }
    const year = Number(text.slice(0, 4));
    const month = Number(text.slice(4, 6));
    const day = Number(text.slice(6, 8));
    if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
      return null;
    }
    if (month < 1 || month > 12) {
      return null;
    }
    if (day < 1 || day > 31) {
      return null;
    }
    return buildMetadata(year, month, day, 'day');
  }

  function parseExcelSerial(text) {
    if (!/^\d+(?:\.\d+)?$/.test(text)) {
      return null;
    }
    const numeric = Number(text);
    if (!Number.isFinite(numeric)) {
      return null;
    }
    if (numeric < 1 || numeric > 600000) {
      return null;
    }
    const ymd = excelSerialToYMD(numeric);
    if (!ymd) {
      return null;
    }
    return buildMetadata(ymd.y, ymd.m, ymd.d, 'day');
  }

  function parseDateMetadata(value) {
    if (value === null || value === undefined || value === '') {
      return null;
    }

    if (value instanceof Date && !Number.isNaN(value.getTime())) {
      return finaliseMetadataForValue(
        value,
        buildMetadata(value.getUTCFullYear(), value.getUTCMonth() + 1, value.getUTCDate(), 'day'),
      );
    }

    const rawText = typeof value === 'string' ? value.trim() : String(value);
    if (!rawText.length) {
      return null;
    }

    const isoDateMatch = rawText.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T\s]|$)/);
    if (isoDateMatch) {
      const year = Number(isoDateMatch[1]);
      const month = Number(isoDateMatch[2]);
      const day = Number(isoDateMatch[3]);
      if (
        Number.isFinite(year)
        && Number.isFinite(month)
        && Number.isFinite(day)
        && month >= 1
        && month <= 12
        && day >= 1
        && day <= 31
      ) {
        return finaliseMetadataForValue(rawText, buildMetadata(year, month, day, 'day'));
      }
    }

    const quarterFromText = parseQuarterFromText(rawText);
    if (quarterFromText) {
      return finaliseMetadataForValue(rawText, quarterFromText);
    }

    const yyyymmdd = parseYYYYMMDD(rawText);
    if (yyyymmdd) {
      return finaliseMetadataForValue(rawText, yyyymmdd);
    }

    const yyyymm = parseYearMonth(rawText);
    if (yyyymm) {
      return finaliseMetadataForValue(rawText, yyyymm);
    }

    const quarterDigits = parseQuarterFromDigits(rawText);
    if (quarterDigits) {
      return finaliseMetadataForValue(rawText, quarterDigits);
    }

    const yearOnly = parseYear(rawText);
    if (yearOnly) {
      return finaliseMetadataForValue(rawText, yearOnly);
    }

    const serial = parseExcelSerial(rawText);
    if (serial) {
      return finaliseMetadataForValue(rawText, serial);
    }

    const match = rawText.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})$/);
    if (match) {
      let year = Number(match[3]);
      if (year < 100) {
        year += year >= 70 ? 1900 : 2000;
      }
      const month = Number(match[2]);
      const day = Number(match[1]);
      if (Number.isFinite(year) && Number.isFinite(month) && Number.isFinite(day)) {
        return finaliseMetadataForValue(rawText, buildMetadata(year, month, day, 'day'));
      }
    }

    const parsed = Date.parse(rawText);
    if (!Number.isNaN(parsed)) {
      const date = new Date(parsed);
      return finaliseMetadataForValue(
        rawText,
        buildMetadata(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate(), 'day'),
      );
    }

    return null;
  }

  function toYMD(value) {
    const metadata = parseDateMetadata(value);
    if (!metadata) {
      return null;
    }
    return { y: metadata.y, m: metadata.m, d: metadata.d };
  }

  function formatDDMMYYYY(value) {
    const metadata = parseDateMetadata(value);
    if (!metadata) {
      return '';
    }
    const dd = String(metadata.d).padStart(2, '0');
    const mm = String(metadata.m).padStart(2, '0');
    const yyyy = String(metadata.y);
    return `${dd}-${mm}-${yyyy}`;
  }

  function sortKeyYYYYMMDD(value) {
    const metadata = parseDateMetadata(value);
    if (!metadata) {
      return '';
    }
    const mm = String(metadata.m).padStart(2, '0');
    const dd = String(metadata.d).padStart(2, '0');
    return `${metadata.y}-${mm}-${dd}`;
  }

  function excelSerialToDate(n) {
    const metadata = parseDateMetadata(n);
    if (!metadata) {
      return null;
    }
    return metadata.date;
  }

  function parseToDate(value) {
    const metadata = parseDateMetadata(value);
    if (!metadata) {
      return null;
    }
    return metadata.date;
  }

  function formatDateLabel(value) {
    const metadata = value && typeof value === 'object' && value.grain
      ? value
      : parseDateMetadata(value);
    if (!metadata) {
      return '';
    }
    const year = String(metadata.y);
    if (metadata.grain === 'year') {
      return year;
    }
    if (metadata.grain === 'quarter') {
      const quarterNumber = metadata.quarter || Math.floor((metadata.m - 1) / 3) + 1;
      return `Q${quarterNumber} ${year}`;
    }
    const monthName = MONTH_NAMES[Math.max(0, Math.min(11, metadata.m - 1))];
    if (metadata.grain === 'month') {
      return `${monthName} ${year}`;
    }
    const day = String(metadata.d).padStart(2, '0');
    return `${day}-${monthName}-${year}`;
  }

  global.excelSerialToYMD = excelSerialToYMD;
  global.toYMD = toYMD;
  global.formatDDMMYYYY = formatDDMMYYYY;
  global.sortKeyYYYYMMDD = sortKeyYYYYMMDD;
  global.excelSerialToDate = excelSerialToDate;
  global.parseToDate = parseToDate;
  global.parseDateMetadata = parseDateMetadata;
  global.formatDateLabel = formatDateLabel;
})(typeof window !== 'undefined' ? window : this);
