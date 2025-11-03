const tabButtons = document.querySelectorAll('.tab-button');
    const tabPanels = document.querySelectorAll('.tab-panel');
    const tabNavElement = document.querySelector('.tab-nav');
    const dataVersionElement = document.getElementById('data-version');
    const dataErrorElement = document.getElementById('data-error');
    const tabNavIndicatorElement = tabNavElement ? tabNavElement.querySelector('.tab-nav__indicator') : null;
    let indicatorPreviousLeft = null;
    const TAB_NAV_SCROLL_EPSILON = 1;
    const loSubTabButtons = document.querySelectorAll('.lo-sub-tab-button');
    const loSubTabPanels = document.querySelectorAll('.lo-sub-tab-panel');
    const platformSubTabButtons = document.querySelectorAll('.platform-tab-button');
    const platformSubTabPanels = document.querySelectorAll('.platform-tab-panel');
    const loFilterButtonElement = document.getElementById('lo-filter-button');
    const loFilterClearButtonElement = document.getElementById('lo-filter-clear-button');
    const platformFilterButtonElement = document.getElementById('platform-filter-button');
    const platformFilterClearButtonElement = document.getElementById('platform-filter-clear-button');
    const newProductStatusElement = document.getElementById('new-product-status');
    const skuFilterClearButtonElement = document.getElementById('sku-filter-clear-button');
    const mainFilterClearButtonElement = document.getElementById('main-filter-clear-button');
    const regularFilterClearButtonElement = document.getElementById('regular-filter-clear-button');
    const dashboardSummaryStoreFilterGroupElement = document.getElementById('dashboard-summary-store-filter-group');
    const dashboardSummaryStoreFilterContainerElement = document.getElementById('dashboard-summary-store-filter');
    const dashboardSummaryStoreFilterToggleElement = document.getElementById('dashboard-summary-store-filter-toggle');
    const dashboardSummaryStoreFilterMenuElement = document.getElementById('dashboard-summary-store-filter-menu');
    const dashboardSummaryStoreFilterOptionsElement = document.getElementById('dashboard-summary-store-filter-options');
    const dashboardSummaryStoreFilterSelectAllElement = document.getElementById('dashboard-summary-store-filter-select-all');
    const dashboardSummaryStoreFilterApplyElement = document.getElementById('dashboard-summary-store-filter-apply');
    const dashboardSummaryStoreFilterResetElement = document.getElementById('dashboard-summary-store-filter-reset');
    const dashboardSummaryStoreFilterMenuEmptyElement = document.getElementById('dashboard-summary-store-filter-menu-empty');
    const dashboardSummaryStoreFilterEmptyElement = document.getElementById('dashboard-summary-store-filter-empty');

    if (dashboardSummaryStoreFilterToggleElement) {
      dashboardSummaryStoreFilterToggleElement.addEventListener('click', () => {
        if (!dashboardSummaryStoreFilterMenuElement) {
          return;
        }
        if (dashboardSummaryStoreFilterMenuElement.hidden) {
          openDashboardSummaryStoreFilterMenu();
        } else {
          closeDashboardSummaryStoreFilterMenu();
        }
      });
    }

    if (dashboardSummaryStoreFilterOptionsElement) {
      dashboardSummaryStoreFilterOptionsElement.addEventListener('change', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') {
          return;
        }
        handleDashboardSummaryStoreFilterOptionToggle(target.value, target.checked);
      });
    }

    if (dashboardSummaryStoreFilterSelectAllElement) {
      dashboardSummaryStoreFilterSelectAllElement.addEventListener('change', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement)) {
          return;
        }
        if (target.checked) {
          dashboardSummaryStoreFilterPendingSelection = null;
        } else {
          dashboardSummaryStoreFilterPendingSelection = new Set();
        }
        renderDashboardSummaryStoreFilterOptions();
      });
    }

    if (dashboardSummaryStoreFilterApplyElement) {
      dashboardSummaryStoreFilterApplyElement.addEventListener('click', () => {
        applyDashboardSummaryStoreFilterSelection();
      });
    }

    if (dashboardSummaryStoreFilterResetElement) {
      dashboardSummaryStoreFilterResetElement.addEventListener('click', () => {
        dashboardSummaryStoreFilterSelection = null;
        dashboardSummaryStoreFilterPendingSelection = null;
        updateDashboardSummaryStoreFilterSummaryLabel();
        renderDashboardSummaryStoreFilterOptions();
        updateDashboardSummaryTable();
        closeDashboardSummaryStoreFilterMenu();
      });
    }

    const reduceMotionQuery = typeof window.matchMedia === 'function' ? window.matchMedia('(prefers-reduced-motion: reduce)') : null;
    let shouldReduceMotion = reduceMotionQuery ? reduceMotionQuery.matches : false;

    function updateTabNavScrollShadows() {
      if (!tabNavElement) {
        return;
      }
      const maxScrollLeft = tabNavElement.scrollWidth - tabNavElement.clientWidth;
      const isScrollable = maxScrollLeft > TAB_NAV_SCROLL_EPSILON;
      const showStartShadow = tabNavElement.scrollLeft > TAB_NAV_SCROLL_EPSILON;
      const showEndShadow = tabNavElement.scrollLeft < maxScrollLeft - TAB_NAV_SCROLL_EPSILON;
      tabNavElement.classList.toggle('tab-nav--scrollable', isScrollable);
      tabNavElement.classList.toggle('tab-nav--show-start-shadow', showStartShadow);
      tabNavElement.classList.toggle('tab-nav--show-end-shadow', showEndShadow);
    }

    function scrollActiveTabIntoView(targetTab) {
      if (!tabNavElement || !targetTab) {
        return;
      }
      const activeButton = Array.from(tabButtons).find((button) => button.dataset.tab === targetTab);
      if (!activeButton) {
        return;
      }
      const behavior = shouldReduceMotion ? 'auto' : 'smooth';
      activeButton.scrollIntoView({ block: 'nearest', inline: 'center', behavior });
      if (typeof requestAnimationFrame === 'function') {
        requestAnimationFrame(updateTabNavScrollShadows);
      } else {
        updateTabNavScrollShadows();
      }
    }

    const animatedCards = Array.from(document.querySelectorAll('.card'));
    let cardObserver = null;

    function revealCardElement(card) {
      if (!(card instanceof HTMLElement)) {
        return;
      }
      card.classList.add('is-visible');
      if (cardObserver) {
        cardObserver.unobserve(card);
      }
    }

    if ('IntersectionObserver' in window) {
      cardObserver = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              revealCardElement(entry.target);
            }
          });
        },
        { root: null, threshold: 0.18, rootMargin: '0px 0px -12%' },
      );
    }

    animatedCards.forEach((card) => {
      if (!(card instanceof HTMLElement)) {
        return;
      }
      card.setAttribute('data-animate', 'true');
      if (shouldReduceMotion || !cardObserver) {
        revealCardElement(card);
      } else {
        cardObserver.observe(card);
      }
    });

    if (reduceMotionQuery) {
      const handleReduceMotionChange = (event) => {
        shouldReduceMotion = event.matches;
        if (shouldReduceMotion) {
          animatedCards.forEach((card) => revealCardElement(card));
        } else if (cardObserver) {
          animatedCards.forEach((card) => {
            if (card instanceof HTMLElement && !card.classList.contains('is-visible')) {
              cardObserver.observe(card);
            }
          });
        }
        scrollActiveTabIntoView(activeTabId);
        updateTabIndicator(activeTabId);
      };
      if (typeof reduceMotionQuery.addEventListener === 'function') {
        reduceMotionQuery.addEventListener('change', handleReduceMotionChange);
      } else if (typeof reduceMotionQuery.addListener === 'function') {
        reduceMotionQuery.addListener(handleReduceMotionChange);
      }
    }

    const PANEL_TRANSITION_DURATION = 340;
    const FILTER_TRANSITION_DURATION = 320;
    const BUTTON_BUSY_TIMEOUT = 420;
    const WORKBOOK_URL = (typeof window !== 'undefined' && window && window.WORKBOOK_URL)
      ? window.WORKBOOK_URL
      : '/GrossProceed/analysis/data/daily.xlsx';

    let regularTable;
    let regularTableInitialised = false;
    let regularTableFooterValues = [];
    let regularTableNumericColumnSet = new Set();
    let regularTableAugmentedDataset = null;
    let regularDatasetCache = null;
    let regularDatasetPromise = null;
    let regularSheetHeader = null;
    let regularCheckoutColumnIndex = -1;
    let mainTable;
    let mainTableInitialised = false;
    let mainTableFooterValues = [];
    let mainTableNumericColumnSet = new Set();
    let mainTableAugmentedDataset = null;
    let mainTillDateColumnIndices = [];
    let mainTillDateHeaders = [];
    let mainDatasetCache = null;
    let mainDatasetPromise = null;
    let loTablesInitialised = false;
    let platformTablesInitialised = false;
    let spendPivotCache = null;
    let spendPivotPromise = null;
    let skuSummaryInitialised = false;
    let skuSummaryPivotCache = null;
    let skuSummaryPivotPromise = null;
    let skuSummaryCurrentPage = 1;
    let skuSummaryPageSize = 25;
    let skuSummaryTotalRows = 0;
    let mainDashboardInitialised = false;
    let mainDashboardPivotCache = null;
    let dashboardPivotPromise = null;
    let dashboardSummaryTableInitialised = false;
    let dashboardSummaryTableCache = null;
    let dashboardSummaryTablePromise = null;
    let dashboardSummaryStoreFilterSelection = null;
    let dashboardSummaryStoreFilterPendingSelection = null;
    let dashboardSummaryStoreFilterOptions = [];
    let dashboardSummaryStoreFilterMenuVisible = false;
    let dashboardSummaryStoreFilterMenuDocumentHandlerRegistered = false;
    let activeTabId = null;
    let newProductInitialised = false;
    let newProductPivotCache = null;
    let newProductPivotPromise = null;
    let salesGapDatasetCache = null;
    let salesGapDatasetPromise = null;
    let salesGapInitialised = false;
    let activeSalesGapFilterConfig = null;
    let salesGapFilterKeyListenerRegistered = false;
    let workbookLoadPromise = null;
    let workbookVersionInfo = null;

    const NUMERIC_COLUMN_EXCLUSIONS = new Set(['ORDER NO', 'PLAIN ORDER NO', 'ORDER #', 'ORDER NO.', 'CHECKOUT']);
    const ZERO_DECIMAL_COLUMNS = new Set(['qty', 'order no', 'plain order no', 'order no.', 'order #']);
    const numberFormatter = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    const integerFormatter = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
    const currencyFormatter = new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    const percentFormatter = new Intl.NumberFormat('en-US', {
      style: 'percent',
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    });
    const displayDateFormatter = {
      format(value) {
        if (typeof window.formatDDMMYYYY === 'function') {
          const formatted = window.formatDDMMYYYY(value);
          if (formatted) {
            return formatted;
          }
        }
        if (typeof window.toYMD === 'function') {
          const ymd = window.toYMD(value);
          if (ymd) {
            const dd = String(ymd.d).padStart(2, '0');
            const mm = String(ymd.m).padStart(2, '0');
            const yyyy = String(ymd.y);
            return `${dd}-${mm}-${yyyy}`;
          }
        }
        return '';
      },
    };
    const parseDateValue = (value) => {
      if (typeof window.toYMD === 'function') {
        const ymd = window.toYMD(value);
        if (ymd) {
          const date = new Date(Date.UTC(ymd.y, ymd.m - 1, ymd.d));
          return Number.isNaN(date.getTime()) ? null : date;
        }
      }
      if (value instanceof Date && !Number.isNaN(value.getTime())) {
        const date = new Date(Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()));
        return Number.isNaN(date.getTime()) ? null : date;
      }
      return null;
    };
    const formatDateValue = (value) => {
      if (typeof window.formatDDMMYYYY === 'function') {
        const formatted = window.formatDDMMYYYY(value);
        if (formatted) {
          return formatted;
        }
      }
      if (typeof window.toYMD === 'function') {
        const ymd = window.toYMD(value);
        if (ymd) {
          const dd = String(ymd.d).padStart(2, '0');
          const mm = String(ymd.m).padStart(2, '0');
          const yyyy = String(ymd.y);
          return `${dd}-${mm}-${yyyy}`;
        }
      }
      return '';
    };
    const isDateColumnName = (name) => {
      if (typeof name !== 'string') {
        return false;
      }
      const normalised = name.trim().toLowerCase();
      if (!normalised.length) {
        return false;
      }
      if (normalised === 'checkout' || normalised === 'date') {
        return true;
      }
      return normalised.includes(' date');
    };
    const TOTAL_ROW_LABEL = 'Grand Total';
    const TOTAL_ROW_LABEL_NORMALISED = TOTAL_ROW_LABEL.trim().toLowerCase();
    const MAIN_TABLE_FORMAT_OPTIONS = { skipDateFormatting: true };
    const TILL_DATE_HEADER_PATTERN = /till date/i;

    let columnValueOptions = [];
    let columnFilters = {};
    let mainColumnValueOptions = [];
    let mainColumnFilters = {};
    let regularFilterButtonElement = null;
    let regularFilterContainerElement = null;
    let regularFilterColumnSelect = null;
    let regularFilterSearchInput = null;
    let regularFilterOptionsElement = null;
    let regularFilterEmptyElement = null;
    let regularFilterSelectAllInput = null;
    let regularFilterApplyButton = null;
    let regularFilterResetButton = null;
    let regularFilterCloseButton = null;
    let mainFilterButtonElement = null;
    let mainFilterContainerElement = null;
    let mainFilterColumnSelect = null;
    let mainFilterSearchInput = null;
    let mainFilterOptionsElement = null;
    let mainFilterEmptyElement = null;
    let mainFilterSelectAllInput = null;
    let mainFilterApplyButton = null;
    let mainFilterResetButton = null;
    let mainFilterCloseButton = null;
    let regularFilterActiveColumnIndex = null;
    let regularFilterSelection = new Set();
    let regularFilterSearchTerm = '';
    let regularFilterInitialised = false;
    let regularFilterEligibleColumns = [];
    let regularFilterButtons = [];
    let regularFilterClearButtons = [];
    let activeRegularFilterTrigger = null;
    let mainFilterActiveColumnIndex = null;
    let mainFilterSelection = new Set();
    let mainFilterSearchTerm = '';
    let mainFilterInitialised = false;
    let mainFilterEligibleColumns = [];
    let mainFilterButtons = [];
    let mainFilterClearButtons = [];
    let activeMainFilterTrigger = null;
    let dashboardPivotRowFilterContainerElement = null;
    let dashboardPivotRowFilterOptionsElement = null;
    let dashboardPivotRowFilterFieldOptionsElement = null;
    let dashboardPivotRowFilterEmptyElement = null;
    let dashboardPivotRowFilterSelectAllInput = null;
    let dashboardPivotRowFilterApplyButton = null;
    let dashboardPivotRowFilterResetButton = null;
    let dashboardPivotRowFilterCloseButton = null;
    let dashboardPivotRowFilterInitialised = false;
    let activeDashboardPivotRowFilterState = null;
    let activeDashboardPivotRowFilterTrigger = null;
    let dashboardPivotRowFilterPendingSelection = null;
    let dashboardPivotSourceDataset = null;
    let dashboardPivotFieldOptionCache = new Map();
    let totalColumnIndex = -1;
    let mainTotalColumnIndex = -1;
    let headerMenuElement;
    let activeHeaderCell = null;
    let activeColumnIndex = null;
    const headerClickHandlers = new WeakMap();
    const HEADER_HEIGHT = 44;
    const ROW_HEIGHT = 34;
    const MIN_VISIBLE_ROWS = 5;
    const TABLE_BOTTOM_MARGIN = 18;
    const LO_TABLE_BOTTOM_MARGIN = 24;
    const MIN_LO_TABLE_HEIGHT = 320;
    const REGULAR_TABLE_FOOTER_MIN_SPACE = 128;
    const REGULAR_TABLE_FOOTER_EXTRA_GAP = 24;
    const DEFAULT_REGULAR_TABLE_RESERVED_SPACE = TABLE_BOTTOM_MARGIN + REGULAR_TABLE_FOOTER_MIN_SPACE;
    const REGULAR_TABLE_PAGE_LENGTH = 200;
    const SHOW_REGULAR_TOTAL_ROW = true;
    const REGULAR_FILTER_MAX_UNIQUE_VALUES = 350;
    let EXCEL_REGULAR_SHEET_CANDIDATES = [
      'REGULAR',
      'Regular',
    ];
    let EXCEL_REGULAR_DISPLAY_NAME = 'Gross Proceed';

    function updateDataVersionIndicator(version) {
      if (!dataVersionElement) {
        return;
      }
      if (!version || !version.sourceLabel) {
        dataVersionElement.textContent = '';
        dataVersionElement.hidden = true;
        dataVersionElement.removeAttribute('title');
        dataVersionElement.removeAttribute('data-source');
        dataVersionElement.removeAttribute('data-cache-buster');
        return;
      }
      const parts = [`Data source: ${version.sourceLabel}`];
      if (version.fetchTimestampDisplay) {
        parts.push(`fetched ${version.fetchTimestampDisplay}`);
      } else if (version.lastModifiedDisplay) {
        parts.push(`last modified ${version.lastModifiedDisplay}`);
      }
      dataVersionElement.textContent = parts.join(' • ');
      const titleParts = [];
      if (version.finalUrl) {
        titleParts.push(`URL: ${version.finalUrl}`);
      }
      if (version.fetchTimestampISO) {
        titleParts.push(`Fetched: ${version.fetchTimestampISO}`);
      }
      if (version.lastModifiedISO) {
        titleParts.push(`Last-Modified: ${version.lastModifiedISO}`);
      }
      if (titleParts.length) {
        dataVersionElement.title = titleParts.join('\n');
      } else {
        dataVersionElement.removeAttribute('title');
      }
      if (version.sourceType) {
        dataVersionElement.setAttribute('data-source', version.sourceType);
      } else {
        dataVersionElement.removeAttribute('data-source');
      }
      if (version.cacheBuster) {
        dataVersionElement.setAttribute('data-cache-buster', version.cacheBuster);
      } else {
        dataVersionElement.removeAttribute('data-cache-buster');
      }
      dataVersionElement.hidden = false;
    }

    function clearDataLoaderError() {
      if (!dataErrorElement) {
        return;
      }
      dataErrorElement.textContent = '';
      dataErrorElement.hidden = true;
      dataErrorElement.removeAttribute('role');
    }

    function showDataLoaderError(message) {
      if (!dataErrorElement) {
        return;
      }
      const text = typeof message === 'string' && message.trim().length
        ? message.trim()
        : 'Unable to load the latest Excel data. Please refresh to try again.';
      dataErrorElement.textContent = text;
      dataErrorElement.hidden = false;
      dataErrorElement.setAttribute('role', 'alert');
    }

    function applyWorkbookDisplayName(displayName) {
      if (typeof displayName !== 'string') {
        return;
      }
      const trimmed = displayName.trim();
      if (!trimmed.length) {
        return;
      }
      EXCEL_REGULAR_DISPLAY_NAME = trimmed;
      document.title = `${trimmed} Performance Dashboard`;
      const headerTitle = document.querySelector('[data-report-title]');
      if (headerTitle) {
        headerTitle.textContent = `${trimmed} Performance Dashboard`;
      }
      const headerSubtitle = document.querySelector('[data-report-subtitle]');
      if (headerSubtitle) {
        headerSubtitle.textContent = `Automated analysis generated from the ${trimmed} workbook.`;
      }
    }

    applyWorkbookDisplayName(EXCEL_REGULAR_DISPLAY_NAME);

    function getRegularDisplayName() {
      return EXCEL_REGULAR_DISPLAY_NAME && EXCEL_REGULAR_DISPLAY_NAME.length
        ? EXCEL_REGULAR_DISPLAY_NAME
        : 'Gross Proceed';
    }
    const EXCEL_MAIN_SHEET_CANDIDATES = ['Main', 'MAIN'];
    const EXCEL_KEY_COLUMN_NAMES = ['ORDER NO', 'Order No', 'ORDER NO.', 'ORDER #', 'Plain Order No'];
    const EXCEL_MAIN_KEY_COLUMN_NAMES = ['SKU', 'Sr. No.', 'NAME', 'DC LIST'];
    const EXCEL_SALES_GAP_SHEET_CANDIDATES = ['SKUWISE GAP', 'LO WISE GAP', 'LO-wise GAP', 'Sales GAP', 'Sales Gap'];
    const EXCEL_SALES_GAP_HEADER_SEARCH_VALUES = ['Row Labels', 'L.O.', 'SKU'];
    const EXCEL_SALES_GAP_KEY_COLUMN_NAMES = ['L.O.', 'Row Labels', 'SKU'];
    const EXCEL_DASHBOARD_SHEET_CANDIDATES = ['Dashboard', 'DASHBOARD'];
    const DASHBOARD_SUMMARY_RANGE = 'A41:O60';
    const DASHBOARD_SUMMARY_PERCENT_PATTERN = /%|percent/i;
    const DASHBOARD_SUMMARY_CURRENCY_KEYWORDS = ['revenue', 'ip', 'spend', 'fees', 'gross'];
    const DASHBOARD_SUMMARY_INTEGER_KEYWORDS = ['qty'];
    const NEW_PRODUCT_FILTER_FIELDS = [
      { id: 'row', label: 'Row labels', type: 'row', columnIndex: 0 },
      { id: 'new-old', label: 'NEW/OLD', type: 'attribute' },
    ];
    const NEW_PRODUCT_DEFAULT_NEW_OLD_SELECTION = ['NEW24', 'NEW25'];
    const NEW_PRODUCT_PIVOT_CONFIGS = [
      {
        id: 'new-product-target',
        tableId: 'new-product-target-table',
        filterButtonId: 'new-product-target-filter-button',
        filterContainerId: 'new-product-target-filter',
        filterOptionsId: 'new-product-target-filter-options',
        filterFieldOptionsId: 'new-product-target-filter-field-options',
        filterFieldLabelId: 'new-product-target-filter-field-label',
        filterApplyId: 'new-product-target-filter-apply',
        filterResetId: 'new-product-target-filter-reset',
        filterEmptyId: 'new-product-target-filter-empty',
        filterTitleId: 'new-product-target-filter-title',
        filterClearButtonId: 'new-product-target-filter-clear-button',
        subtitleId: 'new-product-target-subtitle',
        filterFieldDefinitions: NEW_PRODUCT_FILTER_FIELDS,
      },
      {
        id: 'new-product-secondary',
        tableId: 'new-product-secondary-table',
        filterButtonId: 'new-product-secondary-filter-button',
        filterContainerId: 'new-product-secondary-filter',
        filterOptionsId: 'new-product-secondary-filter-options',
        filterFieldOptionsId: 'new-product-secondary-filter-field-options',
        filterFieldLabelId: 'new-product-secondary-filter-field-label',
        filterApplyId: 'new-product-secondary-filter-apply',
        filterResetId: 'new-product-secondary-filter-reset',
        filterEmptyId: 'new-product-secondary-filter-empty',
        filterTitleId: 'new-product-secondary-filter-title',
        filterClearButtonId: 'new-product-secondary-filter-clear-button',
        subtitleId: 'new-product-secondary-subtitle',
        filterFieldDefinitions: NEW_PRODUCT_FILTER_FIELDS,
      },
    ];
    const SALES_GAP_FILTER_CONFIGS = [
      {
        id: 'sales-gap',
        datasetKey: 'loDataset',
        tableId: 'sales-gap-table',
        filterButtonId: 'sales-gap-filter-button',
        filterClearButtonId: 'sales-gap-filter-clear-button',
        filterContainerId: 'sales-gap-filter',
        filterFieldOptionsId: 'sales-gap-filter-field-options',
        filterOptionsId: 'sales-gap-filter-options',
        filterEmptyId: 'sales-gap-filter-empty',
        filterApplyId: 'sales-gap-filter-apply',
        filterResetId: 'sales-gap-filter-reset',
        filterTitleId: 'sales-gap-filter-title',
      },
      {
        id: 'sku-gap',
        datasetKey: 'skuDataset',
        tableId: 'sku-gap-table',
        filterButtonId: 'sku-gap-filter-button',
        filterClearButtonId: 'sku-gap-filter-clear-button',
        filterContainerId: 'sku-gap-filter',
        filterFieldOptionsId: 'sku-gap-filter-field-options',
        filterOptionsId: 'sku-gap-filter-options',
        filterEmptyId: 'sku-gap-filter-empty',
        filterApplyId: 'sku-gap-filter-apply',
        filterResetId: 'sku-gap-filter-reset',
        filterTitleId: 'sku-gap-filter-title',
      },
    ];
    const NEW_PRODUCT_DATASET_PIVOT_DEFINITIONS = [
      {
        groupColumn: 'AMZ',
        columns: [
          { source: 'TOTAL TARGET SALES', header: 'Sum of TOTAL TARGET SALES' },
          { source: 'Desired Rev Till Date', header: 'Sum of Desired Rev Till Date' },
          { source: 'ACHIVED REV', header: 'Sum of ACHIVED REV' },
          { source: 'Diff in rev', header: 'Sum of Diff in rev' },
          { source: 'Desired IP till Date', header: 'Sum of Desired IP till Date' },
          { source: 'Achieved IP', header: 'Sum of Achieved IP' },
          { source: 'Diff in IP', header: 'Sum of Diff in IP' },
          { source: 'ADVT SPEND', header: 'Sum of ADVT SPEND' },
        ],
        metadata: {
          segmentLabel: 'Channel',
          segmentSelection: 'AMZ (Main sheet)',
        },
      },
      {
        groupColumn: 'EBAY2',
        columns: [
          { source: 'TOTAL TARGET SALES2', header: 'Sum of TOTAL TARGET SALES2' },
          { source: 'Desired Rev Till Date2', header: 'Sum of Desired Rev Till Date2' },
          { source: 'ACHIVED REV2', header: 'Sum of ACHIVED REV2' },
          { source: 'Diff in rev2', header: 'Sum of Diff in rev2' },
          { source: 'Desired IP till Date2', header: 'Sum of Desired IP till Date2' },
          { source: 'Achieved IP2', header: 'Sum of Achieved IP2' },
          { source: 'Diff in IP2', header: 'Sum of Diff in IP2' },
          { source: 'ADVT SPEND2', header: 'Sum of ADVT SPEND2' },
        ],
        metadata: {
          segmentLabel: 'Channel',
          segmentSelection: 'EBAY2 (Main sheet)',
        },
      },
    ];
    const newProductTableFilterRegistry = new Map();
    let newProductFilterHookRegistered = false;
    let DASHBOARD_PIVOT_FILTER_FIELDS = [
      { id: 'row', label: 'Row labels', type: 'row' },
    ];
    const DASHBOARD_PIVOT_CONFIGS = [
      {
        id: 'name',
        tableId: 'dashboard-table-name',
        groupColumn: 'NAME',
        sheetGroupColumn: 'Row Labels',
        displayLabel: 'Name',
        columns: [
          { source: 'TOTAL TARGET SALES', sheetHeader: 'Sum of TOTAL TARGET SALES', header: 'Sum of TOTAL TARGET SALES' },
          { source: 'Desired Rev Till Date', sheetHeader: 'Sum of Desired Rev Till Date', header: 'Sum of Desired Rev Till Date' },
          { source: 'ACHIVED REV', sheetHeader: 'Sum of ACHIVED REV', header: 'Sum of ACHIVED REV' },
          { source: 'Diff in rev', sheetHeader: 'Sum of Diff in rev', header: 'Sum of Diff in rev' },
          { source: 'Desired IP till Date', sheetHeader: 'Sum of Desired IP till Date', header: 'Sum of Desired IP till Date' },
          { source: 'Achieved IP', sheetHeader: 'Sum of Achieved IP', header: 'Sum of Achieved IP' },
          { source: 'Diff in IP', sheetHeader: 'Sum of Diff in IP', header: 'Sum of Diff in IP' },
          { source: 'ADVT SPEND', sheetHeader: 'Sum of ADVT SPEND', header: 'Sum of ADVT SPEND' },
          {
            source: 'A- Ad Spend %',
            sheetHeader: 'Sum of A- Ad Spend %',
            header: 'Sum of A- Ad Spend %',
            computed: {
              type: 'ratio',
              numerator: 'ADVT SPEND',
              denominator: 'REV ACH TILL DATE',
              asPercentage: true,
            },
          },
          { source: 'Storage Fees', sheetHeader: 'Sum of Storage Fees', header: 'Sum of Storage Fees' },
          { source: '% of Ip Ach', sheetHeader: 'Sum of % of Ip Ach', header: 'Sum of % of Ip Ach' },
          { source: 'REV ACH TILL DATE', sheetHeader: 'Sum of REV ACH TILL DATE', header: 'Sum of REV ACH TILL DATE' },
          { source: 'IP ACH TILL DATE', sheetHeader: 'Sum of IP ACH TILL DATE', header: 'Sum of IP ACH TILL DATE' },
        ],
        filterFieldDefinitions: DASHBOARD_PIVOT_FILTER_FIELDS,
      },
      {
        id: 'ebay',
        tableId: 'dashboard-table-ebay',
        groupColumn: 'EBAY',
        sheetGroupColumn: 'Row Labels',
        displayLabel: 'eBay',
        columns: [
          { source: 'TOTAL TARGET SALES2', sheetHeader: 'Sum of TOTAL TARGET SALES2', header: 'Sum of TOTAL TARGET SALES2' },
          { source: 'Desired Rev Till Date2', sheetHeader: 'Sum of Desired Rev Till Date2', header: 'Sum of Desired Rev Till Date2' },
          { source: 'ACHIVED REV2', sheetHeader: 'Sum of ACHIVED REV2', header: 'Sum of ACHIVED REV2' },
          { source: 'Diff in rev2', sheetHeader: 'Sum of Diff in rev2', header: 'Sum of Diff in rev2' },
          { source: 'Desired IP till Date2', sheetHeader: 'Sum of Desired IP till Date2', header: 'Sum of Desired IP till Date2' },
          { source: 'Achieved IP2', sheetHeader: 'Sum of Achieved IP2', header: 'Sum of Achieved IP2' },
          { source: 'Diff in IP2', sheetHeader: 'Sum of Diff in IP2', header: 'Sum of Diff in IP2' },
          { source: 'ADVT SPEND2', sheetHeader: 'Sum of ADVT SPEND2', header: 'Sum of ADVT SPEND2' },
          {
            source: 'Ad Spend %',
            sheetHeader: 'Sum of Ad Spend %',
            header: 'Sum of Ad Spend %',
            computed: {
              type: 'ratio',
              numerator: 'ADVT SPEND2',
              denominator: 'REV ACH TILL DATE(EBAY)',
              asPercentage: true,
            },
          },
          { source: 'Storage Fees2', sheetHeader: 'Sum of Storage Fees2', header: 'Sum of Storage Fees2' },
          { source: '% of Ip Ach(ebay)', sheetHeader: 'Sum of % of Ip Ach(ebay)', header: 'Sum of % of Ip Ach(ebay)' },
          { source: 'REV ACH TILL DATE(EBAY)', sheetHeader: 'Sum of REV ACH TILL DATE(EBAY)', header: 'Sum of REV ACH TILL DATE(EBAY)' },
          { source: 'IP ACH TILL DATE(EBAY)', sheetHeader: 'Sum of IP ACH TILL DATE(EBAY)', header: 'Sum of IP ACH TILL DATE(EBAY)' },
        ],
        filterFieldDefinitions: DASHBOARD_PIVOT_FILTER_FIELDS,
      },
      {
        id: 'website',
        tableId: 'dashboard-table-website',
        groupColumn: 'Website',
        sheetGroupColumn: 'Row Labels',
        displayLabel: 'Website',
        columns: [
          { source: 'TOTAL TARGET SALES3', sheetHeader: 'Sum of TOTAL TARGET SALES3', header: 'Sum of TOTAL TARGET SALES3' },
          { source: 'Desired Rev Till Date3', sheetHeader: 'Sum of Desired Rev Till Date3', header: 'Sum of Desired Rev Till Date3' },
          { source: 'ACHIVED REV3', sheetHeader: 'Sum of ACHIVED REV3', header: 'Sum of ACHIVED REV3' },
          { source: 'Diff in rev3', sheetHeader: 'Sum of Diff in rev3', header: 'Sum of Diff in rev3' },
          { source: 'Desired IP till Date3', sheetHeader: 'Sum of Desired IP till Date3', header: 'Sum of Desired IP till Date3' },
          { source: 'Achieved IP3', sheetHeader: 'Sum of Achieved IP3', header: 'Sum of Achieved IP3' },
          { source: 'Diff in IP3', sheetHeader: 'Sum of Diff in IP3', header: 'Sum of Diff in IP3' },
          { source: 'ADVT SPEND3', sheetHeader: 'Sum of ADVT SPEND3', header: 'Sum of ADVT SPEND3' },
          {
            source: 'AD SPEND%(WEBSITE)',
            sheetHeader: 'Sum of AD SPEND%(WEBSITE)',
            header: 'Sum of AD SPEND%(WEBSITE)',
            computed: {
              type: 'ratio',
              numerator: 'ADVT SPEND3',
              denominator: 'REV ACH TILL DATE(WEBSITE)',
              asPercentage: true,
            },
          },
          { source: 'Storage Fees3', sheetHeader: 'Sum of Storage Fees3', header: 'Sum of Storage Fees3' },
          { source: '% of IP ACH(WEBSITE)', sheetHeader: 'Sum of % of IP ACH(WEBSITE)', header: 'Sum of % of IP ACH(WEBSITE)' },
          { source: 'REV ACH TILL DATE(WEBSITE)', sheetHeader: 'Sum of REV ACH TILL DATE(WEBSITE)', header: 'Sum of REV ACH TILL DATE(WEBSITE)' },
          { source: 'ACH IP TILL DATE(WEBSITE)', sheetHeader: 'Sum of ACH IP TILL DATE(WEBSITE)', header: 'Sum of ACH IP TILL DATE(WEBSITE)' },
        ],
        filterFieldDefinitions: DASHBOARD_PIVOT_FILTER_FIELDS,
      },
    ];
    const dashboardPivotFilterState = new Map();
    const dashboardPivotFilterTableMap = new Map();
    let dashboardPivotFiltersInitialised = false;

    function normalizeDashboardPivotLabel(value) {
      if (value === null || value === undefined) {
        return '';
      }
      return String(value).trim().toLowerCase();
    }

    function normalizeDashboardFilterValue(value) {
      if (value === null || value === undefined) {
        return '';
      }
      return String(value).trim().toLowerCase();
    }

    function getDashboardPivotFieldEntry(state, fieldId) {
      if (!state) {
        return { options: [], keySet: new Set() };
      }
      if (!(state.fieldValueMap instanceof Map)) {
        state.fieldValueMap = new Map();
      }
      let entry = state.fieldValueMap.get(fieldId);
      if (!entry || typeof entry !== 'object') {
        entry = { options: [], keySet: new Set() };
        state.fieldValueMap.set(fieldId, entry);
      }
      if (!Array.isArray(entry.options)) {
        entry.options = [];
      }
      if (!(entry.keySet instanceof Set)) {
        entry.keySet = new Set(entry.options.map((option) => option.key));
      }
      return entry;
    }

    function setDashboardPivotFieldOptions(state, fieldId, options) {
      if (!state) {
        return;
      }
      if (!(state.fieldValueMap instanceof Map)) {
        state.fieldValueMap = new Map();
      }
      const normalizedOptions = [];
      const keySet = new Set();
      if (Array.isArray(options)) {
        options.forEach((option) => {
          const labelValue = option && Object.prototype.hasOwnProperty.call(option, 'label')
            ? option.label
            : option;
          const label = labelValue === null || labelValue === undefined ? '' : String(labelValue);
          const rawKey = option && typeof option.key === 'string' && option.key.length
            ? option.key
            : normalizeDashboardFilterValue(label);
          if (keySet.has(rawKey)) {
            return;
          }
          keySet.add(rawKey);
          normalizedOptions.push({
            key: rawKey,
            label,
            lower: option && typeof option.lower === 'string' && option.lower.length
              ? option.lower
              : rawKey,
            sortKey: Number.isFinite(option && option.sortKey) ? option.sortKey : null,
          });
        });
      }
      normalizedOptions.sort((a, b) => {
        const aHasSort = Number.isFinite(a.sortKey);
        const bHasSort = Number.isFinite(b.sortKey);
        if (aHasSort && bHasSort) {
          if (a.sortKey !== b.sortKey) {
            return a.sortKey - b.sortKey;
          }
        } else if (aHasSort !== bHasSort) {
          return aHasSort ? -1 : 1;
        }
        return a.lower.localeCompare(b.lower, undefined, { numeric: true, sensitivity: 'base' });
      });
      state.fieldValueMap.set(fieldId, { options: normalizedOptions, keySet });
    }

    function sanitizeDashboardPivotFieldSelection(state, fieldId) {
      if (!state || !(state.fieldSelections instanceof Map)) {
        return null;
      }
      const selection = state.fieldSelections.get(fieldId);
      if (!(selection instanceof Set) || !selection.size) {
        state.fieldSelections.set(fieldId, null);
        return null;
      }
      const entry = getDashboardPivotFieldEntry(state, fieldId);
      const keySet = entry.keySet instanceof Set ? entry.keySet : new Set();
      if (!keySet.size) {
        state.fieldSelections.set(fieldId, null);
        return null;
      }
      const sanitized = new Set();
      selection.forEach((key) => {
        if (keySet.has(key)) {
          sanitized.add(key);
        }
      });
      if (sanitized.size) {
        state.fieldSelections.set(fieldId, sanitized);
        return sanitized;
      }
      state.fieldSelections.set(fieldId, null);
      return null;
    }

    function ensureDashboardPivotActiveField(state) {
      if (!state || !Array.isArray(state.fieldDefinitions) || !state.fieldDefinitions.length) {
        return;
      }
      const currentFieldId = state.activeFieldId;
      const hasActive = state.fieldDefinitions.some((definition) => {
        if (definition.id !== currentFieldId) {
          return false;
        }
        const entry = getDashboardPivotFieldEntry(state, definition.id);
        return Array.isArray(entry.options) && entry.options.length > 0;
      });
      if (hasActive) {
        return;
      }
      const fallback = state.fieldDefinitions.find((definition) => {
        const entry = getDashboardPivotFieldEntry(state, definition.id);
        return Array.isArray(entry.options) && entry.options.length > 0;
      });
      if (fallback) {
        state.activeFieldId = fallback.id;
      } else if (state.fieldDefinitions[0]) {
        state.activeFieldId = state.fieldDefinitions[0].id;
      } else {
        state.activeFieldId = null;
      }
    }

    function buildDashboardPivotFilterModel(dataset) {
      const definitions = [{ id: 'row', label: 'Row labels', type: 'row' }];
      const optionMap = new Map();
      if (!dataset || !Array.isArray(dataset.columns) || !Array.isArray(dataset.rows)) {
        return { definitions, optionMap };
      }
      const columnValueOptions = buildColumnOptions(dataset);
      dataset.columns.forEach((columnName, columnIndex) => {
        if (isPlaceholderColumnName(columnName)) {
          return;
        }
        const label = columnName === null || columnName === undefined ? '' : String(columnName).trim();
        if (!label.length) {
          return;
        }
        const fieldId = `column-${columnIndex}`;
        definitions.push({ id: fieldId, label, type: 'column', columnIndex });
        const rawValues = Array.isArray(columnValueOptions[columnIndex]) ? columnValueOptions[columnIndex] : [];
        const seen = new Set();
        const options = [];
        rawValues.forEach((rawValue) => {
          let display = rawValue === null || rawValue === undefined ? '' : String(rawValue);
          let sortKey = null;
          if (typeof window.parseDateMetadata === 'function') {
            const metadata = window.parseDateMetadata(rawValue);
            if (metadata) {
              sortKey = metadata.sortValue;
              if (typeof window.formatDateLabel === 'function') {
                const formatted = window.formatDateLabel(metadata);
                if (formatted) {
                  display = formatted;
                }
              }
            }
          }
          const normalized = normalizeDashboardFilterValue(display);
          if (seen.has(normalized)) {
            return;
          }
          seen.add(normalized);
          options.push({ key: normalized, label: display, lower: normalized, sortKey });
        });
        optionMap.set(fieldId, options);
      });
      return { definitions, optionMap };
    }

    function updateDashboardPivotFilterModel(dataset) {
      const model = buildDashboardPivotFilterModel(dataset);
      DASHBOARD_PIVOT_FILTER_FIELDS = model.definitions;
      dashboardPivotFieldOptionCache = model.optionMap;
      dashboardPivotFilterState.forEach((state) => {
        state.fieldDefinitions = model.definitions.map((definition) => ({ ...definition }));
        if (!(state.fieldSelections instanceof Map)) {
          state.fieldSelections = new Map();
        }
        state.fieldValueMap = new Map();
        state.fieldDefinitions.forEach((definition) => {
          if (definition.id === 'row') {
            return;
          }
          const options = dashboardPivotFieldOptionCache.get(definition.id) || [];
          const clonedOptions = options.map((entry) => ({ ...entry }));
          setDashboardPivotFieldOptions(state, definition.id, clonedOptions);
          sanitizeDashboardPivotFieldSelection(state, definition.id);
        });
        if (!state.fieldDefinitions.some((definition) => definition.id === state.activeFieldId)) {
          state.activeFieldId = state.fieldDefinitions.length ? state.fieldDefinitions[0].id : null;
        }
        if (state.activeSelectionFieldId && !state.fieldDefinitions.some((definition) => definition.id === state.activeSelectionFieldId)) {
          state.activeSelectionFieldId = null;
          state.rowFilterSelection = null;
        }
        if (state.activeSelectionFieldId) {
          const sanitized = sanitizeDashboardPivotFieldSelection(state, state.activeSelectionFieldId);
          if (sanitized instanceof Set) {
            state.rowFilterSelection = new Set(sanitized);
          } else {
            state.rowFilterSelection = null;
            state.activeSelectionFieldId = null;
          }
        } else {
          state.rowFilterSelection = null;
        }
        ensureDashboardPivotActiveField(state);
        updateDashboardPivotRowFilterButtonState(state);
        if (dashboardPivotFiltersInitialised && state === activeDashboardPivotRowFilterState) {
          renderDashboardPivotRowFilterFieldButtons(state);
          renderDashboardPivotRowFilterOptions();
        }
      });
    }

    function doesDashboardPivotRowMatchDatasetFilter(rowMetadata, dataset, columnIndex, selectionSet) {
      if (!rowMetadata || !dataset || !Array.isArray(dataset.rows) || !Array.isArray(dataset.columns)) {
        return false;
      }
      if (!Number.isInteger(columnIndex) || columnIndex < 0 || columnIndex >= dataset.columns.length) {
        return false;
      }
      if (!(selectionSet instanceof Set) || !selectionSet.size) {
        return false;
      }
      const columnName = dataset.columns[columnIndex] || '';
      const rowIndexes = Array.isArray(rowMetadata.rowIndexes) ? rowMetadata.rowIndexes : [];
      if (!rowIndexes.length) {
        return false;
      }
      for (let index = 0; index < rowIndexes.length; index += 1) {
        const datasetRowIndex = rowIndexes[index];
        if (!Number.isInteger(datasetRowIndex) || datasetRowIndex < 0 || datasetRowIndex >= dataset.rows.length) {
          continue;
        }
        const datasetRow = dataset.rows[datasetRowIndex];
        if (!Array.isArray(datasetRow)) {
          continue;
        }
        const rawValue = columnIndex < datasetRow.length ? datasetRow[columnIndex] : '';
        const formatted = formatCellValue(rawValue, columnName);
        const normalized = normalizeDashboardFilterValue(formatted);
        if (selectionSet.has(normalized)) {
          return true;
        }
      }
      return false;
    }

    function updateDashboardPivotRowFilterButtonState(state) {
      if (!state) {
        return;
      }
      const button = state.rowFilterButton || null;
      const clearButton = state.rowFilterClearButton || null;
      ensureDashboardPivotActiveField(state);
      const hasOptions = Array.isArray(state.fieldDefinitions)
        ? state.fieldDefinitions.some((definition) => {
            const entry = getDashboardPivotFieldEntry(state, definition.id);
            return Array.isArray(entry.options) && entry.options.length > 0;
          })
        : false;
      const hasActiveFilter = state.rowFilterSelection instanceof Set && state.rowFilterSelection.size > 0;
      if (button) {
        if (hasOptions) {
          button.removeAttribute('disabled');
          button.setAttribute('aria-hidden', 'false');
        } else {
          button.setAttribute('disabled', 'true');
          button.setAttribute('aria-hidden', 'true');
          button.setAttribute('aria-expanded', 'false');
        }
        button.dataset.active = hasActiveFilter ? 'true' : 'false';
      }
      if (clearButton) {
        if (hasActiveFilter) {
          clearButton.hidden = false;
          clearButton.removeAttribute('disabled');
          clearButton.setAttribute('aria-hidden', 'false');
        } else {
          clearButton.hidden = true;
          clearButton.setAttribute('disabled', 'true');
          clearButton.setAttribute('aria-hidden', 'true');
        }
      }
    }

    function renderDashboardPivotRowFilterFieldButtons(state) {
      if (!dashboardPivotRowFilterFieldOptionsElement) {
        return;
      }
      const selectElement = dashboardPivotRowFilterFieldOptionsElement;
      const fallbackDefinitions = Array.isArray(state?.fieldDefinitions) && state.fieldDefinitions.length
        ? state.fieldDefinitions
        : DASHBOARD_PIVOT_FILTER_FIELDS;
      selectElement.innerHTML = '';
      selectElement.onchange = null;

      if (!state) {
        fallbackDefinitions.forEach((definition, index) => {
          const option = document.createElement('option');
          option.value = definition.id;
          option.textContent = definition.label || definition.id;
          option.disabled = true;
          if (index === 0) {
            option.selected = true;
          }
          selectElement.appendChild(option);
        });
        selectElement.setAttribute('disabled', 'true');
        selectElement.setAttribute('aria-disabled', 'true');
        return;
      }

      const definitions = fallbackDefinitions.map((definition) => ({ ...definition }));
      ensureDashboardPivotActiveField(state);
      let activeFieldId = state.activeFieldId || null;
      let hasEnabledOption = false;

      definitions.forEach((definition) => {
        const option = document.createElement('option');
        option.value = definition.id;
        option.textContent = definition.label || definition.id;
        const entry = getDashboardPivotFieldEntry(state, definition.id);
        const hasValues = Array.isArray(entry.options) && entry.options.length > 0;
        if (!hasValues) {
          option.disabled = true;
          if (activeFieldId === definition.id) {
            activeFieldId = null;
          }
        } else {
          hasEnabledOption = true;
        }
        selectElement.appendChild(option);
      });

      if (!activeFieldId) {
        const fallback = definitions.find((definition) => {
          const entry = getDashboardPivotFieldEntry(state, definition.id);
          return Array.isArray(entry.options) && entry.options.length > 0;
        });
        activeFieldId = fallback ? fallback.id : definitions[0]?.id || null;
      }

      if (activeFieldId) {
        state.activeFieldId = activeFieldId;
        const options = Array.from(selectElement.options);
        const selectedOption = options.find((option) => option.value === activeFieldId && !option.disabled);
        if (selectedOption) {
          selectedOption.selected = true;
          selectElement.value = selectedOption.value;
        } else if (options.length) {
          options[0].selected = true;
          selectElement.value = options[0].value;
        }
      }

      if (hasEnabledOption) {
        selectElement.removeAttribute('disabled');
        selectElement.removeAttribute('aria-disabled');
      } else {
        selectElement.setAttribute('disabled', 'true');
        selectElement.setAttribute('aria-disabled', 'true');
      }

      selectElement.onchange = (event) => {
        const fieldId = event.target.value;
        if (!fieldId || state.activeFieldId === fieldId) {
          return;
        }
        const definition = definitions.find((entry) => entry.id === fieldId);
        const entry = definition ? getDashboardPivotFieldEntry(state, definition.id) : { options: [] };
        const hasValues = Array.isArray(entry.options) && entry.options.length > 0;
        if (!hasValues) {
          renderDashboardPivotRowFilterFieldButtons(state);
          return;
        }
        state.activeFieldId = fieldId;
        if (state.fieldSelections instanceof Map) {
          const storedSelection = state.fieldSelections.get(fieldId);
          if (storedSelection instanceof Set) {
            dashboardPivotRowFilterPendingSelection = new Set(storedSelection);
          } else {
            dashboardPivotRowFilterPendingSelection = null;
          }
        } else {
          dashboardPivotRowFilterPendingSelection = null;
        }
        renderDashboardPivotRowFilterFieldButtons(state);
        renderDashboardPivotRowFilterOptions();
      };
    }

    function updateDashboardPivotRowFilterSelectAllState() {
      if (!dashboardPivotRowFilterSelectAllInput) {
        return;
      }
      const checkboxes = dashboardPivotRowFilterOptionsElement
        ? Array.from(dashboardPivotRowFilterOptionsElement.querySelectorAll('input[type="checkbox"]'))
        : [];
      if (!checkboxes.length) {
        dashboardPivotRowFilterSelectAllInput.checked = false;
        dashboardPivotRowFilterSelectAllInput.indeterminate = false;
        dashboardPivotRowFilterSelectAllInput.disabled = true;
        return;
      }
      dashboardPivotRowFilterSelectAllInput.disabled = false;
      let selectedCount = 0;
      checkboxes.forEach((checkbox) => {
        if (checkbox.checked) {
          selectedCount += 1;
        }
      });
      if (selectedCount === 0) {
        dashboardPivotRowFilterSelectAllInput.checked = false;
        dashboardPivotRowFilterSelectAllInput.indeterminate = false;
      } else if (selectedCount === checkboxes.length) {
        dashboardPivotRowFilterSelectAllInput.checked = true;
        dashboardPivotRowFilterSelectAllInput.indeterminate = false;
      } else {
        dashboardPivotRowFilterSelectAllInput.checked = false;
        dashboardPivotRowFilterSelectAllInput.indeterminate = true;
      }
    }

    function renderDashboardPivotRowFilterOptions() {
      if (!dashboardPivotRowFilterOptionsElement || !activeDashboardPivotRowFilterState) {
        return;
      }
      const state = activeDashboardPivotRowFilterState;
      ensureDashboardPivotActiveField(state);
      const activeFieldId = state.activeFieldId || 'row';
      const entry = getDashboardPivotFieldEntry(state, activeFieldId);
      const options = Array.isArray(entry.options) ? entry.options : [];
      const keySet = entry.keySet instanceof Set
        ? entry.keySet
        : new Set(options.map((option) => option.key));
      const total = options.length;
      if (dashboardPivotRowFilterPendingSelection instanceof Set) {
        const sanitized = new Set();
        dashboardPivotRowFilterPendingSelection.forEach((key) => {
          if (keySet.has(key)) {
            sanitized.add(key);
          }
        });
        if (sanitized.size >= total && total > 0) {
          dashboardPivotRowFilterPendingSelection = null;
        } else {
          dashboardPivotRowFilterPendingSelection = sanitized;
        }
      } else if (dashboardPivotRowFilterPendingSelection !== null && !(dashboardPivotRowFilterPendingSelection instanceof Set)) {
        dashboardPivotRowFilterPendingSelection = null;
      }

      if (!total) {
        dashboardPivotRowFilterOptionsElement.innerHTML = '';
        if (dashboardPivotRowFilterEmptyElement) {
          dashboardPivotRowFilterEmptyElement.hidden = false;
        }
        if (dashboardPivotRowFilterApplyButton) {
          dashboardPivotRowFilterApplyButton.setAttribute('disabled', 'true');
        }
        if (dashboardPivotRowFilterResetButton) {
          dashboardPivotRowFilterResetButton.setAttribute('disabled', 'true');
        }
        if (dashboardPivotRowFilterSelectAllInput) {
          dashboardPivotRowFilterSelectAllInput.checked = false;
          dashboardPivotRowFilterSelectAllInput.indeterminate = false;
          dashboardPivotRowFilterSelectAllInput.disabled = true;
        }
        return;
      }

      if (dashboardPivotRowFilterEmptyElement) {
        dashboardPivotRowFilterEmptyElement.hidden = true;
      }
      if (dashboardPivotRowFilterApplyButton) {
        dashboardPivotRowFilterApplyButton.removeAttribute('disabled');
      }
      if (dashboardPivotRowFilterResetButton) {
        dashboardPivotRowFilterResetButton.removeAttribute('disabled');
      }

      const selection = dashboardPivotRowFilterPendingSelection instanceof Set
        ? dashboardPivotRowFilterPendingSelection
        : null;
      const optionMarkup = options
        .map((entry, index) => {
          const checkboxId = `dashboard-pivot-filter-option-${state.id}-${index}`;
          const isChecked = selection === null || selection.has(entry.key);
          const safeValue = escapeHtml(entry.key);
          const safeLabel = escapeHtml(optionLabel(entry.label));
          return `<label class="regular-filter__option" for="${checkboxId}"><input type="checkbox" id="${checkboxId}" value="${safeValue}" ${isChecked ? 'checked' : ''}><span>${safeLabel}</span></label>`;
        })
        .join('');
      dashboardPivotRowFilterOptionsElement.innerHTML = optionMarkup;
      animateOptionList(dashboardPivotRowFilterOptionsElement, '.regular-filter__option');
      updateDashboardPivotRowFilterSelectAllState();
    }

    function openDashboardPivotRowFilter(state, triggerButton = null) {
      if (!state) {
        return;
      }
      initializeDashboardPivotRowFilterDialog();
      if (!dashboardPivotRowFilterContainerElement) {
        return;
      }
      activeDashboardPivotRowFilterState = state;
      activeDashboardPivotRowFilterTrigger = triggerButton || state.rowFilterButton || null;
      ensureDashboardPivotActiveField(state);
      const initialFieldId = state.activeFieldId || 'row';
      let pendingSelection = null;
      if (state.fieldSelections instanceof Map) {
        const storedSelection = state.fieldSelections.get(initialFieldId);
        if (storedSelection instanceof Set) {
          pendingSelection = new Set(storedSelection);
        }
      }
      if (!pendingSelection && state.activeSelectionFieldId === initialFieldId && state.rowFilterSelection instanceof Set) {
        pendingSelection = new Set(state.rowFilterSelection);
      }
      dashboardPivotRowFilterPendingSelection = pendingSelection;
      const definitions = Array.isArray(state.fieldDefinitions) && state.fieldDefinitions.length
        ? state.fieldDefinitions
        : DASHBOARD_PIVOT_FILTER_FIELDS;
      if (!definitions.some((definition) => definition.id === state.activeFieldId)) {
        state.activeFieldId = definitions.length ? definitions[0].id : 'row';
      }
      const titleElement = document.getElementById('dashboard-pivot-filter-title');
      if (titleElement) {
        const label = state.config && (state.config.displayLabel || state.config.id || 'pivot');
        titleElement.textContent = `Filter ${label} pivot rows`;
      }
      renderDashboardPivotRowFilterFieldButtons(state);
      renderDashboardPivotRowFilterOptions();
      showFilterContainer(dashboardPivotRowFilterContainerElement);
      if (state.rowFilterButton) {
        state.rowFilterButton.setAttribute('aria-expanded', 'true');
      }
      requestAnimationFrame(() => {
        const firstCheckbox = dashboardPivotRowFilterOptionsElement
          ? dashboardPivotRowFilterOptionsElement.querySelector('input[type="checkbox"]')
          : null;
        if (firstCheckbox) {
          firstCheckbox.focus();
        } else if (dashboardPivotRowFilterApplyButton) {
          dashboardPivotRowFilterApplyButton.focus();
        }
      });
    }

    function closeDashboardPivotRowFilter(options = {}) {
      if (!dashboardPivotRowFilterContainerElement) {
        return;
      }
      const { returnFocus = true } = options;
      const previousTrigger = activeDashboardPivotRowFilterTrigger;
      hideFilterContainer(dashboardPivotRowFilterContainerElement);
      if (activeDashboardPivotRowFilterState && activeDashboardPivotRowFilterState.rowFilterButton) {
        activeDashboardPivotRowFilterState.rowFilterButton.setAttribute('aria-expanded', 'false');
      }
      activeDashboardPivotRowFilterState = null;
      activeDashboardPivotRowFilterTrigger = null;
      dashboardPivotRowFilterPendingSelection = null;
      if (returnFocus && previousTrigger && typeof previousTrigger.focus === 'function') {
        previousTrigger.focus();
      }
    }

    function initializeDashboardPivotRowFilterDialog() {
      if (dashboardPivotRowFilterInitialised) {
        return;
      }
      dashboardPivotRowFilterContainerElement = document.getElementById('dashboard-pivot-filter');
      dashboardPivotRowFilterOptionsElement = document.getElementById('dashboard-pivot-filter-options');
      dashboardPivotRowFilterFieldOptionsElement = document.getElementById('dashboard-pivot-filter-field-options');
      dashboardPivotRowFilterEmptyElement = document.getElementById('dashboard-pivot-filter-empty');
      dashboardPivotRowFilterSelectAllInput = document.getElementById('dashboard-pivot-filter-select-all');
      dashboardPivotRowFilterApplyButton = document.getElementById('dashboard-pivot-filter-apply');
      dashboardPivotRowFilterResetButton = document.getElementById('dashboard-pivot-filter-reset');
      dashboardPivotRowFilterCloseButton = dashboardPivotRowFilterContainerElement
        ? dashboardPivotRowFilterContainerElement.querySelector('.regular-filter__close')
        : null;

      const elementsReady = [
        dashboardPivotRowFilterContainerElement,
        dashboardPivotRowFilterOptionsElement,
        dashboardPivotRowFilterFieldOptionsElement,
        dashboardPivotRowFilterApplyButton,
        dashboardPivotRowFilterResetButton,
        dashboardPivotRowFilterCloseButton,
        dashboardPivotRowFilterSelectAllInput,
      ].every(Boolean);

      if (!elementsReady) {
        return;
      }

      dashboardPivotRowFilterCloseButton.addEventListener('click', () => closeDashboardPivotRowFilter());

      dashboardPivotRowFilterContainerElement.addEventListener('click', (event) => {
        const target = event.target;
        if (
          target === dashboardPivotRowFilterContainerElement
          || (target instanceof HTMLElement && target.classList.contains('regular-filter__backdrop'))
        ) {
          closeDashboardPivotRowFilter();
        }
      });

      dashboardPivotRowFilterOptionsElement.addEventListener('change', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') {
          return;
        }
        const state = activeDashboardPivotRowFilterState;
        if (!state) {
          return;
        }
        ensureDashboardPivotActiveField(state);
        const activeFieldId = state.activeFieldId || 'row';
        const entry = getDashboardPivotFieldEntry(state, activeFieldId);
        const options = Array.isArray(entry.options) ? entry.options : [];
        const keySet = entry.keySet instanceof Set
          ? entry.keySet
          : new Set(options.map((option) => option.key));
        if (!keySet.has(target.value) && target.checked) {
          return;
        }
        const allKeys = Array.from(keySet);
        const currentSelection = dashboardPivotRowFilterPendingSelection instanceof Set
          ? new Set(dashboardPivotRowFilterPendingSelection)
          : new Set(allKeys);
        if (target.checked) {
          currentSelection.add(target.value);
        } else {
          currentSelection.delete(target.value);
        }
        if (currentSelection.size === allKeys.length) {
          dashboardPivotRowFilterPendingSelection = null;
        } else {
          dashboardPivotRowFilterPendingSelection = currentSelection;
        }
        renderDashboardPivotRowFilterOptions();
      });

      dashboardPivotRowFilterSelectAllInput.addEventListener('change', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement)) {
          return;
        }
        const state = activeDashboardPivotRowFilterState;
        if (!state) {
          return;
        }
        ensureDashboardPivotActiveField(state);
        const activeFieldId = state.activeFieldId || 'row';
        const entry = getDashboardPivotFieldEntry(state, activeFieldId);
        const options = Array.isArray(entry.options) ? entry.options : [];
        if (!options.length) {
          return;
        }
        if (target.checked) {
          dashboardPivotRowFilterPendingSelection = null;
        } else {
          dashboardPivotRowFilterPendingSelection = new Set();
        }
        renderDashboardPivotRowFilterOptions();
      });

      dashboardPivotRowFilterApplyButton.addEventListener('click', () => {
        if (!activeDashboardPivotRowFilterState) {
          return;
        }
        const state = activeDashboardPivotRowFilterState;
        ensureDashboardPivotActiveField(state);
        const activeFieldId = state.activeFieldId || 'row';
        const entry = getDashboardPivotFieldEntry(state, activeFieldId);
        const options = Array.isArray(entry.options) ? entry.options : [];
        const keySet = entry.keySet instanceof Set
          ? entry.keySet
          : new Set(options.map((option) => option.key));
        const total = keySet.size;
        let selection = null;
        if (dashboardPivotRowFilterPendingSelection instanceof Set) {
          const sanitizedSelection = new Set();
          dashboardPivotRowFilterPendingSelection.forEach((key) => {
            if (keySet.has(key)) {
              sanitizedSelection.add(key);
            }
          });
          if (total > 0 && sanitizedSelection.size > 0 && sanitizedSelection.size < total) {
            selection = sanitizedSelection;
          }
        }
        if (selection instanceof Set) {
          state.rowFilterSelection = new Set(selection);
          state.activeSelectionFieldId = activeFieldId;
          if (!(state.fieldSelections instanceof Map)) {
            state.fieldSelections = new Map();
          }
          state.fieldSelections.set(activeFieldId, new Set(selection));
        } else {
          state.rowFilterSelection = null;
          if (state.fieldSelections instanceof Map) {
            state.fieldSelections.set(activeFieldId, null);
          }
          if (state.activeSelectionFieldId === activeFieldId) {
            state.activeSelectionFieldId = null;
          }
        }
        flashButtonBusy(dashboardPivotRowFilterApplyButton);
        applyDashboardPivotFilter(state.config);
        closeDashboardPivotRowFilter();
      });

      dashboardPivotRowFilterResetButton.addEventListener('click', () => {
        if (!activeDashboardPivotRowFilterState) {
          return;
        }
        dashboardPivotRowFilterPendingSelection = null;
        const state = activeDashboardPivotRowFilterState;
        if (!(state.fieldSelections instanceof Map)) {
          state.fieldSelections = new Map();
        }
        ensureDashboardPivotActiveField(state);
        const activeFieldId = state.activeFieldId || 'row';
        state.fieldSelections.set(activeFieldId, null);
        if (state.activeSelectionFieldId === activeFieldId) {
          state.activeSelectionFieldId = null;
          state.rowFilterSelection = null;
        }
        state.rowFilterSelection = null;
        renderDashboardPivotRowFilterOptions();
        applyDashboardPivotFilter(state.config);
      });

      document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && dashboardPivotRowFilterContainerElement.classList.contains('is-visible')) {
          closeDashboardPivotRowFilter();
        }
      });

      dashboardPivotRowFilterInitialised = true;
    }
    const EXCEL_DEFAULT_HEADER_SEARCH_VALUES = EXCEL_KEY_COLUMN_NAMES;

    function getTabPanelElement(tabId) {
      if (!tabId) {
        return null;
      }
      return Array.from(tabPanels).find((panel) => panel.dataset.tab === tabId) || null;
    }

    function showTabPanel(panel) {
      if (!panel) {
        return;
      }
      panel.classList.remove('is-exiting');
      panel.classList.add('active');
      panel.setAttribute('aria-hidden', 'false');
    }

    function hideTabPanel(panel) {
      if (!panel) {
        return;
      }
      if (!panel.classList.contains('active') && !panel.classList.contains('is-exiting')) {
        panel.setAttribute('aria-hidden', 'true');
        return;
      }
      panel.classList.remove('active');
      panel.setAttribute('aria-hidden', 'true');
      panel.classList.add('is-exiting');
      function finalize() {
        panel.classList.remove('is-exiting');
        panel.removeEventListener('animationend', handleAnimationEnd);
      }
      function handleAnimationEnd(event) {
        if (event.target !== panel) {
          return;
        }
        finalize();
      }
      panel.addEventListener('animationend', handleAnimationEnd);
      window.setTimeout(finalize, PANEL_TRANSITION_DURATION);
    }

    function updateTabIndicator(targetTab) {
      if (!tabNavElement || !tabNavIndicatorElement) {
        return;
      }
      const activeButton = Array.from(tabButtons).find((button) => button.dataset.tab === targetTab);
      if (!activeButton) {
        tabNavElement.classList.remove('has-active-indicator');
        indicatorPreviousLeft = null;
        if (tabNavIndicatorElement) {
          tabNavIndicatorElement.removeAttribute('data-direction');
        }
        return;
      }
      const applyPosition = () => {
        const navRect = tabNavElement.getBoundingClientRect();
        const buttonRect = activeButton.getBoundingClientRect();
        const left = buttonRect.left - navRect.left;
        const top = buttonRect.top - navRect.top;
        if (indicatorPreviousLeft !== null) {
          const direction = left >= indicatorPreviousLeft ? 'right' : 'left';
          tabNavIndicatorElement.setAttribute('data-direction', direction);
        } else {
          tabNavIndicatorElement.setAttribute('data-direction', 'right');
        }
        tabNavIndicatorElement.style.setProperty('--indicator-left', `${left}px`);
        tabNavIndicatorElement.style.setProperty('--indicator-top', `${top}px`);
        tabNavIndicatorElement.style.setProperty('--indicator-width', `${buttonRect.width}px`);
        tabNavIndicatorElement.style.setProperty('--indicator-height', `${buttonRect.height}px`);
        tabNavElement.classList.add('has-active-indicator');
        indicatorPreviousLeft = left;
        updateTabNavScrollShadows();
      };
      if (typeof requestAnimationFrame === 'function') {
        requestAnimationFrame(applyPosition);
      } else {
        applyPosition();
      }
    }

    function setTabPanelLoading(tabId, isLoading, message = 'Loading…') {
      const panel = getTabPanelElement(tabId);
      if (!panel) {
        return;
      }
      let overlay = panel.querySelector(':scope > .panel-loading');
      if (!overlay) {
        overlay = document.createElement('div');
        overlay.className = 'panel-loading';
        overlay.setAttribute('aria-hidden', 'true');
        overlay.innerHTML = `
          <div class="panel-loading__content" role="status">
            <span class="panel-loading__spinner" aria-hidden="true"></span>
            <span class="panel-loading__message"></span>
          </div>
        `;
        panel.appendChild(overlay);
      }
      const messageElement = overlay.querySelector('.panel-loading__message');
      if (messageElement && typeof message === 'string' && message.trim().length) {
        messageElement.textContent = message;
      }
      const shouldShow = Boolean(isLoading);
      overlay.classList.toggle('is-visible', shouldShow);
      overlay.setAttribute('aria-hidden', shouldShow ? 'false' : 'true');
    }

    function showFilterContainer(element) {
      if (!element) {
        return;
      }
      element.classList.remove('is-closing');
      element.removeAttribute('hidden');
      element.setAttribute('aria-hidden', 'false');
      requestAnimationFrame(() => {
        element.classList.add('is-visible');
      });
    }

    function hideFilterContainer(element) {
      if (!element) {
        return;
      }
      element.setAttribute('aria-hidden', 'true');
      element.classList.add('is-closing');
      element.classList.remove('is-visible');
      const dialog = element.querySelector('.regular-filter__dialog');
      const transitionTarget = dialog || element;
      let completed = false;
      function finalize() {
        if (completed) {
          return;
        }
        completed = true;
        element.classList.remove('is-closing');
        element.setAttribute('hidden', '');
        transitionTarget.removeEventListener('transitionend', handleTransitionEnd);
      }
      function handleTransitionEnd(event) {
        if (event.target !== transitionTarget) {
          return;
        }
        finalize();
      }
      transitionTarget.addEventListener('transitionend', handleTransitionEnd);
      window.setTimeout(finalize, FILTER_TRANSITION_DURATION + 60);
    }

    function flashButtonBusy(button, fallbackLabel = 'Applying…') {
      if (!button) {
        return;
      }
      const existingLabel = button.getAttribute('data-loading-label');
      const label = existingLabel && existingLabel.trim().length ? existingLabel : fallbackLabel;
      button.setAttribute('data-loading-label', label);
      button.dataset.loading = 'true';
      window.setTimeout(() => {
        delete button.dataset.loading;
        if (!existingLabel) {
          button.removeAttribute('data-loading-label');
        }
      }, BUTTON_BUSY_TIMEOUT);
    }

    function animateOptionList(container, selector) {
      if (!container) {
        return;
      }
      const options = Array.from(container.querySelectorAll(selector));
      if (!options.length) {
        return;
      }
      options.forEach((option) => {
        option.classList.remove('is-visible');
        option.style.removeProperty('--stagger-index');
      });
      if (shouldReduceMotion) {
        options.forEach((option) => option.classList.add('is-visible'));
        return;
      }
      requestAnimationFrame(() => {
        options.forEach((option, index) => {
          option.style.setProperty('--stagger-index', index);
          option.classList.add('is-visible');
        });
      });
    }

    let loSalesOrderCache = [];
    let loDisplayNameOverridesCache = new Map();
    let loBaselineSpendPivot = null;
    let loSpendScalingData = null;
    let loBaselineAdSpendPivot = null;

    function normaliseSheetName(value) {
      if (typeof value !== 'string') {
        return '';
      }
      return value.replace(/[^0-9a-z]+/gi, '').toLowerCase();
    }

    function normaliseNewProductLabel(value) {
      if (value === null || value === undefined) {
        return '';
      }
      if (typeof value === 'string') {
        return value.trim().toUpperCase();
      }
      return String(value).trim().toUpperCase();
    }

    function normaliseNewProductStatus(value) {
      if (value === null || value === undefined) {
        return '';
      }
      if (typeof value === 'string') {
        return value.trim().toUpperCase();
      }
      return String(value).trim().toUpperCase();
    }

    function sortNewProductNewOldValues(values) {
      if (!Array.isArray(values) || !values.length) {
        return [];
      }
      const priorityOrder = new Map([
        ['NEW', -2],
        ['OLD', -1],
      ]);
      const normalise = (value) => {
        if (typeof value === 'string') {
          return value.trim().toUpperCase();
        }
        if (value === null || value === undefined) {
          return '';
        }
        return String(value).trim().toUpperCase();
      };
      return values
        .slice()
        .filter((value, index, source) => source.indexOf(value) === index)
        .sort((a, b) => {
          const normalisedA = normalise(a);
          const normalisedB = normalise(b);
          const priorityA = priorityOrder.has(normalisedA)
            ? priorityOrder.get(normalisedA)
            : Number.POSITIVE_INFINITY;
          const priorityB = priorityOrder.has(normalisedB)
            ? priorityOrder.get(normalisedB)
            : Number.POSITIVE_INFINITY;
          if (priorityA !== priorityB) {
            return priorityA - priorityB;
          }
          if (!normalisedA && !normalisedB) {
            return 0;
          }
          if (!normalisedA) {
            return 1;
          }
          if (!normalisedB) {
            return -1;
          }
          return normalisedA.localeCompare(normalisedB);
        });
    }

    function coerceCellValue(cell) {
      if (cell === null || cell === undefined) {
        return '';
      }
      if (typeof cell === 'number') {
        return Number.isFinite(cell) ? String(cell) : '';
      }
      if (typeof cell === 'boolean') {
        return cell ? 'TRUE' : 'FALSE';
      }
      const text = String(cell);
      return text.trim().length ? text : '';
    }

    function isPlaceholderColumnName(name) {
      if (name === null || name === undefined) {
        return true;
      }
      const value = typeof name === 'string' ? name : String(name);
      const trimmed = value.trim();
      if (!trimmed.length) {
        return true;
      }
      return /^column\d*$/i.test(trimmed);
    }

    function findWorksheetFromWorkbook(workbook, options = {}) {
      if (!workbook || !Array.isArray(workbook.SheetNames) || !workbook.SheetNames.length) {
        return { worksheet: null, sheetName: null };
      }
      const sheetNames = workbook.SheetNames;
      const normalisedEntries = sheetNames.map((name) => ({
        name,
        normalised: normaliseSheetName(name),
      }));
      const candidates = Array.isArray(options.candidates) && options.candidates.length
        ? options.candidates
        : EXCEL_REGULAR_SHEET_CANDIDATES;
      for (const candidate of candidates) {
        const normalised = normaliseSheetName(candidate);
        if (!normalised) {
          continue;
        }
        const exactMatch = normalisedEntries.find((entry) => entry.normalised === normalised);
        if (exactMatch) {
          return { worksheet: workbook.Sheets[exactMatch.name], sheetName: exactMatch.name };
        }
        const prefixMatch = normalisedEntries.find((entry) => entry.normalised.startsWith(normalised));
        if (prefixMatch) {
          return { worksheet: workbook.Sheets[prefixMatch.name], sheetName: prefixMatch.name };
        }
      }
      const fallbackPredicate = typeof options.fallbackPredicate === 'function'
        ? options.fallbackPredicate
        : (name) => normaliseSheetName(name).includes('regular');
      const inferred = fallbackPredicate
        ? normalisedEntries.find((entry) => fallbackPredicate(entry.name))
        : undefined;
      if (inferred) {
        return { worksheet: workbook.Sheets[inferred.name], sheetName: inferred.name };
      }
      const firstName = sheetNames[0];
      return { worksheet: workbook.Sheets[firstName], sheetName: firstName };
    }

    function buildMergeLookup(worksheet) {
      const merges = Array.isArray(worksheet && worksheet['!merges']) ? worksheet['!merges'] : [];
      if (!merges.length) {
        return new Map();
      }
      const lookup = new Map();
      const encodeKey = (row, column) => `${row}:${column}`;
      merges.forEach((merge) => {
        if (!merge || typeof merge !== 'object') {
          return;
        }
        const start = merge.s || merge.start || {};
        const end = merge.e || merge.end || start;
        const startRow = Number.isFinite(start.r) ? start.r : 0;
        const startCol = Number.isFinite(start.c) ? start.c : 0;
        const endRow = Number.isFinite(end.r) ? end.r : startRow;
        const endCol = Number.isFinite(end.c) ? end.c : startCol;
        for (let row = startRow; row <= endRow; row += 1) {
          for (let column = startCol; column <= endCol; column += 1) {
            if (row === startRow && column === startCol) {
              continue;
            }
            lookup.set(encodeKey(row, column), { row: startRow, column: startCol });
          }
        }
      });
      return lookup;
    }

    function extractWorksheetRows(worksheet) {
      if (!worksheet || typeof worksheet !== 'object') {
        return [];
      }
      const ref = worksheet['!ref'];
      if (typeof ref !== 'string' || !ref.length) {
        return [];
      }
      const range = XLSX.utils.decode_range(ref);
      if (!range || !Number.isFinite(range.s.r) || !Number.isFinite(range.s.c)) {
        return [];
      }
      const rows = [];
      const mergeLookup = buildMergeLookup(worksheet);
      const encodeKey = (row, column) => `${row}:${column}`;
      for (let rowIndex = range.s.r; rowIndex <= range.e.r; rowIndex += 1) {
        const row = [];
        for (let columnIndex = range.s.c; columnIndex <= range.e.c; columnIndex += 1) {
          const address = { r: rowIndex, c: columnIndex };
          let cell = worksheet[XLSX.utils.encode_cell(address)];
          if ((!cell || cell.v === undefined) && mergeLookup.size) {
            const source = mergeLookup.get(encodeKey(rowIndex, columnIndex));
            if (source) {
              cell = worksheet[XLSX.utils.encode_cell({ r: source.row, c: source.column })];
            }
          }
          const value = cell && cell.v !== undefined ? cell.v : '';
          row.push(value);
        }
        rows.push(row);
      }
      return rows;
    }

    function buildDatasetFromWorksheet(worksheet, options = {}) {
      if (!worksheet) {
        throw new Error('Worksheet not found in workbook');
      }
      const rawRows = extractWorksheetRows(worksheet);
      if (!Array.isArray(rawRows) || !rawRows.length) {
        throw new Error('Worksheet is empty');
      }
      const headerSearchValues = Array.isArray(options.headerSearchValues) && options.headerSearchValues.length
        ? options.headerSearchValues
        : EXCEL_DEFAULT_HEADER_SEARCH_VALUES;
      const headerSearchLower = headerSearchValues
        .map((value) => (typeof value === 'string' ? value.trim().toLowerCase() : ''))
        .filter((value) => value.length);

      let headerRow = null;
      let headerIndex = -1;
      const includeHeaderPreamble = options.includeHeaderPreamble === true;

      if (Number.isInteger(options.headerRowIndex)
        && options.headerRowIndex >= 0
        && options.headerRowIndex < rawRows.length) {
        headerIndex = options.headerRowIndex;
        headerRow = Array.isArray(rawRows[headerIndex]) ? rawRows[headerIndex] : [];
      } else {
        for (let i = 0; i < rawRows.length; i += 1) {
          const row = Array.isArray(rawRows[i]) ? rawRows[i] : [];
          const hasHeaderKey = row.some((value) => {
            if (typeof value !== 'string') {
              return false;
            }
            const normalised = value.trim().toLowerCase();
            if (!normalised) {
              return false;
            }
            return headerSearchLower.some((needle) => normalised.includes(needle));
          });
          if (hasHeaderKey) {
            headerRow = row;
            headerIndex = i;
            break;
          }
        }
      }
      if (!headerRow || headerIndex === -1) {
        throw new Error('Failed to locate header row in worksheet');
      }
      const headerCounts = new Map();
      const columns = headerRow.map((cell) => {
        const baseNameRaw = typeof cell === 'string' ? cell.trim() : cell;
        const baseName = baseNameRaw && String(baseNameRaw).trim().length ? String(baseNameRaw).trim() : 'Column';
        const count = (headerCounts.get(baseName) || 0) + 1;
        headerCounts.set(baseName, count);
        if (count === 1) {
          return baseName;
        }
        return `${baseName}${count}`;
      });
      const keyColumnCandidates = Array.isArray(options.keyColumnNames) && options.keyColumnNames.length
        ? options.keyColumnNames
        : EXCEL_KEY_COLUMN_NAMES;
      const keyColumnLookup = keyColumnCandidates
        .map((name) => (typeof name === 'string' ? name.trim().toLowerCase() : ''))
        .filter((name) => name.length);

      const keyColumnIndexes = columns.reduce((indexes, name, index) => {
        const normalized = typeof name === 'string' ? name.trim().toLowerCase() : '';
        if (keyColumnLookup.some((candidate) => candidate === normalized)) {
          indexes.push(index);
        }
        return indexes;
      }, []);
      let headerPreamble = null;
      if (includeHeaderPreamble && headerIndex > 0) {
        const columnCount = columns.length;
        const normalizedRows = [];
        for (let rowIndex = 0; rowIndex < headerIndex; rowIndex += 1) {
          const sourceRow = Array.isArray(rawRows[rowIndex]) ? rawRows[rowIndex] : [];
          const normalizedRow = [];
          for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
            normalizedRow.push(coerceCellValue(sourceRow[columnIndex]));
          }
          normalizedRows.push(normalizedRow);
        }
        const merges = Array.isArray(worksheet['!merges']) ? worksheet['!merges'] : [];
        const mergeEntries = [];
        merges.forEach((merge) => {
          if (!merge || typeof merge !== 'object') {
            return;
          }
          const startRef = merge.s || merge.start || {};
          const endRef = merge.e || merge.end || {};
          const startRow = Number.isFinite(startRef.r) ? startRef.r : 0;
          const endRow = Number.isFinite(endRef.r) ? endRef.r : startRow;
          if (startRow >= headerIndex || endRow >= headerIndex) {
            return;
          }
          const startCol = Number.isFinite(startRef.c) ? startRef.c : 0;
          if (startCol >= columnCount) {
            return;
          }
          const rawEndCol = Number.isFinite(endRef.c) ? endRef.c : startCol;
          const endCol = Math.min(columnCount - 1, rawEndCol);
          if (endCol < startCol) {
            return;
          }
          const rowSpan = Math.max(1, (endRow - startRow) + 1);
          const colSpan = Math.max(1, (endCol - startCol) + 1);
          mergeEntries.push({
            row: startRow,
            column: startCol,
            rowSpan,
            colSpan,
          });
        });
        if (normalizedRows.length) {
          headerPreamble = {
            rows: normalizedRows,
            merges: mergeEntries,
          };
        }
      }
      const rows = [];
      for (let i = headerIndex + 1; i < rawRows.length; i += 1) {
        const sourceRow = Array.isArray(rawRows[i]) ? rawRows[i] : [];
        const normalisedRow = columns.map((_, columnIndex) => coerceCellValue(sourceRow[columnIndex]));
        const isBlankRow = normalisedRow.every((value) => value === '');
        if (isBlankRow) {
          continue;
        }
        const hasKey = keyColumnIndexes.some((index) => {
          if (index < 0 || index >= normalisedRow.length) {
            return false;
          }
          const value = normalisedRow[index];
          if (value === null || value === undefined) {
            return false;
          }
          const trimmed = typeof value === 'string' ? value.trim() : String(value).trim();
          return trimmed !== '' && trimmed !== '-' && trimmed !== '--';
        });
        if (!hasKey) {
          continue;
        }
        rows.push(normalisedRow);
      }
      const dataset = { columns, rows };
      if (headerPreamble) {
        dataset.headerPreamble = headerPreamble;
      }
      return dataset;
    }

    function fetchExcelWorkbook(options = {}) {
      const forceReload = Boolean(options && options.forceReload);
      if (!forceReload && workbookLoadPromise) {
        return workbookLoadPromise;
      }
      if (forceReload) {
        workbookLoadPromise = null;
      }
      if (typeof window !== 'object' || typeof window.loadExcelData !== 'function') {
        const error = new Error('Excel loader is not available');
        showDataLoaderError(error.message);
        return Promise.reject(error);
      }
      const loadOptions = { includeWorkbook: true };
      if (forceReload) {
        loadOptions.forceReload = true;
      }
      const workbookPromise = window.loadExcelData(loadOptions)
        .then((payload) => {
          if (!payload || typeof payload !== 'object' || !payload.workbook) {
            throw new Error('Excel workbook is unavailable');
          }
          workbookVersionInfo = payload.version || null;
          if (workbookVersionInfo) {
            updateDataVersionIndicator(workbookVersionInfo);
          } else {
            updateDataVersionIndicator(null);
          }
          clearDataLoaderError();
          return payload.workbook;
        })
        .catch((error) => {
          workbookLoadPromise = null;
          updateDataVersionIndicator(null);
          const errorMessage = error && typeof error.message === 'string' && error.message.trim().length
            ? `Unable to load the latest Excel data. ${error.message.trim()}`
            : 'Unable to load the latest Excel data. Please refresh to try again.';
          showDataLoaderError(errorMessage);
          console.error('Failed to load Excel workbook:', error);
          throw error;
        });
      workbookLoadPromise = workbookPromise;
      return workbookPromise;
    }

    function loadDatasetFromExcel(options = {}) {
      return fetchExcelWorkbook()
        .then((workbook) => {
          const { worksheet } = findWorksheetFromWorkbook(workbook, {
            candidates: options.sheetCandidates,
            fallbackPredicate: options.fallbackPredicate,
          });
          if (!worksheet) {
            throw new Error(options.errorMessage || 'Worksheet not found in workbook');
          }
          return buildDatasetFromWorksheet(worksheet, {
            headerRowIndex: options.headerRowIndex,
            headerSearchValues: options.headerSearchValues,
            keyColumnNames: options.keyColumnNames,
            includeHeaderPreamble: options.includeHeaderPreamble,
          });
        });
    }

    function buildGapDatasetSlice(dataset, startIndex, endIndex, options = {}) {
      if (!dataset || !Array.isArray(dataset.columns) || !Array.isArray(dataset.rows)) {
        return { columns: [], rows: [] };
      }
      const safeStart = Number.isInteger(startIndex) ? Math.max(0, startIndex) : 0;
      const safeEnd = Number.isInteger(endIndex) ? Math.min(dataset.columns.length, endIndex) : dataset.columns.length;
      if (safeStart >= safeEnd) {
        return { columns: [], rows: [] };
      }
      const transformColumnName = typeof options.transformColumnName === 'function'
        ? options.transformColumnName
        : (name) => {
            if (typeof name === 'string') {
              return name.trim();
            }
            return name === null || name === undefined ? '' : String(name);
          };
      const columnIndexes = [];
      const columnNames = [];
      for (let columnIndex = safeStart; columnIndex < safeEnd; columnIndex += 1) {
        const rawName = dataset.columns[columnIndex];
        if (isPlaceholderColumnName(rawName)) {
          continue;
        }
        let transformed = transformColumnName(rawName, columnIndex);
        if (typeof transformed !== 'string') {
          transformed = transformed === null || transformed === undefined ? '' : String(transformed);
        }
        const trimmed = transformed.trim();
        if (!trimmed.length) {
          continue;
        }
        columnIndexes.push(columnIndex);
        columnNames.push(trimmed);
      }
      if (!columnIndexes.length) {
        return { columns: [], rows: [] };
      }
      const rows = [];
      dataset.rows.forEach((sourceRow) => {
        const safeRow = Array.isArray(sourceRow) ? sourceRow : [];
        const row = columnIndexes.map((columnIndex) => (columnIndex < safeRow.length ? safeRow[columnIndex] : ''));
        const labelValue = row.length ? row[0] : '';
        const hasLabel = options.firstColumnRequired !== false ? !isPlaceholderValue(labelValue) : true;
        const hasData = row.some((value, index) => {
          if (index === 0 && options.firstColumnRequired !== false) {
            return !isPlaceholderValue(value);
          }
          return !isPlaceholderValue(value);
        });
        if ((hasLabel || hasData) && hasData) {
          rows.push(row);
        } else if (hasLabel && options.includeEmptyLabelRows) {
          rows.push(row);
        }
      });

      let finalColumns = columnNames;
      let finalRows = rows;

      if (options.pruneEmptyColumns) {
        const keptColumnIndexes = columnNames.reduce((acc, _name, index) => {
          if (index === 0 && options.firstColumnRequired !== false) {
            acc.push(index);
            return acc;
          }
          const hasContent = rows.some((row) => {
            const value = index < row.length ? row[index] : '';
            return !isPlaceholderValue(value);
          });
          if (hasContent) {
            acc.push(index);
          }
          return acc;
        }, []);

        if (keptColumnIndexes.length && keptColumnIndexes.length !== columnNames.length) {
          finalColumns = keptColumnIndexes.map((index) => columnNames[index]);
          finalRows = rows.map((row) => keptColumnIndexes.map((columnIndex) => (columnIndex < row.length ? row[columnIndex] : '')));
        }
      }

      return { columns: finalColumns, rows: finalRows };
    }

    function formatExcelHeaderLabel(rawValue) {
      if (rawValue === null || rawValue === undefined) {
        return null;
      }
      const text = typeof rawValue === 'string' ? rawValue.trim() : String(rawValue);
      if (!text.length) {
        return null;
      }
      if (typeof window.parseDateMetadata === 'function' && typeof window.formatDateLabel === 'function') {
        const metadata = window.parseDateMetadata(rawValue);
        if (metadata) {
          const formatted = window.formatDateLabel(metadata);
          if (formatted) {
            return formatted;
          }
        }
      }
      const numeric = Number(text);
      if (Number.isFinite(numeric) && numeric >= 30000 && numeric <= 60000) {
        const formatted = formatExcelSerialDate(numeric);
        if (formatted) {
          return formatted;
        }
      }
      if (/^\d+$/.test(text)) {
        let candidate = text.slice(0, -1);
        while (candidate.length >= 5) {
          const candidateNumeric = Number(candidate);
          if (Number.isFinite(candidateNumeric) && candidateNumeric >= 30000 && candidateNumeric <= 60000) {
            const formattedCandidate = formatExcelSerialDate(candidateNumeric);
            if (formattedCandidate) {
              return formattedCandidate;
            }
          }
          candidate = candidate.slice(0, -1);
        }
      }
      const fallback = formatExcelSerialDate(text);
      return fallback || null;
    }

    function transformLoGapColumnName(name) {
      if (name === null || name === undefined) {
        return '';
      }
      const value = typeof name === 'string' ? name.trim() : String(name);
      const lower = value.toLowerCase();
      if (lower === 'row labels') {
        return 'Listing owner';
      }
      if (lower === 'sum of gap') {
        return 'LO Sales GAP';
      }
      const formattedDate = formatExcelHeaderLabel(value);
      if (formattedDate) {
        return formattedDate;
      }
      return value;
    }

    function transformSkuGapColumnName(name) {
      if (name === null || name === undefined) {
        return '';
      }
      const value = typeof name === 'string' ? name.trim() : String(name);
      const lower = value.toLowerCase();
      if (lower === 'l.o.') {
        return 'Listing owner';
      }
      if (lower === 'dc list') {
        return 'DC LIST';
      }
      if (lower === 'cat') {
        return 'Category';
      }
      if (lower === 'difference') {
        return 'Difference';
      }
      const formattedDate = formatExcelHeaderLabel(value);
      if (formattedDate) {
        return formattedDate;
      }
      return value;
    }

    function prepareSalesGapDatasets(dataset) {
      if (!dataset || !Array.isArray(dataset.columns)) {
        const empty = { columns: [], rows: [] };
        return { loDataset: empty, skuDataset: empty };
      }
      const rowLabelIndex = dataset.columns.findIndex((name) => {
        if (typeof name !== 'string') {
          return false;
        }
        return name.trim().toLowerCase() === 'row labels';
      });
      const loDataset = rowLabelIndex === -1
        ? { columns: [], rows: [] }
        : buildGapDatasetSlice(dataset, rowLabelIndex, dataset.columns.length, {
            transformColumnName: transformLoGapColumnName,
            pruneEmptyColumns: true,
          });
      const skuDataset = buildGapDatasetSlice(dataset, 2, 9, {
        transformColumnName: transformSkuGapColumnName,
      });
      return { loDataset, skuDataset };
    }

    function loadDashboardPivotSectionsFromExcel() {
      return fetchExcelWorkbook()
        .then((workbook) => {
          const { worksheet } = findWorksheetFromWorkbook(workbook, {
            candidates: EXCEL_MAIN_SHEET_CANDIDATES,
            fallbackPredicate: (name) => normaliseSheetName(name).includes('main'),
          });
          if (!worksheet) {
            throw new Error('Main worksheet not found in workbook');
          }
          const dataset = buildDatasetFromWorksheet(worksheet, {
            headerRowIndex: 3,
            headerSearchValues: ['SKU', 'DC LIST', 'TOTAL SALES'],
            keyColumnNames: EXCEL_MAIN_KEY_COLUMN_NAMES,
            includeHeaderPreamble: true,
          });
          mainDatasetCache = dataset;
          ensureMainFilterSetup(dataset);
          return buildDashboardPivotResultsFromDataset(dataset);
        });
    }

    function fetchDashboardPivotSections() {
      if (mainDashboardPivotCache instanceof Map && mainDashboardInitialised) {
        return Promise.resolve(mainDashboardPivotCache);
      }
      if (dashboardPivotPromise) {
        return dashboardPivotPromise;
      }
      dashboardPivotPromise = loadDashboardPivotSectionsFromExcel()
        .then((results) => {
          dashboardPivotPromise = null;
          return results;
        })
        .catch((error) => {
          dashboardPivotPromise = null;
          throw error;
        });
      return dashboardPivotPromise;
    }

    function normaliseDashboardSummaryDataset(rawRows) {
      if (!Array.isArray(rawRows) || !rawRows.length) {
        return { columns: [], rows: [] };
      }
      const headerRow = Array.isArray(rawRows[0]) ? rawRows[0] : [];
      const columns = headerRow.map((cell) => {
        if (cell === null || cell === undefined) {
          return '';
        }
        return String(cell).trim();
      });
      const rowCount = columns.length;
      const rows = [];
      for (let index = 1; index < rawRows.length; index += 1) {
        const sourceRow = Array.isArray(rawRows[index]) ? rawRows[index] : [];
        const normalised = columns.map((_, columnIndex) => {
          const value = columnIndex < sourceRow.length ? sourceRow[columnIndex] : '';
          if (typeof value === 'string') {
            return value.trim();
          }
          if (value === null || value === undefined) {
            return '';
          }
          return value;
        });
        const hasValue = normalised.some((value) => {
          if (typeof value === 'number') {
            return !Number.isNaN(value);
          }
          if (value === null || value === undefined) {
            return false;
          }
          return String(value).trim().length > 0;
        });
        if (hasValue) {
          rows.push(normalised.slice(0, rowCount));
        }
      }
      return { columns, rows };
    }

    function normaliseDashboardSummaryStoreKey(value) {
      if (typeof value === 'string') {
        return value.trim().toLowerCase();
      }
      if (value === null || value === undefined) {
        return '';
      }
      return String(value).trim().toLowerCase();
    }

    function updateDashboardSummaryStoreFilterSummaryLabel() {
      if (!dashboardSummaryStoreFilterToggleElement) {
        return;
      }
      const selection = dashboardSummaryStoreFilterSelection instanceof Set
        ? dashboardSummaryStoreFilterSelection
        : null;
      let label = 'All stores';
      if (selection instanceof Set) {
        if (selection.size === 0) {
          label = 'No stores selected';
        } else if (selection.size === 1) {
          const [firstKey] = selection;
          const match = dashboardSummaryStoreFilterOptions.find((option) => option.key === firstKey);
          label = match ? match.label : '1 store';
        } else {
          label = `${selection.size} stores`;
        }
      }
      dashboardSummaryStoreFilterToggleElement.textContent = label;
      if (dashboardSummaryStoreFilterContainerElement) {
        dashboardSummaryStoreFilterContainerElement.dataset.active = selection instanceof Set ? 'true' : 'false';
      }
      dashboardSummaryStoreFilterToggleElement.setAttribute('aria-expanded', dashboardSummaryStoreFilterMenuVisible ? 'true' : 'false');
    }

    function updateDashboardSummaryStoreFilterSelectAllState() {
      if (!dashboardSummaryStoreFilterSelectAllElement) {
        return;
      }
      const options = Array.isArray(dashboardSummaryStoreFilterOptions)
        ? dashboardSummaryStoreFilterOptions
        : [];
      const total = options.length;
      if (!total) {
        dashboardSummaryStoreFilterSelectAllElement.checked = false;
        dashboardSummaryStoreFilterSelectAllElement.indeterminate = false;
        dashboardSummaryStoreFilterSelectAllElement.disabled = true;
        return;
      }
      dashboardSummaryStoreFilterSelectAllElement.disabled = false;
      const selection = dashboardSummaryStoreFilterPendingSelection instanceof Set
        ? dashboardSummaryStoreFilterPendingSelection
        : dashboardSummaryStoreFilterSelection instanceof Set
          ? dashboardSummaryStoreFilterSelection
          : null;
      if (!(selection instanceof Set)) {
        dashboardSummaryStoreFilterSelectAllElement.checked = true;
        dashboardSummaryStoreFilterSelectAllElement.indeterminate = false;
        return;
      }
      if (selection.size === 0) {
        dashboardSummaryStoreFilterSelectAllElement.checked = false;
        dashboardSummaryStoreFilterSelectAllElement.indeterminate = false;
        return;
      }
      if (selection.size >= total) {
        dashboardSummaryStoreFilterSelectAllElement.checked = true;
        dashboardSummaryStoreFilterSelectAllElement.indeterminate = false;
        dashboardSummaryStoreFilterPendingSelection = null;
        return;
      }
      dashboardSummaryStoreFilterSelectAllElement.checked = false;
      dashboardSummaryStoreFilterSelectAllElement.indeterminate = true;
    }

    function renderDashboardSummaryStoreFilterOptions() {
      if (!dashboardSummaryStoreFilterOptionsElement) {
        return;
      }
      const options = Array.isArray(dashboardSummaryStoreFilterOptions)
        ? dashboardSummaryStoreFilterOptions
        : [];
      const keySet = new Set(options.map((option) => option.key));

      if (dashboardSummaryStoreFilterPendingSelection instanceof Set) {
        const sanitizedPending = new Set();
        dashboardSummaryStoreFilterPendingSelection.forEach((key) => {
          if (keySet.has(key)) {
            sanitizedPending.add(key);
          }
        });
        if (sanitizedPending.size >= keySet.size && keySet.size > 0) {
          dashboardSummaryStoreFilterPendingSelection = null;
        } else {
          dashboardSummaryStoreFilterPendingSelection = sanitizedPending;
        }
      } else if (dashboardSummaryStoreFilterPendingSelection !== null && !(dashboardSummaryStoreFilterPendingSelection instanceof Set)) {
        dashboardSummaryStoreFilterPendingSelection = null;
      }

      const selection = dashboardSummaryStoreFilterPendingSelection instanceof Set
        ? dashboardSummaryStoreFilterPendingSelection
        : dashboardSummaryStoreFilterSelection instanceof Set
          ? dashboardSummaryStoreFilterSelection
          : null;

      if (dashboardSummaryStoreFilterApplyElement) {
        if (options.length) {
          dashboardSummaryStoreFilterApplyElement.removeAttribute('disabled');
        } else {
          dashboardSummaryStoreFilterApplyElement.setAttribute('disabled', 'true');
        }
      }
      if (dashboardSummaryStoreFilterResetElement) {
        if (options.length) {
          dashboardSummaryStoreFilterResetElement.removeAttribute('disabled');
        } else {
          dashboardSummaryStoreFilterResetElement.setAttribute('disabled', 'true');
        }
      }

      if (!options.length) {
        dashboardSummaryStoreFilterOptionsElement.innerHTML = '';
        if (dashboardSummaryStoreFilterMenuEmptyElement) {
          dashboardSummaryStoreFilterMenuEmptyElement.hidden = false;
        }
        updateDashboardSummaryStoreFilterSelectAllState();
        return;
      }

      if (dashboardSummaryStoreFilterMenuEmptyElement) {
        dashboardSummaryStoreFilterMenuEmptyElement.hidden = true;
      }

      const optionMarkup = options
        .map((option, index) => {
          const optionId = `dashboard-summary-store-filter-option-${index}`;
          const isChecked = !(selection instanceof Set) || selection.has(option.key);
          const safeValue = escapeHtml(option.key);
          const safeLabel = escapeHtml(option.label);
          return `<label class="dashboard-card__quick-filter-option" for="${optionId}"><input type="checkbox" id="${optionId}" value="${safeValue}" ${isChecked ? 'checked' : ''}><span>${safeLabel}</span></label>`;
        })
        .join('');
      dashboardSummaryStoreFilterOptionsElement.innerHTML = optionMarkup;
      animateOptionList(dashboardSummaryStoreFilterOptionsElement, '.dashboard-card__quick-filter-option');
      updateDashboardSummaryStoreFilterSelectAllState();
    }

    function updateDashboardSummaryStoreFilterOptions(rows) {
      const storeMap = new Map();
      if (Array.isArray(rows)) {
        rows.forEach((row) => {
          if (!Array.isArray(row) || !row.length) {
            return;
          }
          const rawLabel = row[0];
          const label = typeof rawLabel === 'string'
            ? rawLabel.trim()
            : String(rawLabel ?? '').trim();
          const key = normaliseDashboardSummaryStoreKey(label);
          if (!label || !key || key === TOTAL_ROW_LABEL_NORMALISED) {
            return;
          }
          if (!storeMap.has(key)) {
            storeMap.set(key, label);
          }
        });
      }

      dashboardSummaryStoreFilterOptions = Array.from(storeMap.entries())
        .map(([key, label]) => ({ key, label }))
        .sort((a, b) => a.label.localeCompare(b.label, undefined, { sensitivity: 'base' }));

      const availableKeys = new Set(dashboardSummaryStoreFilterOptions.map((option) => option.key));
      if (dashboardSummaryStoreFilterSelection instanceof Set) {
        const sanitizedSelection = new Set();
        dashboardSummaryStoreFilterSelection.forEach((key) => {
          if (availableKeys.has(key)) {
            sanitizedSelection.add(key);
          }
        });
        if (sanitizedSelection.size >= availableKeys.size && availableKeys.size > 0) {
          dashboardSummaryStoreFilterSelection = null;
        } else {
          dashboardSummaryStoreFilterSelection = sanitizedSelection;
        }
      }

      if (dashboardSummaryStoreFilterPendingSelection instanceof Set) {
        const sanitizedPending = new Set();
        dashboardSummaryStoreFilterPendingSelection.forEach((key) => {
          if (availableKeys.has(key)) {
            sanitizedPending.add(key);
          }
        });
        if (sanitizedPending.size === 0) {
          dashboardSummaryStoreFilterPendingSelection = new Set();
        } else if (sanitizedPending.size >= availableKeys.size && availableKeys.size > 0) {
          dashboardSummaryStoreFilterPendingSelection = null;
        } else {
          dashboardSummaryStoreFilterPendingSelection = sanitizedPending;
        }
      }

      const hasOptions = dashboardSummaryStoreFilterOptions.length > 0;
      if (dashboardSummaryStoreFilterToggleElement) {
        if (hasOptions) {
          dashboardSummaryStoreFilterToggleElement.removeAttribute('disabled');
        } else {
          dashboardSummaryStoreFilterToggleElement.setAttribute('disabled', 'true');
        }
      }

      if (!hasOptions) {
        dashboardSummaryStoreFilterSelection = null;
      }

      if (!hasOptions && dashboardSummaryStoreFilterMenuVisible) {
        closeDashboardSummaryStoreFilterMenu();
      }

      renderDashboardSummaryStoreFilterOptions();
      updateDashboardSummaryStoreFilterSummaryLabel();

      if (dashboardSummaryStoreFilterEmptyElement) {
        dashboardSummaryStoreFilterEmptyElement.hidden = hasOptions;
      }
    }

    function handleDashboardSummaryStoreFilterOptionToggle(optionKey, isChecked) {
      const options = Array.isArray(dashboardSummaryStoreFilterOptions)
        ? dashboardSummaryStoreFilterOptions
        : [];
      if (!options.length) {
        return;
      }
      const keySet = new Set(options.map((option) => option.key));
      if (!keySet.has(optionKey)) {
        return;
      }
      const allKeys = Array.from(keySet);
      const baseSelection = dashboardSummaryStoreFilterPendingSelection instanceof Set
        ? new Set(dashboardSummaryStoreFilterPendingSelection)
        : dashboardSummaryStoreFilterSelection instanceof Set
          ? new Set(dashboardSummaryStoreFilterSelection)
          : new Set(allKeys);
      if (isChecked) {
        baseSelection.add(optionKey);
      } else {
        baseSelection.delete(optionKey);
      }
      if (baseSelection.size >= allKeys.length) {
        dashboardSummaryStoreFilterPendingSelection = null;
      } else {
        dashboardSummaryStoreFilterPendingSelection = baseSelection;
      }
      renderDashboardSummaryStoreFilterOptions();
    }

    function applyDashboardSummaryStoreFilterSelection() {
      const options = Array.isArray(dashboardSummaryStoreFilterOptions)
        ? dashboardSummaryStoreFilterOptions
        : [];
      const keySet = new Set(options.map((option) => option.key));
      let nextSelection = null;
      if (dashboardSummaryStoreFilterPendingSelection instanceof Set) {
        const sanitizedSelection = new Set();
        dashboardSummaryStoreFilterPendingSelection.forEach((key) => {
          if (keySet.has(key)) {
            sanitizedSelection.add(key);
          }
        });
        if (sanitizedSelection.size === 0) {
          nextSelection = new Set();
        } else if (sanitizedSelection.size >= keySet.size && keySet.size > 0) {
          nextSelection = null;
        } else {
          nextSelection = sanitizedSelection;
        }
      } else if (dashboardSummaryStoreFilterPendingSelection === null) {
        nextSelection = null;
      }

      if (nextSelection === null) {
        dashboardSummaryStoreFilterSelection = null;
      } else {
        dashboardSummaryStoreFilterSelection = nextSelection;
      }

      dashboardSummaryStoreFilterPendingSelection = null;
      updateDashboardSummaryStoreFilterSummaryLabel();
      renderDashboardSummaryStoreFilterOptions();
      updateDashboardSummaryTable();
      closeDashboardSummaryStoreFilterMenu();
    }

    function handleDashboardSummaryStoreFilterDocumentClick(event) {
      if (!dashboardSummaryStoreFilterMenuVisible) {
        return;
      }
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (dashboardSummaryStoreFilterGroupElement && dashboardSummaryStoreFilterGroupElement.contains(target)) {
        return;
      }
      closeDashboardSummaryStoreFilterMenu();
    }

    function handleDashboardSummaryStoreFilterKeydown(event) {
      if (!dashboardSummaryStoreFilterMenuVisible) {
        return;
      }
      if (event.key === 'Escape') {
        event.preventDefault();
        closeDashboardSummaryStoreFilterMenu(true);
      }
    }

    function registerDashboardSummaryStoreFilterDismissListeners() {
      if (dashboardSummaryStoreFilterMenuDocumentHandlerRegistered) {
        return;
      }
      document.addEventListener('click', handleDashboardSummaryStoreFilterDocumentClick, true);
      document.addEventListener('keydown', handleDashboardSummaryStoreFilterKeydown, true);
      dashboardSummaryStoreFilterMenuDocumentHandlerRegistered = true;
    }

    function unregisterDashboardSummaryStoreFilterDismissListeners() {
      if (!dashboardSummaryStoreFilterMenuDocumentHandlerRegistered) {
        return;
      }
      document.removeEventListener('click', handleDashboardSummaryStoreFilterDocumentClick, true);
      document.removeEventListener('keydown', handleDashboardSummaryStoreFilterKeydown, true);
      dashboardSummaryStoreFilterMenuDocumentHandlerRegistered = false;
    }

    function openDashboardSummaryStoreFilterMenu() {
      if (!dashboardSummaryStoreFilterMenuElement || dashboardSummaryStoreFilterMenuVisible) {
        return;
      }
      if (!Array.isArray(dashboardSummaryStoreFilterOptions) || !dashboardSummaryStoreFilterOptions.length) {
        return;
      }
      dashboardSummaryStoreFilterPendingSelection = dashboardSummaryStoreFilterSelection instanceof Set
        ? new Set(dashboardSummaryStoreFilterSelection)
        : null;
      renderDashboardSummaryStoreFilterOptions();
      dashboardSummaryStoreFilterMenuElement.hidden = false;
      dashboardSummaryStoreFilterMenuVisible = true;
      updateDashboardSummaryStoreFilterSummaryLabel();
      registerDashboardSummaryStoreFilterDismissListeners();
      setTimeout(() => {
        if (!dashboardSummaryStoreFilterMenuVisible) {
          return;
        }
        const firstCheckbox = dashboardSummaryStoreFilterOptionsElement
          ? dashboardSummaryStoreFilterOptionsElement.querySelector('input[type="checkbox"]')
          : null;
        if (firstCheckbox instanceof HTMLElement) {
          firstCheckbox.focus();
        } else if (dashboardSummaryStoreFilterSelectAllElement) {
          dashboardSummaryStoreFilterSelectAllElement.focus();
        }
      }, 0);
    }

    function closeDashboardSummaryStoreFilterMenu(focusToggle = false) {
      if (!dashboardSummaryStoreFilterMenuElement || dashboardSummaryStoreFilterMenuElement.hidden) {
        return;
      }
      dashboardSummaryStoreFilterMenuElement.hidden = true;
      dashboardSummaryStoreFilterMenuVisible = false;
      dashboardSummaryStoreFilterPendingSelection = null;
      unregisterDashboardSummaryStoreFilterDismissListeners();
      updateDashboardSummaryStoreFilterSummaryLabel();
      if (focusToggle && dashboardSummaryStoreFilterToggleElement) {
        dashboardSummaryStoreFilterToggleElement.focus();
      }
    }

    function filterDashboardSummaryRowsByStore(rows) {
      if (!Array.isArray(rows)) {
        return [];
      }
      if (!(dashboardSummaryStoreFilterSelection instanceof Set)) {
        return rows;
      }
      if (dashboardSummaryStoreFilterSelection.size === 0) {
        return [];
      }
      return rows.filter((row) => {
        if (!Array.isArray(row) || !row.length) {
          return false;
        }
        const key = normaliseDashboardSummaryStoreKey(row[0]);
        return dashboardSummaryStoreFilterSelection.has(key);
      });
    }

    function buildDashboardSummaryTotalRow(rows, columns, fallbackTotalRow) {
      if (!Array.isArray(rows) || !rows.length || !Array.isArray(columns) || !columns.length) {
        return null;
      }
      const totalLength = columns.length;
      const totals = new Array(totalLength);
      for (let columnIndex = 0; columnIndex < totalLength; columnIndex += 1) {
        if (columnIndex === 0) {
          const fallbackLabel = Array.isArray(fallbackTotalRow) && fallbackTotalRow.length
            ? fallbackTotalRow[0]
            : 'Grand Total';
          totals[columnIndex] = fallbackLabel || 'Grand Total';
          continue;
        }
        let sum = 0;
        let hasNumeric = false;
        rows.forEach((row) => {
          const value = columnIndex < row.length ? row[columnIndex] : null;
          if (typeof value === 'number' && !Number.isNaN(value)) {
            sum += value;
            hasNumeric = true;
          }
        });
        if (hasNumeric) {
          totals[columnIndex] = sum;
        } else if (Array.isArray(fallbackTotalRow) && columnIndex < fallbackTotalRow.length) {
          totals[columnIndex] = fallbackTotalRow[columnIndex];
        } else {
          totals[columnIndex] = '';
        }
      }
      return totals;
    }

    function fetchDashboardSummaryTable() {
      if (dashboardSummaryTableCache) {
        return Promise.resolve(dashboardSummaryTableCache);
      }
      if (dashboardSummaryTablePromise) {
        return dashboardSummaryTablePromise;
      }
      dashboardSummaryTablePromise = fetchExcelWorkbook()
        .then((workbook) => {
          const { worksheet } = findWorksheetFromWorkbook(workbook, {
            candidates: EXCEL_DASHBOARD_SHEET_CANDIDATES,
            fallbackPredicate: (name) => normaliseSheetName(name).includes('dashboard'),
          });
          if (!worksheet) {
            throw new Error('Dashboard worksheet not found in workbook');
          }
          const rawRows = XLSX.utils.sheet_to_json(worksheet, {
            header: 1,
            range: DASHBOARD_SUMMARY_RANGE,
            blankrows: false,
            defval: '',
            raw: true,
          });
          const dataset = normaliseDashboardSummaryDataset(rawRows);
          dashboardSummaryTableCache = dataset;
          return dataset;
        })
        .finally(() => {
          dashboardSummaryTablePromise = null;
        });
      return dashboardSummaryTablePromise;
    }

    function formatDashboardSummaryValue(value, columnName) {
      if (value === null || value === undefined || value === '') {
        return '';
      }
      if (typeof value === 'string') {
        return value;
      }
      if (typeof value !== 'number' || Number.isNaN(value)) {
        return String(value);
      }
      const label = typeof columnName === 'string' ? columnName.trim().toLowerCase() : '';
      if (DASHBOARD_SUMMARY_PERCENT_PATTERN.test(label)) {
        return percentFormatter.format(value);
      }
      if (DASHBOARD_SUMMARY_INTEGER_KEYWORDS.some((keyword) => label.includes(keyword))) {
        return integerFormatter.format(value);
      }
      if (DASHBOARD_SUMMARY_CURRENCY_KEYWORDS.some((keyword) => label.includes(keyword))) {
        return currencyFormatter.format(value);
      }
      return numberFormatter.format(value);
    }

    function renderDashboardSummaryTable(dataset) {
      const tableElement = document.getElementById('dashboard-table-summary');
      if (!tableElement) {
        return;
      }
      const columns = Array.isArray(dataset?.columns) ? dataset.columns : [];
      const rows = Array.isArray(dataset?.rows) ? dataset.rows : [];
      if (!columns.length) {
        updateDashboardSummaryStoreFilterOptions([]);
        setDashboardTableMessage('dashboard-table-summary', 'No data available', 3);
        return;
      }

      const dataRows = [];
      let totalRow = null;
      rows.forEach((row) => {
        const label = row && row.length ? row[0] : '';
        const isGrandTotal = typeof label === 'string'
          && label.trim().toLowerCase() === TOTAL_ROW_LABEL_NORMALISED;
        if (isGrandTotal) {
          totalRow = row;
        } else {
          dataRows.push(row);
        }
      });

      if (!dataRows.length && !totalRow) {
        updateDashboardSummaryStoreFilterOptions([]);
        setDashboardTableMessage('dashboard-table-summary', 'No data available', Math.max(2, columns.length));
        return;
      }

      updateDashboardSummaryStoreFilterOptions(dataRows);
      const filteredRows = filterDashboardSummaryRowsByStore(dataRows);
      const storeFilterActive = dashboardSummaryStoreFilterSelection instanceof Set;

      tableElement.innerHTML = '';
      const thead = tableElement.createTHead();
      const headerRow = thead.insertRow();
      columns.forEach((column) => {
        const th = document.createElement('th');
        th.textContent = column || '';
        headerRow.appendChild(th);
      });

      const tbody = tableElement.createTBody();
      if (!filteredRows.length) {
        const tr = tbody.insertRow();
        const messageCell = tr.insertCell();
        messageCell.colSpan = columns.length || 1;
        messageCell.className = 'dashboard-table__message';
        messageCell.textContent = storeFilterActive
          ? 'No rows match the selected store'
          : 'No data available';
      } else {
        filteredRows.forEach((row) => {
          const tr = tbody.insertRow();
          columns.forEach((column, columnIndex) => {
            const cellTag = columnIndex === 0 ? 'th' : 'td';
            const cell = document.createElement(cellTag);
            if (columnIndex === 0) {
              cell.scope = 'row';
            }
            const value = columnIndex < row.length ? row[columnIndex] : '';
            const formatted = formatDashboardSummaryValue(value, column);
            cell.textContent = formatted;
            if (columnIndex !== 0 && typeof value === 'number' && value < 0) {
              cell.classList.add('dashboard-table__value--error');
            }
            tr.appendChild(cell);
          });
        });
      }

      const shouldRenderTotal = totalRow
        && totalRow.length
        && (!storeFilterActive || filteredRows.length);
      if (shouldRenderTotal) {
        const computedTotalRow = storeFilterActive && filteredRows.length
          ? buildDashboardSummaryTotalRow(filteredRows, columns, totalRow)
          : totalRow;
        const finalTotalRow = Array.isArray(computedTotalRow) ? computedTotalRow : totalRow;
        const tfoot = tableElement.createTFoot();
        const tr = tfoot.insertRow();
        tr.className = 'dashboard-table__total';
        columns.forEach((column, columnIndex) => {
          const cellTag = columnIndex === 0 ? 'th' : 'td';
          const cell = document.createElement(cellTag);
          if (columnIndex === 0) {
            cell.scope = 'row';
          }
          const value = columnIndex < finalTotalRow.length ? finalTotalRow[columnIndex] : '';
          const formatted = formatDashboardSummaryValue(value, column);
          cell.textContent = formatted;
          if (columnIndex !== 0 && typeof value === 'number' && value < 0) {
            cell.classList.add('dashboard-table__value--error');
          }
          tr.appendChild(cell);
        });
      }
    }

    function updateDashboardSummaryTable() {
      if (dashboardSummaryTableCache) {
        renderDashboardSummaryTable(dashboardSummaryTableCache);
        dashboardSummaryTableInitialised = true;
        return;
      }
      if (dashboardSummaryTableInitialised && !dashboardSummaryTablePromise) {
        setDashboardTableMessage('dashboard-table-summary', 'No data available', 3);
        updateDashboardSummaryStoreFilterOptions([]);
        return;
      }
      if (!dashboardSummaryTablePromise) {
        setDashboardTableMessage('dashboard-table-summary', 'Loading data…', 3);
      }
      fetchDashboardSummaryTable()
        .then((dataset) => {
          renderDashboardSummaryTable(dataset);
          dashboardSummaryTableInitialised = true;
        })
        .catch((error) => {
          console.error('Failed to load dashboard summary table:', error);
          setDashboardTableMessage('dashboard-table-summary', 'Unable to load summary data', 3);
          updateDashboardSummaryStoreFilterOptions([]);
        });
    }

    function selectRegularSheetName(sheetNames) {
      if (!Array.isArray(sheetNames) || !sheetNames.length) {
        return null;
      }
      const matches = sheetNames
        .map((name, index) => ({
          name,
          index,
          label: name === null || name === undefined ? '' : String(name).trim(),
        }))
        .filter((entry) => /regular/i.test(entry.label));
      if (!matches.length) {
        return null;
      }
      const scored = matches.map((entry) => {
        const upper = entry.label.toUpperCase();
        let score = 2;
        if (upper === 'REGULAR') {
          score = 0;
        } else if (upper.startsWith('REGULAR')) {
          score = 1;
        }
        return { ...entry, score };
      });
      scored.sort((a, b) => {
        if (a.score !== b.score) {
          return a.score - b.score;
        }
        return b.index - a.index;
      });
      const selected = scored[0];
      if (scored.length > 1) {
        const candidates = scored.map((entry) => entry.name).join(', ');
        console.log('[Regular] multiple REGULAR sheet matches:', candidates, '→ using', selected.name);
      }
      return selected.name;
    }

    function buildRegularDatasetFromWorkbook(workbook) {
      if (!workbook || typeof workbook !== 'object') {
        throw new Error('Regular: Workbook is unavailable');
      }

      const sheetNames = Array.isArray(workbook.SheetNames) ? workbook.SheetNames : [];
      const targetSheetName = selectRegularSheetName(sheetNames);
      if (!targetSheetName) {
        console.warn('[Regular] No worksheet containing "REGULAR" was found. Available sheets:', sheetNames);
        const error = new Error('No worksheet containing "REGULAR" was found in the workbook.');
        error.availableSheets = sheetNames;
        throw error;
      }

      const worksheet = workbook.Sheets[targetSheetName];
      if (!worksheet) {
        const error = new Error(`Regular worksheet "${targetSheetName}" is missing in workbook`);
        error.availableSheets = sheetNames;
        throw error;
      }

      console.log('[Regular] available sheets:', sheetNames);
      console.log('[Regular] selected sheet:', targetSheetName);

      const matrix = XLSX.utils.sheet_to_json(worksheet, {
        header: 1,
        raw: true,
        defval: null,
        blankrows: true,
      });

      if (!Array.isArray(matrix) || matrix.length < 2) {
        console.error('[Regular] REGULAR sheet: Expected header on row 2. Found rows:', Array.isArray(matrix) ? matrix.length : 0);
        const error = new Error('Regular sheet is missing header row 2.');
        error.sheetName = targetSheetName;
        throw error;
      }

      const headerRowIndex = 1;
      const dataRowStartIndex = 2;

      const rawHeaderRow = Array.isArray(matrix[headerRowIndex]) ? matrix[headerRowIndex] : [];
      const headerLog = rawHeaderRow.map((value) => {
        if (value === null || value === undefined) {
          return '';
        }
        return String(value).trim();
      });

      const columnCount = matrix.slice(headerRowIndex).reduce((max, row) => {
        if (!Array.isArray(row)) {
          return max;
        }
        return Math.max(max, row.length);
      }, rawHeaderRow.length);

      const header = new Array(columnCount).fill('').map((_, index) => {
        const rawValue = index < headerLog.length ? headerLog[index] : '';
        return rawValue && rawValue.length ? rawValue : `Column ${index + 1}`;
      });

      const dataRows = [];
      for (let rowIndex = dataRowStartIndex; rowIndex < matrix.length; rowIndex += 1) {
        const sourceRow = Array.isArray(matrix[rowIndex]) ? matrix[rowIndex] : [];
        const normalisedRow = [];
        for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
          normalisedRow.push(columnIndex < sourceRow.length ? sourceRow[columnIndex] : null);
        }
        const hasValue = normalisedRow.some((value) => {
          if (value === null || value === undefined) {
            return false;
          }
          if (typeof value === 'string') {
            return value.trim().length > 0;
          }
          return true;
        });
        if (hasValue) {
          dataRows.push(normalisedRow);
        }
      }

      regularSheetHeader = header.slice();
      console.log('[Regular] sheet:', targetSheetName);
      console.log('[Regular] header:', headerLog);
      console.log('[Regular] rows:', dataRows.length);

      if (matrix.length < 3) {
        const error = new Error('Regular sheet does not contain data rows starting from row 3.');
        error.sheetName = targetSheetName;
        throw error;
      }

      return {
        sheetName: targetSheetName,
        columns: header,
        rows: dataRows,
      };
    }

    function fetchRegularDataset(options = {}) {
      const forceReload = Boolean(options && options.forceReload);
      if (!forceReload && regularDatasetCache) {
        return Promise.resolve(regularDatasetCache);
      }
      if (regularDatasetPromise) {
        return regularDatasetPromise;
      }

      if (forceReload) {
        regularDatasetCache = null;
      }

      // Reuse the shared workbook loader so the Regular tab sees the same
      // data snapshot as the Main tab, including any forced refreshes.
      regularDatasetPromise = fetchExcelWorkbook({ forceReload })
        .then((workbook) => {
          if (!workbook || typeof workbook !== 'object') {
            throw new Error('Regular: Workbook payload is invalid.');
          }

          const workbookUrl = workbookVersionInfo && typeof workbookVersionInfo.finalUrl === 'string' && workbookVersionInfo.finalUrl.length
            ? workbookVersionInfo.finalUrl
            : (typeof WORKBOOK_URL === 'string' ? `${WORKBOOK_URL}` : '');
          console.log('[Regular] using workbook:', workbookUrl, forceReload ? '(force reload)' : '(cached)');

          const dataset = buildRegularDatasetFromWorkbook(workbook);
          regularDatasetCache = { columns: dataset.columns, rows: dataset.rows };
          return regularDatasetCache;
        })
        .catch((error) => {
          console.error('Failed to load dataset from Excel:', error);
          regularDatasetCache = null;
          throw error;
        })
        .finally(() => {
          regularDatasetPromise = null;
        });
      return regularDatasetPromise;
    }

    function fetchMainDataset() {
      if (mainDatasetCache) {
        return Promise.resolve(mainDatasetCache);
      }
      if (mainDatasetPromise) {
        return mainDatasetPromise;
      }
      mainDatasetPromise = loadDatasetFromExcel({
        sheetCandidates: EXCEL_MAIN_SHEET_CANDIDATES,
        fallbackPredicate: (name) => {
          const normalised = normaliseSheetName(name);
          return normalised.includes('main');
        },
        errorMessage: 'Main worksheet not found in workbook',
        headerRowIndex: 3,
        headerSearchValues: ['SKU', 'DC LIST', 'TOTAL SALES'],
        keyColumnNames: EXCEL_MAIN_KEY_COLUMN_NAMES,
        includeHeaderPreamble: true,
      })
        .then((data) => {
          mainDatasetCache = data;
          return data;
        })
        .catch((error) => {
          mainDatasetPromise = null;
          throw error;
        });
      return mainDatasetPromise;
    }

    function fetchSalesGapDataset() {
      if (salesGapDatasetCache) {
        return Promise.resolve(salesGapDatasetCache);
      }
      if (salesGapDatasetPromise) {
        return salesGapDatasetPromise;
      }
      salesGapDatasetPromise = loadDatasetFromExcel({
        sheetCandidates: EXCEL_SALES_GAP_SHEET_CANDIDATES,
        fallbackPredicate: (name) => {
          const normalised = normaliseSheetName(name);
          return normalised.includes('skuwisegap')
            || normalised.includes('lowisegap')
            || normalised.includes('salesgap');
        },
        errorMessage: 'Sales GAP worksheet not found in workbook',
        headerRowIndex: 2,
        headerSearchValues: EXCEL_SALES_GAP_HEADER_SEARCH_VALUES,
        keyColumnNames: EXCEL_SALES_GAP_KEY_COLUMN_NAMES,
        includeHeaderPreamble: false,
      })
        .then((dataset) => {
          const prepared = prepareSalesGapDatasets(dataset);
          salesGapDatasetCache = prepared;
          return prepared;
        })
        .catch((error) => {
          salesGapDatasetPromise = null;
          throw error;
        });
      return salesGapDatasetPromise;
    }

    function fetchSpendPivot() {
      if (spendPivotCache) {
        return Promise.resolve(spendPivotCache);
      }
      if (spendPivotPromise) {
        return spendPivotPromise;
      }
      const datasetSource = regularDatasetCache ? Promise.resolve(regularDatasetCache) : fetchRegularDataset();
      spendPivotPromise = datasetSource
        .then((dataset) => {
          const options = {};
          if (Array.isArray(loSalesOrderCache) && loSalesOrderCache.length) {
            options.normalizedOrder = loSalesOrderCache;
          }
          if (loDisplayNameOverridesCache instanceof Map && loDisplayNameOverridesCache.size) {
            options.displayNameOverrides = loDisplayNameOverridesCache;
          }
          const pivot = buildLoPivot(dataset, 'Ad Spend', options);
          spendPivotCache = pivot;
          return pivot;
        })
        .catch((error) => {
          spendPivotPromise = null;
          throw error;
        });
      return spendPivotPromise;
    }

    function fetchSkuSummaryPivot() {
      if (skuSummaryPivotCache) {
        return Promise.resolve(skuSummaryPivotCache);
      }
      if (skuSummaryPivotPromise) {
        return skuSummaryPivotPromise;
      }
      const datasetSource = regularDatasetCache
        ? Promise.resolve(regularDatasetCache)
        : fetchRegularDataset();
      skuSummaryPivotPromise = datasetSource
        .then((dataset) => {
          if (!dataset) {
            throw new Error('Dataset is unavailable');
          }
          const pivot = buildSkuSummaryPivotFromDataset(dataset);
          if (!pivot || !Array.isArray(pivot.columns) || !pivot.columns.length) {
            throw new Error('Unable to build SKU summary pivot from dataset');
          }
          skuSummaryPivotCache = pivot;
          return pivot;
        })
        .catch((error) => {
          skuSummaryPivotPromise = null;
          throw error;
        });
      return skuSummaryPivotPromise;
    }

    function showNewProductStatus(message, state = 'info') {
      if (!newProductStatusElement) {
        return;
      }
      if (!message) {
        newProductStatusElement.textContent = '';
        newProductStatusElement.hidden = true;
        newProductStatusElement.removeAttribute('data-state');
        return;
      }
      newProductStatusElement.textContent = message;
      newProductStatusElement.hidden = false;
      if (state && state !== 'info') {
        newProductStatusElement.dataset.state = state;
      } else {
        newProductStatusElement.removeAttribute('data-state');
      }
    }

    function buildDatasetColumnLookup(columns) {
      const lookup = new Map();
      if (!Array.isArray(columns)) {
        return lookup;
      }
      columns.forEach((column, index) => {
        if (typeof column !== 'string') {
          return;
        }
        const key = column.trim().toLowerCase();
        if (key && !lookup.has(key)) {
          lookup.set(key, index);
        }
      });
      return lookup;
    }

    function buildNewProductPivotFromDataset(dataset, definition) {
      if (!dataset || !Array.isArray(dataset.columns) || !Array.isArray(dataset.rows)) {
        return null;
      }
      if (!definition || !definition.groupColumn || !Array.isArray(definition.columns)) {
        return null;
      }
      const columnLookup = buildDatasetColumnLookup(dataset.columns);
      const normalise = (value) => (typeof value === 'string' ? value.trim().toLowerCase() : '');
      const groupIndex = columnLookup.get(normalise(definition.groupColumn));
      if (groupIndex === undefined) {
        return null;
      }
      const metricDescriptors = [];
      const headers = [];
      definition.columns.forEach((column) => {
        if (!column || !column.source || !column.header) {
          return;
        }
        const datasetIndex = columnLookup.get(normalise(column.source));
        if (datasetIndex === undefined) {
          return;
        }
        metricDescriptors.push({ datasetIndex, header: column.header });
        headers.push(column.header);
      });
      if (!metricDescriptors.length) {
        return null;
      }
      const newOldIndex = columnLookup.get(normalise('NEW/OLD'));
      const groups = [];
      const groupLookup = new Map();
      const normalizedTotal = TOTAL_ROW_LABEL.toLowerCase();

      dataset.rows.forEach((row) => {
        if (!Array.isArray(row)) {
          return;
        }
        const rawLabel = groupIndex < row.length ? row[groupIndex] : null;
        if (isPlaceholderValue(rawLabel)) {
          return;
        }
        const label = typeof rawLabel === 'string'
          ? rawLabel.trim()
          : (rawLabel === null || rawLabel === undefined ? '' : String(rawLabel).trim());
        if (!label || label.toLowerCase() === normalizedTotal) {
          return;
        }
        const normalizedLabel = normaliseNewProductLabel(label);
        if (!normalizedLabel) {
          return;
        }
        let entry = groupLookup.get(normalizedLabel);
        if (!entry) {
          entry = {
            label,
            normalizedLabel,
            totals: new Array(metricDescriptors.length).fill(0),
            hasValue: new Array(metricDescriptors.length).fill(false),
            newOldValues: new Set(),
            newOldBreakdown: new Map(),
          };
          groupLookup.set(normalizedLabel, entry);
          groups.push(entry);
        }
        let newOldEntry = null;
        if (newOldIndex !== undefined && newOldIndex < row.length) {
          const rawNewOld = row[newOldIndex];
          const trimmed = typeof rawNewOld === 'string'
            ? rawNewOld.trim()
            : (rawNewOld === null || rawNewOld === undefined ? '' : String(rawNewOld).trim());
          const normalizedNewOld = normaliseNewProductStatus(trimmed);
          if (normalizedNewOld) {
            entry.newOldValues.add(trimmed);
            if (!entry.newOldBreakdown.has(normalizedNewOld)) {
              entry.newOldBreakdown.set(normalizedNewOld, {
                label: trimmed,
                normalizedLabel: normalizedNewOld,
                totals: new Array(metricDescriptors.length).fill(0),
                hasValue: new Array(metricDescriptors.length).fill(false),
              });
            }
            newOldEntry = entry.newOldBreakdown.get(normalizedNewOld);
          }
        }
        metricDescriptors.forEach((descriptor, descriptorIndex) => {
          if (!descriptor) {
            return;
          }
          const value = descriptor.datasetIndex < row.length ? row[descriptor.datasetIndex] : null;
          if (isPlaceholderValue(value)) {
            return;
          }
          const numericValue = parseNumericValue(value);
          if (numericValue !== null) {
            entry.totals[descriptorIndex] += numericValue;
            entry.hasValue[descriptorIndex] = true;
            if (newOldEntry) {
              newOldEntry.totals[descriptorIndex] += numericValue;
              newOldEntry.hasValue[descriptorIndex] = true;
            }
          }
        });
      });

      const rows = [];
      const rowAttributes = [];
      groups.forEach((entry) => {
        if (!entry.hasValue.some((value) => value)) {
          return;
        }
        const values = [entry.label];
        entry.totals.forEach((total, index) => {
          values.push(entry.hasValue[index] ? total : '');
        });
        rows.push(values);
        rowAttributes.push({
          label: entry.label,
          normalizedLabel: entry.normalizedLabel,
          newOld: new Set(entry.newOldValues),
          totals: entry.totals.slice(),
          hasValue: entry.hasValue.slice(),
          newOldBreakdown: (() => {
            const breakdown = new Map();
            entry.newOldBreakdown.forEach((value, key) => {
              breakdown.set(key, {
                label: value.label,
                normalizedLabel: value.normalizedLabel,
                totals: value.totals.slice(),
                hasValue: value.hasValue.slice(),
              });
            });
            return breakdown;
          })(),
        });
      });

      if (!rows.length) {
        return null;
      }

      return {
        columns: ['Row Labels', ...headers],
        rows,
        metadata: definition.metadata || null,
        rowAttributes,
      };
    }

    function buildNewProductPivotSectionsFromDataset(dataset) {
      return NEW_PRODUCT_DATASET_PIVOT_DEFINITIONS.map((definition) => buildNewProductPivotFromDataset(dataset, definition));
    }

    function cloneNewProductRowAttributes(attributes) {
      if (!Array.isArray(attributes)) {
        return [];
      }
      return attributes.map((attribute) => {
        if (!attribute || typeof attribute !== 'object') {
          return {
            label: '',
            normalizedLabel: '',
            newOld: new Set(),
            totals: [],
            hasValue: [],
            newOldBreakdown: new Map(),
          };
        }
        const breakdown = new Map();
        if (attribute.newOldBreakdown instanceof Map) {
          attribute.newOldBreakdown.forEach((value, key) => {
            if (!value || typeof value !== 'object') {
              return;
            }
            const totals = Array.isArray(value.totals) ? value.totals.slice() : [];
            const hasValue = Array.isArray(value.hasValue) ? value.hasValue.slice() : [];
            breakdown.set(key, {
              label: value.label || '',
              normalizedLabel: value.normalizedLabel || key,
              totals,
              hasValue,
            });
          });
        }
        return {
          label: attribute.label || '',
          normalizedLabel: attribute.normalizedLabel || normaliseNewProductLabel(attribute.label || ''),
          newOld: attribute.newOld instanceof Set ? new Set(attribute.newOld) : new Set(),
          totals: Array.isArray(attribute.totals) ? attribute.totals.slice() : [],
          hasValue: Array.isArray(attribute.hasValue) ? attribute.hasValue.slice() : [],
          newOldBreakdown: breakdown,
        };
      });
    }

    function loadNewProductPivotSectionsFromDataset() {
      const datasetSource = mainDatasetCache ? Promise.resolve(mainDatasetCache) : fetchMainDataset();
      return datasetSource.then((dataset) => {
        const sections = buildNewProductPivotSectionsFromDataset(dataset);
        const hasData = Array.isArray(sections)
          && sections.some((section) => Array.isArray(section?.rows) && section.rows.length);
        if (!hasData) {
          throw new Error('No new product pivot data available from the Main worksheet');
        }
        return sections;
      });
    }

    function fetchNewProductPivotSections() {
      if (Array.isArray(newProductPivotCache) && newProductPivotCache.length) {
        return Promise.resolve(newProductPivotCache);
      }
      if (newProductPivotPromise) {
        return newProductPivotPromise;
      }
      newProductPivotPromise = loadNewProductPivotSectionsFromDataset()
        .then((sections) => {
          newProductPivotCache = sections;
          newProductPivotPromise = null;
          return sections;
        })
        .catch((error) => {
          newProductPivotPromise = null;
          throw error;
        });
      return newProductPivotPromise;
    }


    function ensureNewProductFilterHook() {
      if (newProductFilterHookRegistered) {
        return;
      }
      $.fn.dataTable.ext.search.push((settings, data) => {
        if (!settings || !settings.sTableId) {
          return true;
        }
        const config = newProductTableFilterRegistry.get(settings.sTableId);
        if (!config) {
          return true;
        }
        const filterState = config.filterState;
        if (!filterState || !(filterState.fields instanceof Map)) {
          return true;
        }
        const newOldField = filterState.fields.get('new-old');
        if (newOldField && newOldField.activeSelection instanceof Set) {
          if (!newOldField.activeSelection.size) {
            return false;
          }
          const label = Array.isArray(data) ? data[0] : '';
          const normalizedLabel = normaliseNewProductLabel(label);
          const labelSet = config.rowLabelNewOldMap instanceof Map
            ? config.rowLabelNewOldMap.get(normalizedLabel)
            : null;
          if (!(labelSet instanceof Set) || !labelSet.size) {
            return false;
          }
          let matches = false;
          newOldField.activeSelection.forEach((value) => {
            if (labelSet.has(value)) {
              matches = true;
            }
          });
          return matches;
        }
        return true;
      });
      newProductFilterHookRegistered = true;
    }

    function ensureNewProductFilterState(config) {
      if (!config) {
        return null;
      }
      if (!Object.prototype.hasOwnProperty.call(config, 'defaultNewOldSelectionApplied')) {
        config.defaultNewOldSelectionApplied = false;
      }
      if (!config.filterState) {
        const definitionsSource = Array.isArray(config.filterFieldDefinitions) && config.filterFieldDefinitions.length
          ? config.filterFieldDefinitions
          : NEW_PRODUCT_FILTER_FIELDS;
        const definitions = definitionsSource.map((definition) => ({
          id: definition.id,
          label: definition.label || definition.id,
          type: definition.type || 'attribute',
          columnIndex: Number.isInteger(definition.columnIndex) ? definition.columnIndex : null,
        }));
        const fields = new Map();
        definitions.forEach((definition) => {
          fields.set(definition.id, {
            id: definition.id,
            label: definition.label,
            type: definition.type,
            columnIndex: definition.columnIndex,
            availableValues: [],
            activeSelection: null,
            pendingSelection: null,
          });
        });
        config.filterState = {
          definitions,
          fields,
          activeFieldId: definitions[0]?.id || null,
        };
      }
      const filterState = config.filterState;
      if (!config.defaultNewOldSelectionApplied) {
        const newOldField = filterState.fields.get('new-old');
        if (newOldField && !(newOldField.activeSelection instanceof Set)) {
          const initialSelection = NEW_PRODUCT_DEFAULT_NEW_OLD_SELECTION
            .map((value) => (typeof value === 'string' ? value.trim() : ''))
            .filter((value) => value.length > 0);
          if (initialSelection.length) {
            newOldField.activeSelection = new Set(initialSelection);
            config.defaultNewOldSelectionApplied = true;
          }
        }
      }
      return filterState;
    }

    function getActiveNewProductFilterField(config) {
      const filterState = ensureNewProductFilterState(config);
      if (!filterState) {
        return null;
      }
      let field = filterState.fields.get(filterState.activeFieldId);
      if (field && Array.isArray(field.availableValues) && field.availableValues.length) {
        return field;
      }
      const fallback = filterState.definitions.find((definition) => {
        const candidate = filterState.fields.get(definition.id);
        return candidate && Array.isArray(candidate.availableValues) && candidate.availableValues.length;
      });
      if (fallback) {
        filterState.activeFieldId = fallback.id;
        field = filterState.fields.get(fallback.id);
        return field;
      }
      return field || null;
    }

    function renderNewProductFilterFieldButtons(config) {
      if (!config || !config.filterFieldOptionsElement) {
        return;
      }
      const filterState = ensureNewProductFilterState(config);
      if (!filterState) {
        config.filterFieldOptionsElement.innerHTML = '';
        return;
      }
      const buttonsMarkup = filterState.definitions
        .map((definition) => {
          const field = filterState.fields.get(definition.id);
          const hasValues = field && Array.isArray(field.availableValues) && field.availableValues.length > 0;
          const isActive = hasValues && filterState.activeFieldId === definition.id;
          const pressedAttr = isActive ? ' aria-pressed="true"' : ' aria-pressed="false"';
          const disabledAttr = hasValues ? '' : ' aria-disabled="true" disabled';
          const safeLabel = escapeHtml(definition.label || definition.id);
          return `<button type="button" class="regular-filter__field-button"${pressedAttr}${disabledAttr} data-field-id="${definition.id}">${safeLabel}</button>`;
        })
        .join('');
      config.filterFieldOptionsElement.innerHTML = buttonsMarkup;
      const buttons = config.filterFieldOptionsElement.querySelectorAll('button[data-field-id]');
      buttons.forEach((button) => {
        button.addEventListener('click', (event) => {
          event.preventDefault();
          const fieldId = button.getAttribute('data-field-id');
          if (!fieldId) {
            return;
          }
          const field = filterState.fields.get(fieldId);
          if (!field || !Array.isArray(field.availableValues) || !field.availableValues.length) {
            return;
          }
          filterState.activeFieldId = fieldId;
          renderNewProductFilterFieldButtons(config);
          renderNewProductFilterOptions(config);
        });
      });
    }

    function updateNewProductFilterButtonState(config) {
      if (!config) {
        return;
      }
      const button = config.filterButton || document.getElementById(config.filterButtonId);
      const clearButton = config.filterClearButton || (config.filterClearButtonId
        ? document.getElementById(config.filterClearButtonId)
        : null);
      if (!button) {
        return;
      }
      const filterState = ensureNewProductFilterState(config);
      const hasValues = filterState
        ? Array.from(filterState.fields.values()).some((field) => Array.isArray(field.availableValues) && field.availableValues.length)
        : false;
      if (!hasValues) {
        button.setAttribute('aria-disabled', 'true');
        button.disabled = true;
        button.setAttribute('data-active', 'false');
        if (clearButton) {
          clearButton.hidden = true;
          clearButton.setAttribute('aria-hidden', 'true');
          clearButton.disabled = true;
        }
        return;
      }
      button.disabled = false;
      button.removeAttribute('aria-disabled');
      const isActive = filterState
        ? Array.from(filterState.fields.values()).some((field) => field.activeSelection instanceof Set && field.activeSelection.size > 0)
        : false;
      button.setAttribute('data-active', isActive ? 'true' : 'false');
      if (clearButton) {
        if (isActive) {
          clearButton.hidden = false;
          clearButton.removeAttribute('aria-hidden');
          clearButton.disabled = false;
        } else {
          clearButton.hidden = true;
          clearButton.setAttribute('aria-hidden', 'true');
          clearButton.disabled = true;
        }
      }
    }

    function computeNewProductRowValues(baseRow, attribute, normalizedNewOldSelection) {
      const safeRow = Array.isArray(baseRow) ? baseRow : [];
      const label = safeRow.length ? safeRow[0] : '';
      const metricCount = Math.max(0, safeRow.length - 1);
      if (!Array.isArray(normalizedNewOldSelection)) {
        const hasValues = (() => {
          if (!attribute || typeof attribute !== 'object') {
            return safeRow.slice(1).some((value) => !isPlaceholderValue(value));
          }
          if (Array.isArray(attribute.hasValue)) {
            return attribute.hasValue.some((flag) => Boolean(flag));
          }
          return safeRow.slice(1).some((value) => !isPlaceholderValue(value));
        })();
        return {
          row: safeRow.slice(),
          hasValues,
        };
      }
      if (!normalizedNewOldSelection.length) {
        return {
          row: [label, ...new Array(metricCount).fill('')],
          hasValues: false,
        };
      }
      const breakdown = attribute && attribute.newOldBreakdown instanceof Map ? attribute.newOldBreakdown : null;
      if (!breakdown || !breakdown.size) {
        return {
          row: [label, ...new Array(metricCount).fill('')],
          hasValues: false,
        };
      }
      const totals = new Array(metricCount).fill(0);
      const hasValueFlags = new Array(metricCount).fill(false);
      normalizedNewOldSelection.forEach((key) => {
        if (!key || !breakdown.has(key)) {
          return;
        }
        const entry = breakdown.get(key);
        if (!entry || typeof entry !== 'object') {
          return;
        }
        const entryTotals = Array.isArray(entry.totals) ? entry.totals : [];
        const entryHasValue = Array.isArray(entry.hasValue) ? entry.hasValue : [];
        entryTotals.forEach((value, index) => {
          if (!Number.isFinite(value)) {
            return;
          }
          totals[index] += value;
          if (entryHasValue[index] || value !== 0) {
            hasValueFlags[index] = true;
          }
        });
      });
      const hasValues = hasValueFlags.some((flag) => Boolean(flag));
      const computedRow = [label];
      for (let index = 0; index < metricCount; index += 1) {
        if (hasValueFlags[index]) {
          computedRow.push(totals[index]);
        } else {
          computedRow.push('');
        }
      }
      return {
        row: computedRow,
        hasValues,
      };
    }

    function updateNewProductTableFilters(config) {
      if (!config || !config.table) {
        return;
      }
      const filterState = ensureNewProductFilterState(config);
      const rowField = filterState ? filterState.fields.get('row') : null;
      const newOldField = filterState ? filterState.fields.get('new-old') : null;
      const rowSelection = rowField ? rowField.activeSelection : null;
      const newOldSelection = newOldField ? newOldField.activeSelection : null;
      const rawRows = Array.isArray(config.rawRows) ? config.rawRows : [];
      const rowAttributes = Array.isArray(config.rowAttributes) ? config.rowAttributes : [];
      const columns = Array.isArray(config.columns) ? config.columns : [];
      const normalizedRowSelection = rowSelection instanceof Set
        ? new Set(Array.from(rowSelection).map((value) => normaliseNewProductLabel(value)))
        : null;
      const normalizedNewOldSelection = newOldSelection instanceof Set
        ? Array.from(newOldSelection)
          .map((value) => normaliseNewProductStatus(value))
          .filter((value) => value.length)
        : null;
      const filteredRows = [];
      rawRows.forEach((row, index) => {
        if (!Array.isArray(row) || !row.length) {
          return;
        }
        const label = row[0];
        const normalizedLabel = normaliseNewProductLabel(label);
        if (normalizedRowSelection instanceof Set) {
          if (!normalizedRowSelection.size) {
            return;
          }
          if (!normalizedRowSelection.has(normalizedLabel)) {
            return;
          }
        }
        const attribute = rowAttributes[index];
        const computed = computeNewProductRowValues(row, attribute, normalizedNewOldSelection);
        if (!computed.hasValues) {
          return;
        }
        filteredRows.push(computed.row);
      });
      const formattedRows = filteredRows.map((row) => row.map((value, columnIndex) => formatCellValue(value, columns[columnIndex])));
      config.table.clear();
      if (formattedRows.length) {
        config.table.rows.add(formattedRows);
      }
      config.table.draw();
      if (typeof config.tableId === 'string') {
        newProductTableFilterRegistry.set(config.tableId, config);
      }
    }

    function renderNewProductFilterOptions(config) {
      if (!config || !config.filterOptionsElement) {
        return;
      }
      const filterState = ensureNewProductFilterState(config);
      const field = getActiveNewProductFilterField(config);
      if (!field || !Array.isArray(field.availableValues) || !field.availableValues.length) {
        config.filterOptionsElement.innerHTML = '';
        if (config.filterEmptyElement) {
          const message = field && field.label ? `No values available for ${field.label}` : 'No filter values available';
          config.filterEmptyElement.textContent = message;
          config.filterEmptyElement.hidden = false;
        }
        if (config.filterApplyButton) {
          config.filterApplyButton.disabled = true;
        }
        if (config.filterResetButton) {
          config.filterResetButton.disabled = true;
        }
        return;
      }
      if (config.filterEmptyElement) {
        config.filterEmptyElement.hidden = true;
      }
      if (config.filterApplyButton) {
        config.filterApplyButton.disabled = false;
      }
      if (config.filterResetButton) {
        config.filterResetButton.disabled = false;
      }
      let selection = field.pendingSelection;
      if (!(selection === null || selection instanceof Set)) {
        selection = field.activeSelection instanceof Set ? new Set(field.activeSelection) : null;
      }
      field.pendingSelection = selection instanceof Set ? new Set(selection) : selection;
      const optionsMarkup = field.availableValues
        .map((value, index) => {
          const checkboxId = `${config.id}-${field.id}-filter-option-${index}`;
          const isSelected = selection === null || (selection instanceof Set && selection.has(value));
          const checkedAttr = isSelected ? ' checked' : '';
          const safeLabel = escapeHtml(value);
          return `<label class="regular-filter__option" for="${checkboxId}"><input type="checkbox" id="${checkboxId}" value="${safeLabel}"${checkedAttr}><span>${safeLabel}</span></label>`;
        })
        .join('');
      config.filterOptionsElement.innerHTML = optionsMarkup;
      animateOptionList(config.filterOptionsElement, '.regular-filter__option');
      const checkboxes = config.filterOptionsElement.querySelectorAll('input[type="checkbox"]');
      checkboxes.forEach((checkbox) => {
        checkbox.addEventListener('change', (event) => {
          const target = event.target;
          const { checked } = target;
          const value = target.value;
          let selectionSet = field.pendingSelection;
          if (selectionSet === null) {
            selectionSet = new Set(field.availableValues);
          } else if (selectionSet instanceof Set) {
            selectionSet = new Set(selectionSet);
          } else {
            selectionSet = new Set(field.availableValues);
          }
          if (checked) {
            selectionSet.add(value);
          } else {
            selectionSet.delete(value);
          }
          if (selectionSet.size === field.availableValues.length) {
            selectionSet = null;
          }
          field.pendingSelection = selectionSet;
        });
      });
    }

    function openNewProductFilter(config) {
      if (!config || !config.filterContainer) {
        return;
      }
      const filterState = ensureNewProductFilterState(config);
      if (filterState) {
        filterState.fields.forEach((field) => {
          if (field.activeSelection instanceof Set) {
            field.pendingSelection = new Set(field.activeSelection);
          } else if (field.activeSelection === null) {
            field.pendingSelection = null;
          } else {
            field.pendingSelection = null;
          }
        });
      }
      renderNewProductFilterOptions(config);
      renderNewProductFilterFieldButtons(config);
      showFilterContainer(config.filterContainer);
      if (config.filterButton) {
        config.filterButton.setAttribute('aria-expanded', 'true');
      }
      const firstCheckbox = config.filterOptionsElement
        ? config.filterOptionsElement.querySelector('input[type="checkbox"]')
        : null;
      if (firstCheckbox) {
        firstCheckbox.focus();
      } else if (config.filterApplyButton) {
        config.filterApplyButton.focus();
      }
    }

    function closeNewProductFilter(config, options = {}) {
      if (!config || !config.filterContainer) {
        return;
      }
      const { keepSelection } = options;
      if (!keepSelection) {
        const filterState = ensureNewProductFilterState(config);
        if (filterState) {
          filterState.fields.forEach((field) => {
            field.pendingSelection = null;
          });
        }
      }
      hideFilterContainer(config.filterContainer);
      if (config.filterButton) {
        config.filterButton.setAttribute('aria-expanded', 'false');
      }
      if (options.returnFocus !== false && config.filterButton && typeof config.filterButton.focus === 'function') {
        config.filterButton.focus();
      }
    }

    function applyNewProductFilter(config) {
      if (!config) {
        return;
      }
      if (config.filterApplyButton) {
        flashButtonBusy(config.filterApplyButton);
      }
      const filterState = ensureNewProductFilterState(config);
      if (filterState) {
        filterState.fields.forEach((field) => {
          if (field.pendingSelection === null) {
            field.activeSelection = null;
          } else if (field.pendingSelection instanceof Set) {
            field.activeSelection = new Set(field.pendingSelection);
          } else {
            field.activeSelection = null;
          }
          field.pendingSelection = null;
        });
      }
      closeNewProductFilter(config, { keepSelection: true, returnFocus: true });
      updateNewProductFilterButtonState(config);
      if (config.table) {
        updateNewProductTableFilters(config);
        config.table.draw();
      }
    }

    function clearNewProductFilters(config) {
      if (!config) {
        return;
      }
      const filterState = ensureNewProductFilterState(config);
      if (filterState) {
        filterState.fields.forEach((field) => {
          field.activeSelection = null;
          field.pendingSelection = null;
        });
      }
      closeNewProductFilter(config, { keepSelection: true, returnFocus: false });
      updateNewProductFilterButtonState(config);
      if (config.table) {
        updateNewProductTableFilters(config);
        config.table.draw();
      }
    }

    function resetNewProductFilterState(config) {
      const filterState = ensureNewProductFilterState(config);
      if (filterState) {
        filterState.fields.forEach((field) => {
          field.availableValues = [];
          field.activeSelection = null;
          field.pendingSelection = null;
        });
      }
      config.rowLabelNewOldMap = new Map();
      config.defaultNewOldSelectionApplied = false;
    }

    function setupNewProductFilter(config) {
      if (!config) {
        return;
      }
      config.filterButton = document.getElementById(config.filterButtonId);
      config.filterContainer = document.getElementById(config.filterContainerId);
      config.filterOptionsElement = document.getElementById(config.filterOptionsId);
      config.filterFieldOptionsElement = document.getElementById(config.filterFieldOptionsId);
      config.filterApplyButton = document.getElementById(config.filterApplyId);
      config.filterResetButton = document.getElementById(config.filterResetId);
      config.filterEmptyElement = document.getElementById(config.filterEmptyId);
      config.filterCloseButton = config.filterContainer
        ? config.filterContainer.querySelector('.regular-filter__close')
        : null;
      config.filterClearButton = config.filterClearButtonId
        ? document.getElementById(config.filterClearButtonId)
        : null;
      ensureNewProductFilterState(config);
      if (!config.filterButton || !config.filterContainer) {
        return;
      }
      config.filterContainer.setAttribute('hidden', '');
      config.filterButton.addEventListener('click', () => {
        if (config.filterContainer.classList.contains('is-visible')) {
          closeNewProductFilter(config, { returnFocus: false });
        } else {
          openNewProductFilter(config);
        }
      });
      if (config.filterCloseButton) {
        config.filterCloseButton.addEventListener('click', () => closeNewProductFilter(config));
      }
      config.filterContainer.addEventListener('click', (event) => {
        const target = event.target;
        if (target === config.filterContainer || (target instanceof HTMLElement && target.classList.contains('regular-filter__backdrop'))) {
          closeNewProductFilter(config);
        }
      });
      config.filterContainer.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
          event.preventDefault();
          closeNewProductFilter(config);
        }
      });
      if (config.filterApplyButton) {
        config.filterApplyButton.addEventListener('click', () => applyNewProductFilter(config));
      }
      if (config.filterResetButton) {
        config.filterResetButton.addEventListener('click', () => {
          const filterState = ensureNewProductFilterState(config);
          if (filterState) {
            filterState.fields.forEach((field) => {
              field.pendingSelection = null;
              field.activeSelection = null;
            });
          }
          applyNewProductFilter(config);
        });
      }
      if (config.filterClearButton) {
        config.filterClearButton.addEventListener('click', () => clearNewProductFilters(config));
      }
    }

    function renderNewProductPivotTable(config, pivot) {
      const tableElement = document.getElementById(config.tableId);
      if (!tableElement) {
        return;
      }
      if ($.fn.DataTable.isDataTable(tableElement)) {
        $(tableElement).DataTable().destroy();
      }
      tableElement.innerHTML = '';
      const columns = Array.isArray(pivot?.columns) ? pivot.columns.slice() : [];
      const rows = Array.isArray(pivot?.rows)
        ? pivot.rows.map((row) => (Array.isArray(row) ? row.slice() : []))
        : [];
      const rowAttributes = Array.isArray(pivot?.rowAttributes)
        ? cloneNewProductRowAttributes(pivot.rowAttributes)
        : [];
      config.rawRows = rows.map((row) => row.slice());
      config.rowAttributes = rowAttributes;
      config.columns = columns.slice();
      if (!columns.length || !rows.length) {
        tableElement.innerHTML = '<tbody><tr><td>No data available</td></tr></tbody>';
        config.table = null;
        resetNewProductFilterState(config);
        if (typeof config.tableId === 'string') {
          newProductTableFilterRegistry.delete(config.tableId);
        }
        if (config.filterInitialised) {
          renderNewProductFilterFieldButtons(config);
        }
        updateNewProductFilterButtonState(config);
        return;
      }
      const dataColumns = columns.map((title) => ({ title }));
      const formattedRows = rows.map((row) => {
        return columns.map((columnName, columnIndex) => formatCellValue(row[columnIndex], columnName));
      });
      const columnDefs = columns.map((_, index) => ({
        targets: index,
        className: index === 0 ? 'cell-label' : 'cell-numeric',
      }));
      const table = $(tableElement).DataTable({
        data: formattedRows,
        columns: dataColumns,
        columnDefs,
        scrollX: true,
        deferRender: true,
        paging: false,
        ordering: false,
        info: false,
        searching: false,
        autoWidth: false,
      });
      config.table = table;
      ensureNewProductFilterHook();
      const filterState = ensureNewProductFilterState(config);
      const rowField = filterState ? filterState.fields.get('row') : null;
      const newOldField = filterState ? filterState.fields.get('new-old') : null;
      const labelValues = formattedRows.map((row) => row[0]).filter((value) => typeof value === 'string' && value.length);
      const uniqueLabels = Array.from(new Set(labelValues));
      if (rowField) {
        rowField.availableValues = uniqueLabels;
        if (!(rowField.activeSelection instanceof Set) && rowField.activeSelection !== null) {
          rowField.activeSelection = null;
        }
        if (rowField.activeSelection instanceof Set) {
          const validValues = Array.from(rowField.activeSelection).filter((value) => rowField.availableValues.includes(value));
          rowField.activeSelection = validValues.length ? new Set(validValues) : null;
        }
      }
      const rowLabelNewOldMap = new Map();
      const newOldValues = new Set();
      const appendNewOldValue = (valueSet, rawValue) => {
        if (typeof rawValue !== 'string') {
          return;
        }
        const trimmed = rawValue.trim();
        if (!trimmed) {
          return;
        }
        valueSet.add(trimmed);
        newOldValues.add(trimmed);
      };
      formattedRows.forEach((row, index) => {
        const label = row[0];
        const normalizedLabel = normaliseNewProductLabel(label);
        if (!normalizedLabel) {
          return;
        }
        let valueSet = rowLabelNewOldMap.get(normalizedLabel);
        if (!valueSet) {
          valueSet = new Set();
          rowLabelNewOldMap.set(normalizedLabel, valueSet);
        }
        const attribute = rowAttributes[index];
        const rawNewOld = attribute ? attribute.newOld : null;
        if (rawNewOld instanceof Set) {
          rawNewOld.forEach((value) => appendNewOldValue(valueSet, value));
        } else if (Array.isArray(rawNewOld)) {
          rawNewOld.forEach((value) => appendNewOldValue(valueSet, value));
        } else if (typeof rawNewOld === 'string') {
          appendNewOldValue(valueSet, rawNewOld);
        }
      });
      config.rowLabelNewOldMap = rowLabelNewOldMap;
      if (newOldField) {
        newOldField.availableValues = sortNewProductNewOldValues(Array.from(newOldValues));
        if (!(newOldField.activeSelection instanceof Set) && newOldField.activeSelection !== null) {
          newOldField.activeSelection = null;
        }
        if (newOldField.activeSelection instanceof Set) {
          const validValues = Array.from(newOldField.activeSelection).filter((value) => newOldField.availableValues.includes(value));
          newOldField.activeSelection = validValues.length ? new Set(validValues) : null;
        }
      }
      if (!config.filterInitialised) {
        setupNewProductFilter(config);
        config.filterInitialised = true;
      }
      renderNewProductFilterFieldButtons(config);
      updateNewProductTableFilters(config);
      if (config.table) {
        config.table.draw();
      }
      updateNewProductFilterButtonState(config);
      const subtitleElement = document.getElementById(config.subtitleId);
      if (subtitleElement) {
        const parts = [];
        const meta = pivot?.metadata || {};
        if (typeof meta.segmentLabel === 'string' && meta.segmentLabel.trim().length) {
          parts.push(meta.segmentLabel.trim());
        }
        if (typeof meta.segmentSelection === 'string' && meta.segmentSelection.trim().length) {
          parts.push(meta.segmentSelection.trim());
        }
        subtitleElement.textContent = parts.length
          ? `Segment: ${parts.join(' • ')}`
          : 'Pivot metrics derived from the Main sheet dataset';
      }
    }

    function renderNewProductPivots(sections) {
      if (!Array.isArray(sections) || !sections.length) {
        NEW_PRODUCT_PIVOT_CONFIGS.forEach((config) => {
          const tableElement = document.getElementById(config.tableId);
          if (tableElement) {
            tableElement.innerHTML = '<tbody><tr><td>No data available</td></tr></tbody>';
          }
          config.table = null;
          resetNewProductFilterState(config);
          if (typeof config.tableId === 'string') {
            newProductTableFilterRegistry.delete(config.tableId);
          }
          if (config.filterInitialised) {
            renderNewProductFilterFieldButtons(config);
          }
          updateNewProductFilterButtonState(config);
        });
        showNewProductStatus('No new product pivot data available', 'error');
        return;
      }
      NEW_PRODUCT_PIVOT_CONFIGS.forEach((config, index) => {
        const pivot = sections[index];
        if (pivot) {
          renderNewProductPivotTable(config, pivot);
        } else {
          const tableElement = document.getElementById(config.tableId);
          if (tableElement) {
            tableElement.innerHTML = '<tbody><tr><td>No data available</td></tr></tbody>';
          }
          config.table = null;
          resetNewProductFilterState(config);
          if (typeof config.tableId === 'string') {
            newProductTableFilterRegistry.delete(config.tableId);
          }
          if (config.filterInitialised) {
            renderNewProductFilterFieldButtons(config);
          }
          updateNewProductFilterButtonState(config);
        }
      });
      showNewProductStatus('');
    }

    function loadNewProductPivots() {
      if (newProductInitialised && Array.isArray(newProductPivotCache) && newProductPivotCache.length) {
        renderNewProductPivots(newProductPivotCache);
        setTabPanelLoading('new-product', false);
        return Promise.resolve(newProductPivotCache);
      }
      setTabPanelLoading('new-product', true, 'Loading new product pivots…');
      showNewProductStatus('Loading new product pivots…');
      return fetchNewProductPivotSections()
        .then((sections) => {
          newProductInitialised = true;
          newProductPivotCache = sections;
          renderNewProductPivots(sections);
        })
        .catch((error) => {
          console.error('Failed to load new product pivots:', error);
          showNewProductStatus(error.message || 'Unable to load new product pivots', 'error');
        })
        .finally(() => {
          setTabPanelLoading('new-product', false);
        });
    }

    function getStickyOffsetValue() {
      const rawValue = getComputedStyle(document.documentElement).getPropertyValue('--sticky-header-offset');
      const parsed = Number.parseFloat(rawValue);
      return Number.isFinite(parsed) ? parsed : 0;
    }

    function calculateScrollBodyHeight(rowCount, viewportTopOffset, reservedSpace) {
      const baselineMinHeight = HEADER_HEIGHT + MIN_VISIBLE_ROWS * ROW_HEIGHT;
      const viewportHeight = Number.isFinite(window.innerHeight) ? window.innerHeight : baselineMinHeight;
      let availableViewport = viewportHeight;
      const bottomSpacing = Number.isFinite(reservedSpace)
        ? reservedSpace
        : TABLE_BOTTOM_MARGIN;
      if (Number.isFinite(viewportTopOffset)) {
        availableViewport = viewportHeight - viewportTopOffset - bottomSpacing;
      } else {
        const stickyOffset = getStickyOffsetValue() + 64;
        availableViewport = viewportHeight - stickyOffset - bottomSpacing;
      }

      const usableViewport = Math.max(availableViewport, baselineMinHeight);
      const rowsToFillViewport = Math.max(
        MIN_VISIBLE_ROWS,
        Math.ceil(Math.max(0, usableViewport - HEADER_HEIGHT) / ROW_HEIGHT)
      );
      const effectiveRowCount = Math.max(rowCount, rowsToFillViewport);
      const desiredHeight = HEADER_HEIGHT + effectiveRowCount * ROW_HEIGHT;
      const heightWithinViewport = Math.min(desiredHeight, usableViewport);
      const minimumAllowedHeight = Math.min(baselineMinHeight, viewportHeight - bottomSpacing);
      const finalHeight = Math.max(heightWithinViewport, minimumAllowedHeight, HEADER_HEIGHT + MIN_VISIBLE_ROWS * ROW_HEIGHT);
      return Math.round(finalHeight);
    }

    function resizeLoTableContainers() {
      const containers = document.querySelectorAll('.lo-table-container');
      if (!containers.length) {
        return;
      }

      const viewportHeight = Number.isFinite(window.innerHeight) ? window.innerHeight : null;
      containers.forEach((container) => {
        if (!(container instanceof HTMLElement)) {
          return;
        }

        const parentPanel = container.closest('.tab-panel, .sub-tab-panel');
        const panelHidden = parentPanel && parentPanel.getAttribute('aria-hidden') === 'true';
        const hasClientRect = container.getClientRects().length > 0;
        if (panelHidden || !hasClientRect) {
          container.style.removeProperty('height');
          container.style.removeProperty('max-height');
          return;
        }

        const rect = container.getBoundingClientRect();
        if (!rect) {
          return;
        }

        const topOffset = Number.isFinite(rect.top) ? rect.top : null;
        let availableHeight = viewportHeight;
        if (Number.isFinite(viewportHeight) && Number.isFinite(topOffset)) {
          availableHeight = viewportHeight - topOffset - LO_TABLE_BOTTOM_MARGIN;
        }

        if (!Number.isFinite(availableHeight)) {
          availableHeight = MIN_LO_TABLE_HEIGHT;
        }

        const finalHeight = Math.max(Math.floor(availableHeight), MIN_LO_TABLE_HEIGHT);
        container.style.height = `${finalHeight}px`;
        container.style.maxHeight = `${finalHeight}px`;
      });
    }

    function syncHeaderColumnWidths(table) {
      if (!table) {
        return;
      }
      const container = table.table().container();
      const scrollHead = container.querySelector('.dataTables_scrollHead');
      const scrollBody = container.querySelector('.dataTables_scrollBody');
      const scrollFoot = container.querySelector('.dataTables_scrollFoot');
      const headerTable = scrollHead ? scrollHead.querySelector('table') : null;
      const bodyTable = scrollBody ? scrollBody.querySelector('table') : null;
      const footTables = [];
      if (scrollFoot) {
        const scrollFootTable = scrollFoot.querySelector('table');
        if (scrollFootTable) {
          footTables.push(scrollFootTable);
        }
      }
      const baseFoot = table.table().footer();
      if (baseFoot) {
        footTables.push(baseFoot);
      }
      if (!headerTable || !bodyTable) {
        return;
      }

      const columnWidths = [];
      const columnIndexes = table.columns().indexes().toArray();
      columnIndexes.forEach((columnIndex) => {
        const numericIndex = Number(columnIndex);
        const column = table.column(columnIndex);
        const headerCell = column.header();
        if (!headerCell) {
          return;
        }

        const bodyCells = column.nodes().toArray();
        let maxWidth = 0;
        bodyCells.forEach((cell) => {
          if (!(cell instanceof HTMLElement)) {
            return;
          }
          const { width } = cell.getBoundingClientRect();
          if (width > maxWidth) {
            maxWidth = width;
          }
        });

        if (maxWidth <= 0) {
          const { width } = headerCell.getBoundingClientRect();
          maxWidth = width;
        }

        if (maxWidth > 0) {
          const numericWidth = Math.ceil(maxWidth);
          columnWidths[numericIndex] = numericWidth;
          const widthPx = `${numericWidth}px`;
          headerCell.style.width = widthPx;
          headerCell.style.minWidth = widthPx;
          headerCell.style.maxWidth = widthPx;
          headerCell.style.boxSizing = 'border-box';
          bodyCells.forEach((cell) => {
            if (cell instanceof HTMLElement) {
              cell.style.width = widthPx;
              cell.style.minWidth = widthPx;
              cell.style.maxWidth = widthPx;
              cell.style.boxSizing = 'border-box';
            }
          });
          footTables.forEach((footTable) => {
            const footCells = footTable.querySelectorAll('th');
            const footCell = footCells[columnIndex];
            if (footCell instanceof HTMLElement) {
              footCell.style.width = widthPx;
              footCell.style.minWidth = widthPx;
              footCell.style.maxWidth = widthPx;
              footCell.style.boxSizing = 'border-box';
            }
          });
        }
      });

      const updatePreambleCellWidths = (tableLike) => {
        if (!tableLike) {
          return;
        }
        const head = tableLike.tHead || tableLike.querySelector('thead');
        if (!head) {
          return;
        }
        const rows = Array.from(head.rows);
        if (rows.length <= 1) {
          return;
        }
        const preambleRows = rows.slice(0, -1);
        preambleRows.forEach((row) => {
          let columnPosition = 0;
          Array.from(row.cells).forEach((cell) => {
            const span = Math.max(1, Number(cell.colSpan) || 1);
            let totalWidth = 0;
            for (let offset = 0; offset < span; offset += 1) {
              const lookupIndex = columnPosition + offset;
              const widthValue = columnWidths[lookupIndex];
              if (Number.isFinite(widthValue)) {
                totalWidth += widthValue;
              }
            }
            if (totalWidth > 0) {
              const widthPx = `${totalWidth}px`;
              cell.style.width = widthPx;
              cell.style.minWidth = widthPx;
              cell.style.maxWidth = widthPx;
              cell.style.boxSizing = 'border-box';
            }
            columnPosition += span;
          });
        });
      };

      updatePreambleCellWidths(headerTable);
      const baseHeader = table.table().header();
      if (baseHeader) {
        const baseHeaderTable = baseHeader.closest('table');
        updatePreambleCellWidths(baseHeaderTable);
      }

      const bodyWidth = bodyTable.getBoundingClientRect().width;
      if (bodyWidth > 0) {
        const widthPx = `${Math.ceil(bodyWidth)}px`;
        headerTable.style.width = widthPx;
        const scrollHeadInner = scrollHead.querySelector('.dataTables_scrollHeadInner');
        if (scrollHeadInner) {
          scrollHeadInner.style.width = widthPx;
        }
        if (scrollFoot) {
          const scrollFootInner = scrollFoot.querySelector('.dataTables_scrollFootInner');
          if (scrollFootInner) {
            scrollFootInner.style.width = widthPx;
          }
        }
        footTables.forEach((footTable) => {
          footTable.style.width = widthPx;
        });
      }
    }

    function adjustScrollBodyPadding(table) {
      if (!table) {
        return;
      }
      const container = table.table().container();
      const scrollBody = container.querySelector('.dataTables_scrollBody');
      if (!scrollBody) {
        return;
      }
      const scrollFoot = container.querySelector('.dataTables_scrollFoot');
      let paddingBottom = 0;
      if (scrollFoot) {
        const footRect = scrollFoot.getBoundingClientRect();
        if (footRect && Number.isFinite(footRect.height)) {
          paddingBottom = Math.max(0, Math.ceil(footRect.height) - 6);
        }
      }
      const MINIMUM_PADDING = 16;
      const appliedPadding = Math.max(paddingBottom, MINIMUM_PADDING);
      scrollBody.style.paddingBottom = `${appliedPadding}px`;
    }

    function applyHeaderPreambleToTable(tableElement, preamble, columnTitles) {
      if (!tableElement) {
        return;
      }
      const columns = Array.isArray(columnTitles) ? columnTitles.slice() : [];
      const columnCount = columns.length;
      const existingThead = tableElement.querySelector('thead');
      if (existingThead) {
        tableElement.removeChild(existingThead);
      }
      if (columnCount === 0) {
        return;
      }

      const thead = document.createElement('thead');
      const hasPreamble = preamble
        && Array.isArray(preamble.rows)
        && preamble.rows.length > 0;

      if (hasPreamble) {
        const normalisedRows = [];
        for (let rowIndex = 0; rowIndex < preamble.rows.length; rowIndex += 1) {
          const sourceRow = Array.isArray(preamble.rows[rowIndex]) ? preamble.rows[rowIndex] : [];
          const row = new Array(columnCount).fill('');
          for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
            if (columnIndex < sourceRow.length) {
              row[columnIndex] = sourceRow[columnIndex];
            }
          }
          normalisedRows.push(row);
        }

        const mergeEntries = Array.isArray(preamble.merges) ? preamble.merges : [];
        const mergeMap = new Map();
        const skipSet = new Set();
        mergeEntries.forEach((entry) => {
          if (!entry) {
            return;
          }
          const startRow = Number.isFinite(entry.row) ? Math.max(0, Math.floor(entry.row)) : 0;
          const startCol = Number.isFinite(entry.column) ? Math.max(0, Math.floor(entry.column)) : 0;
          const requestedRowSpan = Number.isFinite(entry.rowSpan) ? Math.floor(entry.rowSpan) : 1;
          const requestedColSpan = Number.isFinite(entry.colSpan) ? Math.floor(entry.colSpan) : 1;
          const rowSpan = Math.max(1, requestedRowSpan);
          const colSpan = Math.max(1, requestedColSpan);
          if (startRow >= normalisedRows.length || startCol >= columnCount) {
            return;
          }
          const maxRow = Math.min(normalisedRows.length - 1, startRow + rowSpan - 1);
          const maxCol = Math.min(columnCount - 1, startCol + colSpan - 1);
          if (maxRow < startRow || maxCol < startCol) {
            return;
          }
          const effectiveRowSpan = Math.max(1, maxRow - startRow + 1);
          const effectiveColSpan = Math.max(1, maxCol - startCol + 1);
          const key = `${startRow},${startCol}`;
          mergeMap.set(key, { rowSpan: effectiveRowSpan, colSpan: effectiveColSpan });
          for (let row = startRow; row <= maxRow; row += 1) {
            for (let col = startCol; col <= maxCol; col += 1) {
              if (row === startRow && col === startCol) {
                continue;
              }
              skipSet.add(`${row},${col}`);
            }
          }
        });

        normalisedRows.forEach((row, rowIndex) => {
          const tr = document.createElement('tr');
          tr.classList.add('table-preamble-row');
          for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
            const cellKey = `${rowIndex},${columnIndex}`;
            if (skipSet.has(cellKey)) {
              continue;
            }
            const span = mergeMap.get(cellKey) || null;
            const rawValue = row && columnIndex < row.length ? row[columnIndex] : '';
            let displayValue = rawValue;
            if (typeof window.formatDateLabel === 'function') {
              const formattedDate = window.formatDateLabel(rawValue);
              if (formattedDate) {
                displayValue = formattedDate;
              }
            }
            const text = typeof displayValue === 'string'
              ? displayValue
              : (displayValue === null || displayValue === undefined ? '' : String(displayValue));
            const trimmed = text.trim();
            const th = document.createElement('th');
            th.classList.add('table-preamble__cell');
            if (span) {
              if (span.rowSpan > 1) {
                th.rowSpan = span.rowSpan;
              }
              if (span.colSpan > 1) {
                th.colSpan = span.colSpan;
              }
            }
            if (trimmed.length === 0) {
              th.classList.add('is-empty');
              th.innerHTML = '&nbsp;';
            } else {
              th.innerHTML = escapeHtml(text);
            }
            tr.appendChild(th);
          }
          thead.appendChild(tr);
        });
      }

      const headerRow = document.createElement('tr');
      columns.forEach((title, columnIndex) => {
        const th = document.createElement('th');
        th.dataset.columnIndex = String(columnIndex);
        const value = typeof title === 'string'
          ? title
          : (title === null || title === undefined ? '' : String(title));
        th.innerHTML = escapeHtml(value);
        headerRow.appendChild(th);
      });
      thead.appendChild(headerRow);
      tableElement.insertBefore(thead, tableElement.firstChild || null);
    }

    function calculateRegularTableReservedSpace(container) {
      let reservedSpace = DEFAULT_REGULAR_TABLE_RESERVED_SPACE;
      if (!container) {
        return reservedSpace;
      }
      const footer = container.querySelector('.regular-table__footer, .main-table__footer');
      if (!footer) {
        return reservedSpace;
      }
      const footerRect = footer.getBoundingClientRect();
      if (!footerRect || !Number.isFinite(footerRect.height)) {
        return reservedSpace;
      }
      const measuredFooterSpace = Math.ceil(footerRect.height) + REGULAR_TABLE_FOOTER_EXTRA_GAP;
      reservedSpace = Math.max(reservedSpace, TABLE_BOTTOM_MARGIN + measuredFooterSpace);
      return reservedSpace;
    }

    function applyTableHeight(table) {
      if (!table) {
        return;
      }
      const rowCount = table.rows({ page: 'current' }).count();
      const container = table.table().container();
      const scrollBody = container.querySelector('.dataTables_scrollBody');
      const scrollBodyRect = scrollBody ? scrollBody.getBoundingClientRect() : null;
      const viewportTopOffset = scrollBodyRect && Number.isFinite(scrollBodyRect.top) ? scrollBodyRect.top : null;
      const reservedSpace = calculateRegularTableReservedSpace(container);
      const height = calculateScrollBodyHeight(rowCount, viewportTopOffset, reservedSpace);
      if (scrollBody) {
        scrollBody.style.height = `${height}px`;
        scrollBody.style.maxHeight = `${height}px`;
      }
      const settings = table.settings()[0];
      if (settings && settings.oScroll) {
        settings.oScroll.sY = `${height}px`;
      }
      table.columns.adjust();
      requestAnimationFrame(() => {
        syncHeaderColumnWidths(table);
        adjustScrollBodyPadding(table);
      });
    }

    function applyFooterValuesToCells(cells, values, numericColumnSet, totalIndex = totalColumnIndex) {
      if (!cells || !values || values.length === 0) {
        return;
      }
      values.forEach((value, index) => {
        const cell = cells[index];
        if (!cell) {
          return;
        }
        const displayValue = value ?? '';
        const isLabelCell = index === 0;
        const isNumeric = numericColumnSet.has(index);
        const isTotalColumn = Number.isFinite(totalIndex) && totalIndex >= 0 && index === totalIndex;
        cell.textContent = displayValue;
        cell.classList.toggle('cell-total-label', isLabelCell);
        cell.classList.toggle('cell-total', !isLabelCell);
        cell.classList.toggle('cell-numeric', isNumeric && !isLabelCell);
        cell.classList.toggle('cell-total-column', isTotalColumn);
        if (isLabelCell) {
          cell.style.textAlign = 'left';
        } else if (isNumeric) {
          cell.style.textAlign = 'right';
        } else {
          cell.style.textAlign = 'left';
        }
      });
    }

    function ensureTableFooter(tableElement, columnCount, values = [], numericColumnSet = new Set(), totalIndex = totalColumnIndex) {
      if (!tableElement) {
        return;
      }
      const existingFoot = tableElement.querySelector('tfoot');
      if (existingFoot) {
        tableElement.removeChild(existingFoot);
      }
      const tfoot = document.createElement('tfoot');
      const row = document.createElement('tr');
      for (let index = 0; index < columnCount; index += 1) {
        row.appendChild(document.createElement('th'));
      }
      tfoot.appendChild(row);
      tableElement.appendChild(tfoot);
      if (values.length === columnCount) {
        const cells = tfoot.querySelectorAll('th');
        applyFooterValuesToCells(cells, values, numericColumnSet, totalIndex);
      }
    }

    function renderFooterRow(table, values, numericColumnSet, totalIndex) {
      if (!table || !Array.isArray(values) || !values.length) {
        return;
      }
      const container = table.table().container();
      const baseFooter = table.table().footer();
      const footerTables = [];
      if (baseFooter) {
        footerTables.push(baseFooter);
      }
      const scrollFoot = container.querySelector('.dataTables_scrollFoot table');
      if (scrollFoot) {
        footerTables.push(scrollFoot);
      }
      footerTables.forEach((footerTable) => {
        const cells = footerTable.querySelectorAll('th');
        applyFooterValuesToCells(cells, values, numericColumnSet, totalIndex);
      });
      adjustScrollBodyPadding(table);
    }

    function refreshRegularTableLayout() {
      if (!regularTableInitialised || !regularTable) {
        return;
      }
      applyTableHeight(regularTable);
      if (SHOW_REGULAR_TOTAL_ROW) {
        updateRegularTableFooter(regularTable);
      } else if (regularTableFooterValues.length) {
        renderFooterRow(regularTable, regularTableFooterValues, regularTableNumericColumnSet, totalColumnIndex);
      }
      moveRegularTablePagination();
    }

    function moveRegularTablePagination() {
      const paginationHost = document.getElementById('regular-table-pagination');
      const tableWrapper = document.getElementById('regularTable_wrapper');
      if (!paginationHost || !tableWrapper) {
        return;
      }
      let paginate = tableWrapper.querySelector('.dataTables_paginate');
      if (!paginate) {
        paginate = paginationHost.querySelector('.dataTables_paginate');
      }
      if (!paginate) {
        paginationHost.textContent = '';
        return;
      }
      if (paginate.parentElement !== paginationHost) {
        paginationHost.textContent = '';
        paginationHost.appendChild(paginate);
      }
    }

    function calculateMainTableFooterValues(table) {
      if (!table || !mainTableAugmentedDataset) {
        return mainTableFooterValues;
      }

      const columns = Array.isArray(mainTableAugmentedDataset.columns)
        ? mainTableAugmentedDataset.columns
        : [];
      const columnCount = columns.length;
      if (columnCount === 0) {
        return [];
      }

      const baseValues = buildFormattedFooterValues(mainTableAugmentedDataset, {
        formatOptions: MAIN_TABLE_FORMAT_OPTIONS,
      });
      const values = Array.isArray(baseValues) && baseValues.length === columnCount
        ? baseValues.slice()
        : (() => {
            const fallback = new Array(columnCount).fill('');
            const totalsRow = Array.isArray(mainTableAugmentedDataset.totalsRow)
              ? mainTableAugmentedDataset.totalsRow
              : [];
            const labelValue = totalsRow[0];
            if (typeof labelValue === 'string' && labelValue.trim().length) {
              fallback[0] = labelValue;
            } else {
              fallback[0] = TOTAL_ROW_LABEL;
            }
            return fallback;
          })();

      if (!mainTableNumericColumnSet || mainTableNumericColumnSet.size === 0) {
        return values;
      }

      const filteredRows = table.rows({ search: 'applied' }).data().toArray();

      mainTableNumericColumnSet.forEach((columnIndex) => {
        if (typeof columnIndex !== 'number' || columnIndex < 0 || columnIndex >= columnCount) {
          return;
        }
        let sum = 0;
        let hasValue = false;
        filteredRows.forEach((row) => {
          if (!row || columnIndex >= row.length) {
            return;
          }
          const numericValue = parseNumericValue(row[columnIndex]);
          if (numericValue !== null) {
            sum += numericValue;
            hasValue = true;
          }
        });

        const formattedTotal = hasValue
          ? formatCellValue(sum, columns[columnIndex], MAIN_TABLE_FORMAT_OPTIONS)
          : formatCellValue(0, columns[columnIndex], MAIN_TABLE_FORMAT_OPTIONS);
        values[columnIndex] = formattedTotal;
      });

      return values;
    }

    function logMainTillDateHeaders() {
      const headers = Array.isArray(mainTillDateHeaders) ? mainTillDateHeaders : [];
      console.log('[Main] non-date columns (till date):', headers);
    }

    function verifyMainTillDateCells(table) {
      if (!table || typeof table.column !== 'function') {
        return;
      }
      if (!Array.isArray(mainTillDateColumnIndices) || mainTillDateColumnIndices.length === 0) {
        return;
      }
      const datePattern = /^[0-9]{2}-[0-9]{2}-[0-9]{4}$/;
      mainTillDateColumnIndices.forEach((columnIndex, headerIndex) => {
        if (!Number.isFinite(columnIndex)) {
          return;
        }
        const column = table.column(columnIndex);
        if (!column || typeof column.data !== 'function') {
          return;
        }
        const data = column.data().toArray();
        const hasDatePattern = data.some((value) => {
          if (value === null || value === undefined) {
            return false;
          }
          const text = typeof value === 'string' ? value.trim() : String(value).trim();
          return datePattern.test(text);
        });
        if (hasDatePattern) {
          const headerName = Array.isArray(mainTillDateHeaders)
            ? (mainTillDateHeaders[headerIndex]
              || (Array.isArray(mainTableAugmentedDataset?.columns)
                ? mainTableAugmentedDataset.columns[columnIndex]
                : `Column ${columnIndex + 1}`))
            : (Array.isArray(mainTableAugmentedDataset?.columns)
              ? mainTableAugmentedDataset.columns[columnIndex]
              : `Column ${columnIndex + 1}`);
          console.warn(`[Main] warning: dd-mm-yyyy detected in "${headerName}" column`);
        }
      });
    }

    function updateMainTableFooter(table) {
      if (!SHOW_REGULAR_TOTAL_ROW || !table) {
        return;
      }
      mainTableFooterValues = calculateMainTableFooterValues(table);
      renderFooterRow(table, mainTableFooterValues, mainTableNumericColumnSet, mainTotalColumnIndex);
    }

    function refreshMainTableLayout() {
      if (!mainTableInitialised || !mainTable) {
        return;
      }
      applyTableHeight(mainTable);
      if (SHOW_REGULAR_TOTAL_ROW) {
        updateMainTableFooter(mainTable);
      } else if (mainTableFooterValues.length) {
        renderFooterRow(mainTable, mainTableFooterValues, mainTableNumericColumnSet, mainTotalColumnIndex);
      }
      moveMainTablePagination();
    }

    function moveMainTablePagination() {
      const paginationHost = document.getElementById('main-table-pagination');
      const tableWrapper = document.getElementById('main-table_wrapper');
      if (!paginationHost || !tableWrapper) {
        return;
      }
      let paginate = tableWrapper.querySelector('.dataTables_paginate');
      if (!paginate) {
        paginate = paginationHost.querySelector('.dataTables_paginate');
      }
      if (!paginate) {
        paginationHost.textContent = '';
        return;
      }
      if (paginate.parentElement !== paginationHost) {
        paginationHost.textContent = '';
        paginationHost.appendChild(paginate);
      }
    }

    function setActiveTab(targetTab) {
      const previousTab = activeTabId;
      activeTabId = targetTab;
      tabButtons.forEach((button) => {
        const isActive = button.dataset.tab === targetTab;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-selected', String(isActive));
      });
      scrollActiveTabIntoView(targetTab);
      updateTabIndicator(targetTab);
      const nextPanel = getTabPanelElement(targetTab);
      const previousPanel = previousTab ? getTabPanelElement(previousTab) : null;
      if (previousPanel && previousPanel !== nextPanel) {
        hideTabPanel(previousPanel);
      }
      if (nextPanel) {
        showTabPanel(nextPanel);
      }
      if (targetTab !== 'main' && mainFilterContainerElement && mainFilterContainerElement.classList.contains('is-visible')) {
        closeMainFilter({ returnFocus: false });
      }
      if (targetTab === 'regular') {
        loadRegularTable();
        refreshRegularTableLayout();
        return;
      }
      closeHeaderMenu();
      if (targetTab !== 'sales-gap' && activeSalesGapFilterConfig && activeSalesGapFilterConfig.filterContainer && activeSalesGapFilterConfig.filterContainer.classList.contains('is-visible')) {
        closeSalesGapFilter(activeSalesGapFilterConfig, { returnFocus: false });
      }
      if (targetTab === 'main') {
        if (regularFilterContainerElement && regularFilterContainerElement.classList.contains('is-visible')) {
          closeRegularFilter({ returnFocus: false });
        }
        loadMainTable();
        refreshMainTableLayout();
        return;
      }
      if (targetTab === 'dashboard') {
        if (regularFilterContainerElement && regularFilterContainerElement.classList.contains('is-visible')) {
          closeRegularFilter({ returnFocus: false });
        }
        loadMainDashboard();
        return;
      }
      if (targetTab === 'sku-summary') {
        loadSkuSummaryTable();
        return;
      }
      if (targetTab === 'new-product') {
        loadNewProductPivots();
        return;
      }
      if (targetTab === 'sales-gap') {
        if (regularFilterContainerElement && regularFilterContainerElement.classList.contains('is-visible')) {
          closeRegularFilter({ returnFocus: false });
        }
        loadSalesGapTable();
        return;
      }
      if (targetTab === 'lo' || targetTab === 'platform') {
        if (regularFilterContainerElement && regularFilterContainerElement.classList.contains('is-visible')) {
          closeRegularFilter({ returnFocus: false });
        }
        const showLoLoader = targetTab === 'lo' && !loTablesInitialised;
        const showPlatformLoader = targetTab === 'platform' && !platformTablesInitialised;
        if (showLoLoader || showPlatformLoader) {
          renderLoMessage('Loading data…');
        }
        if (showLoLoader) {
          setTabPanelLoading('lo', true, 'Loading listing owner metrics…');
        }
        if (showPlatformLoader) {
          setTabPanelLoading('platform', true, 'Loading store metrics…');
        }
        fetchRegularDataset()
          .then((dataset) => {
            initializeLoTables(dataset);
          })
          .catch((error) => {
            renderLoMessage(error.message || 'Unable to load data');
          })
          .finally(() => {
            setTabPanelLoading('lo', false);
            setTabPanelLoading('platform', false);
          });
        requestAnimationFrame(() => resizeLoTableContainers());
      }
    }

    tabButtons.forEach((button) => {
      button.addEventListener('click', () => setActiveTab(button.dataset.tab));
    });

    function setActiveLoSubTab(targetSubTab) {
      if (!loSubTabButtons.length) {
        return;
      }
      loSubTabButtons.forEach((button) => {
        const isActive = button.dataset.subtab === targetSubTab;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-selected', String(isActive));
      });
      loSubTabPanels.forEach((panel) => {
        const isActive = panel.dataset.subtab === targetSubTab;
        panel.classList.toggle('active', isActive);
        panel.setAttribute('aria-hidden', String(!isActive));
      });
      if (loFilterButtonElement) {
        const hideFilter = targetSubTab === 'spend';
        loFilterButtonElement.classList.toggle('lo-card__filter-button--hidden', hideFilter);
        loFilterButtonElement.setAttribute('aria-hidden', String(hideFilter));
        if (hideFilter) {
          loFilterButtonElement.setAttribute('tabindex', '-1');
          if (regularFilterContainerElement && regularFilterContainerElement.classList.contains('is-visible')) {
            closeRegularFilter({ returnFocus: false });
          }
        } else {
          loFilterButtonElement.removeAttribute('tabindex');
        }
      }
      requestAnimationFrame(() => resizeLoTableContainers());
    }

    loSubTabButtons.forEach((button) => {
      button.addEventListener('click', () => setActiveLoSubTab(button.dataset.subtab));
    });

    if (loSubTabButtons.length > 0) {
      setActiveLoSubTab('sales');
    }

    function setActivePlatformSubTab(targetSubTab) {
      if (!platformSubTabButtons.length) {
        return;
      }
      platformSubTabButtons.forEach((button) => {
        const isActive = button.dataset.subtab === targetSubTab;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-selected', String(isActive));
      });
      platformSubTabPanels.forEach((panel) => {
        const isActive = panel.dataset.subtab === targetSubTab;
        panel.classList.toggle('active', isActive);
        panel.setAttribute('aria-hidden', String(!isActive));
      });
      requestAnimationFrame(() => resizeLoTableContainers());
    }

    platformSubTabButtons.forEach((button) => {
      button.addEventListener('click', () => setActivePlatformSubTab(button.dataset.subtab));
    });

    if (platformSubTabButtons.length > 0) {
      setActivePlatformSubTab('sales');
    }

    function updateStickyOffset() {
      const headerEl = document.querySelector('header');
      const navEl = document.querySelector('.tab-nav');
      const headerHeight = headerEl ? headerEl.offsetHeight : 0;
      const navHeight = navEl ? navEl.offsetHeight : 0;
      let navTop = 0;
      if (navEl) {
        const computedTop = window.getComputedStyle(navEl).top;
        const numericTop = Number.parseFloat(computedTop || '0');
        navTop = Number.isFinite(numericTop) ? numericTop : 0;
      }
      const offset = headerHeight + navHeight + navTop + 24;
      document.documentElement.style.setProperty('--sticky-header-offset', `${offset}px`);
    }

    initializeDashboardPivotFilters();
    setActiveTab('regular');
    NEW_PRODUCT_PIVOT_CONFIGS.forEach((config) => updateNewProductFilterButtonState(config));
    updateStickyOffset();
    updateTabIndicator(activeTabId);
    updateTabNavScrollShadows();
    window.addEventListener('resize', () => {
      updateStickyOffset();
      updateTabIndicator(activeTabId);
      scrollActiveTabIntoView(activeTabId);
      updateTabNavScrollShadows();
      resizeLoTableContainers();
      if (regularTableInitialised && regularTable) {
        applyTableHeight(regularTable);
      }
      if (mainTableInitialised && mainTable) {
        applyTableHeight(mainTable);
      }
    });

    if (tabNavElement) {
      tabNavElement.addEventListener(
        'scroll',
        () => {
          updateTabNavScrollShadows();
        },
        { passive: true },
      );
    }

    function ensureHeaderMenu() {
      if (!headerMenuElement) {
        headerMenuElement = document.createElement('div');
        headerMenuElement.className = 'header-menu hidden';
        headerMenuElement.setAttribute('role', 'dialog');
        headerMenuElement.setAttribute('aria-modal', 'false');
        document.body.appendChild(headerMenuElement);

        document.addEventListener('click', (event) => {
          if (!headerMenuElement.classList.contains('hidden')) {
            const target = event.target;
            if (headerMenuElement && !headerMenuElement.contains(target) && !(target.closest('#regularTable thead')) && !(target.closest('#main-table thead'))) {
              closeHeaderMenu();
            }
          }
        });

        document.addEventListener('keydown', (event) => {
          if (event.key === 'Escape') {
            closeHeaderMenu();
          }
        });

        window.addEventListener('resize', () => {
          if (!headerMenuElement.classList.contains('hidden') && activeHeaderCell) {
            positionHeaderMenu(activeHeaderCell);
          }
        });

        window.addEventListener('scroll', () => {
          if (!headerMenuElement.classList.contains('hidden') && activeHeaderCell) {
            positionHeaderMenu(activeHeaderCell);
          }
        }, { passive: true });
      }
    }

    function closeHeaderMenu() {
      if (headerMenuElement) {
        headerMenuElement.classList.add('hidden');
        headerMenuElement.innerHTML = '';
      }
      activeHeaderCell = null;
      activeColumnIndex = null;
    }

    function hasActiveColumnFilters(filters = columnFilters) {
      const source = filters || {};
      return Object.values(source).some((values) => Array.isArray(values) && values.length > 0);
    }

    function setFilterClearButtonsVisibility(buttons, isActive) {
      const shouldShow = Boolean(isActive);
      buttons.forEach((button) => {
        if (!button) {
          return;
        }
        if (shouldShow) {
          button.hidden = false;
          button.removeAttribute('aria-hidden');
          button.disabled = false;
        } else {
          button.hidden = true;
          button.setAttribute('aria-hidden', 'true');
          button.disabled = true;
        }
      });
    }

    function updateRegularFilterButtonState() {
      const isActive = hasActiveColumnFilters();
      const targets = regularFilterButtons.length
        ? regularFilterButtons
        : (regularFilterButtonElement ? [regularFilterButtonElement] : []);
      targets.forEach((button) => {
        if (button) {
          button.setAttribute('data-active', isActive ? 'true' : 'false');
        }
      });
      setFilterClearButtonsVisibility(regularFilterClearButtons, isActive);
    }

    function updateMainFilterButtonState() {
      const isActive = hasActiveColumnFilters(mainColumnFilters);
      const targets = mainFilterButtons.length
        ? mainFilterButtons
        : (mainFilterButtonElement ? [mainFilterButtonElement] : []);
      targets.forEach((button) => {
        if (button) {
          button.setAttribute('data-active', isActive ? 'true' : 'false');
        }
      });
      setFilterClearButtonsVisibility(mainFilterClearButtons, isActive);
    }

    function syncRegularFilterSelectionFromFilters(columnIndex) {
      if (!Number.isFinite(columnIndex)) {
        return;
      }
      if (!(regularFilterSelection instanceof Set)) {
        regularFilterSelection = new Set();
      }
      regularFilterSelection.clear();
      const options = columnValueOptions[columnIndex] || [];
      const activeValues = columnFilters[columnIndex];
      if (Array.isArray(activeValues) && activeValues.length > 0) {
        activeValues.forEach((value) => regularFilterSelection.add(value));
      } else {
        options.forEach((value) => regularFilterSelection.add(value));
      }
    }

    function syncMainFilterSelectionFromFilters(columnIndex) {
      if (!Number.isFinite(columnIndex)) {
        return;
      }
      if (!(mainFilterSelection instanceof Set)) {
        mainFilterSelection = new Set();
      }
      mainFilterSelection.clear();
      const options = mainColumnValueOptions[columnIndex] || [];
      const activeValues = mainColumnFilters[columnIndex];
      if (Array.isArray(activeValues) && activeValues.length > 0) {
        activeValues.forEach((value) => mainFilterSelection.add(value));
      } else {
        options.forEach((value) => mainFilterSelection.add(value));
      }
    }

    function updateRegularFilterSelectAllState() {
      if (!regularFilterSelectAllInput) {
        return;
      }
      const checkboxes = regularFilterOptionsElement
        ? Array.from(regularFilterOptionsElement.querySelectorAll('input[type="checkbox"]'))
        : [];
      if (!checkboxes.length) {
        regularFilterSelectAllInput.checked = false;
        regularFilterSelectAllInput.indeterminate = false;
        regularFilterSelectAllInput.disabled = true;
        return;
      }
      regularFilterSelectAllInput.disabled = false;
      let selectedCount = 0;
      checkboxes.forEach((checkbox) => {
        if (regularFilterSelection.has(checkbox.value)) {
          checkbox.checked = true;
          selectedCount += 1;
        } else {
          checkbox.checked = false;
        }
      });
      if (selectedCount === 0) {
        regularFilterSelectAllInput.checked = false;
        regularFilterSelectAllInput.indeterminate = false;
      } else if (selectedCount === checkboxes.length) {
        regularFilterSelectAllInput.checked = true;
        regularFilterSelectAllInput.indeterminate = false;
      } else {
        regularFilterSelectAllInput.checked = false;
        regularFilterSelectAllInput.indeterminate = true;
      }
    }

    function updateMainFilterSelectAllState() {
      if (!mainFilterSelectAllInput) {
        return;
      }
      const checkboxes = mainFilterOptionsElement
        ? Array.from(mainFilterOptionsElement.querySelectorAll('input[type="checkbox"]'))
        : [];
      if (!checkboxes.length) {
        mainFilterSelectAllInput.checked = false;
        mainFilterSelectAllInput.indeterminate = false;
        mainFilterSelectAllInput.disabled = true;
        return;
      }
      mainFilterSelectAllInput.disabled = false;
      let selectedCount = 0;
      checkboxes.forEach((checkbox) => {
        if (mainFilterSelection.has(checkbox.value)) {
          checkbox.checked = true;
          selectedCount += 1;
        } else {
          checkbox.checked = false;
        }
      });
      if (selectedCount === 0) {
        mainFilterSelectAllInput.checked = false;
        mainFilterSelectAllInput.indeterminate = false;
      } else if (selectedCount === checkboxes.length) {
        mainFilterSelectAllInput.checked = true;
        mainFilterSelectAllInput.indeterminate = false;
      } else {
        mainFilterSelectAllInput.checked = false;
        mainFilterSelectAllInput.indeterminate = true;
      }
    }

    function renderRegularFilterOptions() {
      if (!regularFilterOptionsElement || !Number.isFinite(regularFilterActiveColumnIndex)) {
        return;
      }
      const allOptions = columnValueOptions[regularFilterActiveColumnIndex] || [];
      const normalizedQuery = regularFilterSearchTerm.trim().toLowerCase();
      const filteredOptions = normalizedQuery.length
        ? allOptions.filter((value) => optionLabel(value).toLowerCase().includes(normalizedQuery))
        : allOptions.slice();

      if (!filteredOptions.length) {
        regularFilterOptionsElement.innerHTML = '';
      } else {
        const optionsMarkup = filteredOptions
          .map((value) => {
            const label = optionLabel(value);
            const safeLabel = escapeHtml(label);
            const safeValue = escapeHtml(value);
            const checkedAttr = regularFilterSelection.has(value) ? ' checked' : '';
            return `<label class="regular-filter__option"><input type="checkbox" value="${safeValue}"${checkedAttr}>${safeLabel}</label>`;
          })
          .join('');
        regularFilterOptionsElement.innerHTML = optionsMarkup;
        animateOptionList(regularFilterOptionsElement, '.regular-filter__option');
      }

      if (regularFilterEmptyElement) {
        regularFilterEmptyElement.hidden = filteredOptions.length !== 0;
      }
      updateRegularFilterSelectAllState();
    }

    function renderMainFilterOptions() {
      if (!mainFilterOptionsElement || !Number.isFinite(mainFilterActiveColumnIndex)) {
        return;
      }
      const allOptions = mainColumnValueOptions[mainFilterActiveColumnIndex] || [];
      const normalizedQuery = mainFilterSearchTerm.trim().toLowerCase();
      const filteredOptions = normalizedQuery.length
        ? allOptions.filter((value) => optionLabel(value).toLowerCase().includes(normalizedQuery))
        : allOptions.slice();

      if (!filteredOptions.length) {
        mainFilterOptionsElement.innerHTML = '';
      } else {
        const optionsMarkup = filteredOptions
          .map((value) => {
            const label = optionLabel(value);
            const safeLabel = escapeHtml(label);
            const safeValue = escapeHtml(value);
            const checkedAttr = mainFilterSelection.has(value) ? ' checked' : '';
            return `<label class="regular-filter__option"><input type="checkbox" value="${safeValue}"${checkedAttr}>${safeLabel}</label>`;
          })
          .join('');
        mainFilterOptionsElement.innerHTML = optionsMarkup;
        animateOptionList(mainFilterOptionsElement, '.regular-filter__option');
      }

      if (mainFilterEmptyElement) {
        mainFilterEmptyElement.hidden = filteredOptions.length !== 0;
      }
      updateMainFilterSelectAllState();
    }

    function setRegularFilterColumn(columnIndex) {
      if (!Number.isFinite(columnIndex)) {
        return;
      }
      regularFilterActiveColumnIndex = columnIndex;
      if (regularFilterColumnSelect) {
        regularFilterColumnSelect.value = String(columnIndex);
      }
      syncRegularFilterSelectionFromFilters(columnIndex);
      regularFilterSearchTerm = '';
      if (regularFilterSearchInput) {
        regularFilterSearchInput.value = '';
      }
      renderRegularFilterOptions();
    }

    function setMainFilterColumn(columnIndex) {
      if (!Number.isFinite(columnIndex)) {
        return;
      }
      mainFilterActiveColumnIndex = columnIndex;
      if (mainFilterColumnSelect) {
        mainFilterColumnSelect.value = String(columnIndex);
      }
      syncMainFilterSelectionFromFilters(columnIndex);
      mainFilterSearchTerm = '';
      if (mainFilterSearchInput) {
        mainFilterSearchInput.value = '';
      }
      renderMainFilterOptions();
    }

    function openRegularFilter(triggerButton = null) {
      if (!regularFilterContainerElement || !regularFilterInitialised) {
        return;
      }
      const targets = regularFilterButtons.length
        ? regularFilterButtons.slice()
        : (regularFilterButtonElement ? [regularFilterButtonElement] : []);
      if (triggerButton) {
        activeRegularFilterTrigger = triggerButton;
      } else if (!activeRegularFilterTrigger && targets.length) {
        activeRegularFilterTrigger = targets[0];
      }
      if (activeRegularFilterTrigger && !targets.includes(activeRegularFilterTrigger)) {
        targets.push(activeRegularFilterTrigger);
      }
      if (!Number.isFinite(regularFilterActiveColumnIndex)) {
        const selectedOption = regularFilterColumnSelect && regularFilterColumnSelect.value !== ''
          ? Number(regularFilterColumnSelect.value)
          : null;
        if (Number.isFinite(selectedOption)) {
          setRegularFilterColumn(selectedOption);
        } else if (regularFilterEligibleColumns.length) {
          setRegularFilterColumn(regularFilterEligibleColumns[0].index);
        }
      } else {
        syncRegularFilterSelectionFromFilters(regularFilterActiveColumnIndex);
        renderRegularFilterOptions();
      }
      showFilterContainer(regularFilterContainerElement);
      targets.forEach((button) => {
        if (button) {
          const expanded = button === activeRegularFilterTrigger;
          button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        }
      });
      requestAnimationFrame(() => {
        if (regularFilterSearchInput) {
          regularFilterSearchInput.focus();
        }
      });
    }

    function closeRegularFilter(options = {}) {
      if (!regularFilterContainerElement) {
        return;
      }
      const { returnFocus = true } = options;
      const previousTrigger = activeRegularFilterTrigger;
      hideFilterContainer(regularFilterContainerElement);
      const targets = regularFilterButtons.length
        ? regularFilterButtons
        : (regularFilterButtonElement ? [regularFilterButtonElement] : []);
      targets.forEach((button) => {
        if (button) {
          button.setAttribute('aria-expanded', 'false');
        }
      });
      activeRegularFilterTrigger = null;
      if (returnFocus && previousTrigger && typeof previousTrigger.focus === 'function') {
        previousTrigger.focus();
      }
    }

    function openMainFilter(triggerButton = null) {
      if (!mainFilterContainerElement || !mainFilterInitialised) {
        return;
      }
      const targets = mainFilterButtons.length
        ? mainFilterButtons.slice()
        : (mainFilterButtonElement ? [mainFilterButtonElement] : []);
      if (triggerButton) {
        activeMainFilterTrigger = triggerButton;
      } else if (!activeMainFilterTrigger && targets.length) {
        activeMainFilterTrigger = targets[0];
      }
      if (activeMainFilterTrigger && !targets.includes(activeMainFilterTrigger)) {
        targets.push(activeMainFilterTrigger);
      }
      if (!Number.isFinite(mainFilterActiveColumnIndex)) {
        const selectedOption = mainFilterColumnSelect && mainFilterColumnSelect.value !== ''
          ? Number(mainFilterColumnSelect.value)
          : null;
        if (Number.isFinite(selectedOption)) {
          setMainFilterColumn(selectedOption);
        } else if (mainFilterEligibleColumns.length) {
          setMainFilterColumn(mainFilterEligibleColumns[0].index);
        }
      } else {
        syncMainFilterSelectionFromFilters(mainFilterActiveColumnIndex);
        renderMainFilterOptions();
      }
      showFilterContainer(mainFilterContainerElement);
      targets.forEach((button) => {
        if (button) {
          const expanded = button === activeMainFilterTrigger;
          button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        }
      });
      requestAnimationFrame(() => {
        if (mainFilterSearchInput) {
          mainFilterSearchInput.focus();
        }
      });
    }

    function closeMainFilter(options = {}) {
      if (!mainFilterContainerElement) {
        return;
      }
      const { returnFocus = true } = options;
      const previousTrigger = activeMainFilterTrigger;
      hideFilterContainer(mainFilterContainerElement);
      const targets = mainFilterButtons.length
        ? mainFilterButtons
        : (mainFilterButtonElement ? [mainFilterButtonElement] : []);
      targets.forEach((button) => {
        if (button) {
          button.setAttribute('aria-expanded', 'false');
        }
      });
      activeMainFilterTrigger = null;
      if (returnFocus && previousTrigger && typeof previousTrigger.focus === 'function') {
        previousTrigger.focus();
      }
    }

    function clearAllColumnFilters(table, filters = columnFilters, onChange = handleFilterChange) {
      const hasTable = table && typeof table.column === 'function';
      const filterSource = filters || {};
      const activeIndices = Object.keys(filterSource)
        .map((key) => Number(key))
        .filter((index) => Number.isFinite(index));
      if (!activeIndices.length) {
        if (Object.keys(filterSource).length) {
          if (filters === columnFilters) {
            columnFilters = {};
          } else {
            Object.keys(filterSource).forEach((key) => {
              delete filterSource[key];
            });
          }
          onChange();
        }
        return;
      }
      if (hasTable) {
        activeIndices.forEach((columnIndex) => {
          table.column(columnIndex).search('', false, false);
          const headerCell = table.column(columnIndex).header();
          if (headerCell) {
            headerCell.classList.remove('has-filter');
          }
        });
        table.draw();
      }
      if (filters === columnFilters) {
        columnFilters = {};
      } else {
        Object.keys(filterSource).forEach((key) => {
          delete filterSource[key];
        });
      }
      onChange();
    }

    function buildFilteredDataset(baseDataset, filters, options = {}) {
      if (!baseDataset || !Array.isArray(baseDataset.columns) || !Array.isArray(baseDataset.rows)) {
        return baseDataset;
      }
      const formatOptions = options && typeof options === 'object' ? options.formatOptions : undefined;
      const filterEntries = Object.entries(filters || {})
        .map(([key, values]) => {
          const columnIndex = Number(key);
          if (!Number.isFinite(columnIndex)) {
            return null;
          }
          if (!Array.isArray(values) || values.length === 0) {
            return null;
          }
          return { columnIndex, values };
        })
        .filter(Boolean);
      if (!filterEntries.length) {
        return baseDataset;
      }
      const filteredRows = baseDataset.rows.filter((row) => {
        return filterEntries.every(({ columnIndex, values }) => {
          if (columnIndex < 0 || columnIndex >= baseDataset.columns.length) {
            return true;
          }
          const columnName = baseDataset.columns[columnIndex];
          const formattedValue = formatCellValue(row[columnIndex], columnName, formatOptions);
          return values.includes(formattedValue);
        });
      });
      return {
        columns: baseDataset.columns,
        rows: filteredRows,
      };
    }

    function handleFilterChange() {
      updateRegularFilterButtonState();
      if (!regularDatasetCache || !Array.isArray(regularDatasetCache.rows)) {
        return;
      }
      const animateRegular = regularTableInitialised && Boolean(regularTable);
      const animateLo = loTablesInitialised;
      const animatePlatform = platformTablesInitialised;
      const animateSku = skuSummaryInitialised;
      if (animateRegular) {
        setTabPanelLoading('regular', true, 'Applying filters…');
      }
      if (animateLo) {
        setTabPanelLoading('lo', true, 'Updating view…');
      }
      if (animatePlatform) {
        setTabPanelLoading('platform', true, 'Updating view…');
      }
      if (animateSku) {
        setTabPanelLoading('sku-summary', true, 'Updating summary…');
      }
      const filteredDataset = buildFilteredDataset(regularDatasetCache, columnFilters);
      if (loTablesInitialised) {
        updateLoTablesWithDataset(filteredDataset);
      }
      if (platformTablesInitialised) {
        updatePlatformTablesWithDataset(filteredDataset);
      }
      if (skuSummaryInitialised) {
        updateSkuSummaryWithDataset(filteredDataset);
      }
      requestAnimationFrame(() => {
        if (animateRegular) {
          setTabPanelLoading('regular', false);
        }
        if (animateLo) {
          setTabPanelLoading('lo', false);
        }
        if (animatePlatform) {
          setTabPanelLoading('platform', false);
        }
        if (animateSku) {
          setTabPanelLoading('sku-summary', false);
        }
      });
    }

    function handleMainFilterChange() {
      updateMainFilterButtonState();
      if (mainTableInitialised && mainTable) {
        requestAnimationFrame(() => updateMainTableFooter(mainTable));
      }
      const animateMain = mainTableInitialised && Boolean(mainTable);
      if (animateMain) {
        setTabPanelLoading('main', true, 'Applying filters…');
      }
      setTabPanelLoading('dashboard', true, 'Updating dashboard…');
      const datasetSource = mainDatasetCache
        ? Promise.resolve(mainDatasetCache)
        : fetchMainDataset();
      datasetSource
        .then((dataset) => {
          if (!dataset) {
            return;
          }
          ensureMainFilterSetup(dataset);
          const effectiveDataset = buildFilteredDataset(dataset, mainColumnFilters, {
            formatOptions: MAIN_TABLE_FORMAT_OPTIONS,
          });
          const pivotResults = buildDashboardPivotResultsFromDataset(effectiveDataset);
          if (pivotResults instanceof Map) {
            mainDashboardPivotCache = pivotResults;
            mainDashboardInitialised = true;
            renderMainDashboard(pivotResults);
          }
        })
        .catch((error) => {
          console.error('Failed to update dashboard filters:', error);
        })
        .finally(() => {
          if (animateMain) {
            setTabPanelLoading('main', false);
          }
          setTabPanelLoading('dashboard', false);
        });
    }

    function initializeRegularFilterControls(augmentedDataset) {
      if (regularFilterInitialised) {
        return;
      }
      regularFilterButtonElement = document.getElementById('regular-filter-button');
      regularFilterContainerElement = document.getElementById('regular-filter');
      regularFilterColumnSelect = document.getElementById('regular-filter-column');
      regularFilterSearchInput = document.getElementById('regular-filter-search');
      regularFilterOptionsElement = document.getElementById('regular-filter-options');
      regularFilterEmptyElement = document.getElementById('regular-filter-empty');
      regularFilterSelectAllInput = document.getElementById('regular-filter-select-all');
      regularFilterApplyButton = document.getElementById('regular-filter-apply');
      regularFilterResetButton = document.getElementById('regular-filter-reset');
      regularFilterCloseButton = regularFilterContainerElement
        ? regularFilterContainerElement.querySelector('.regular-filter__close')
        : null;
      const skuFilterButton = document.getElementById('sku-filter-button');
      regularFilterButtons = [regularFilterButtonElement, loFilterButtonElement, platformFilterButtonElement, skuFilterButton].filter((button) => button);
      regularFilterClearButtons = [
        regularFilterClearButtonElement,
        loFilterClearButtonElement,
        platformFilterClearButtonElement,
        skuFilterClearButtonElement,
      ].filter((button) => button);

      const handleRegularClear = () => {
        const tableInstance = regularTableInitialised && regularTable ? regularTable : null;
        clearAllColumnFilters(tableInstance);
        closeRegularFilter({ returnFocus: false });
      };
      regularFilterClearButtons.forEach((button) => {
        button.addEventListener('click', handleRegularClear);
      });

      const elementsReady = [
        regularFilterButtonElement,
        regularFilterContainerElement,
        regularFilterColumnSelect,
        regularFilterSearchInput,
        regularFilterOptionsElement,
        regularFilterEmptyElement,
        regularFilterSelectAllInput,
        regularFilterApplyButton,
        regularFilterResetButton,
        regularFilterCloseButton,
      ].every(Boolean);

      if (!elementsReady) {
        return;
      }

      const filterTargets = regularFilterButtons.length
        ? regularFilterButtons
        : [regularFilterButtonElement];

      const registerFilterButton = (button) => {
        if (!button) {
          return;
        }
        button.addEventListener('click', () => {
          const isVisible = regularFilterContainerElement.classList.contains('is-visible');
          if (isVisible && activeRegularFilterTrigger === button) {
            closeRegularFilter();
          } else {
            openRegularFilter(button);
          }
        });
      };

      filterTargets.forEach(registerFilterButton);

      const columns = Array.isArray(augmentedDataset?.columns)
        ? augmentedDataset.columns
            .map((title, index) => ({
              title: title || `Column ${index + 1}`,
              index,
              options: columnValueOptions[index] || [],
            }))
            .filter((entry) => entry.options.length > 0 && entry.options.length <= REGULAR_FILTER_MAX_UNIQUE_VALUES)
        : [];

      if (!columns.length) {
        filterTargets.forEach((button) => {
          if (button) {
            button.setAttribute('aria-disabled', 'true');
            button.disabled = true;
          }
        });
        return;
      }

      filterTargets.forEach((button) => {
        if (button) {
          button.removeAttribute('aria-disabled');
          button.disabled = false;
        }
      });

      regularFilterEligibleColumns = columns;
      const optionsMarkup = columns
        .map((entry) => `<option value="${entry.index}">${escapeHtml(entry.title)}</option>`)
        .join('');
      regularFilterColumnSelect.innerHTML = optionsMarkup;

      regularFilterCloseButton.addEventListener('click', () => closeRegularFilter());

      regularFilterContainerElement.addEventListener('click', (event) => {
        const target = event.target;
        if (target === regularFilterContainerElement || (target instanceof HTMLElement && target.classList.contains('regular-filter__backdrop'))) {
          closeRegularFilter();
        }
      });

      regularFilterColumnSelect.addEventListener('change', (event) => {
        const selectedValue = Number(event.target.value);
        if (Number.isFinite(selectedValue)) {
          setRegularFilterColumn(selectedValue);
        }
      });

      regularFilterSearchInput.addEventListener('input', (event) => {
        regularFilterSearchTerm = event.target.value || '';
        renderRegularFilterOptions();
      });

      regularFilterOptionsElement.addEventListener('change', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') {
          return;
        }
        const value = target.value;
        if (target.checked) {
          regularFilterSelection.add(value);
        } else {
          regularFilterSelection.delete(value);
        }
        updateRegularFilterSelectAllState();
      });

      regularFilterSelectAllInput.addEventListener('change', (event) => {
        if (!Number.isFinite(regularFilterActiveColumnIndex)) {
          return;
        }
        const selectAll = event.target.checked;
        const allOptions = columnValueOptions[regularFilterActiveColumnIndex] || [];
        if (selectAll) {
          regularFilterSelection = new Set(allOptions);
        } else {
          regularFilterSelection = new Set();
        }
        renderRegularFilterOptions();
      });

      regularFilterApplyButton.addEventListener('click', () => {
        if (!Number.isFinite(regularFilterActiveColumnIndex)) {
          return;
        }
        flashButtonBusy(regularFilterApplyButton);
        const tableInstance = regularTableInitialised && regularTable ? regularTable : null;
        const allOptions = columnValueOptions[regularFilterActiveColumnIndex] || [];
        const selectedValues = Array.from(regularFilterSelection);
        const valuesToApply = selectedValues.length === allOptions.length ? [] : selectedValues;
        const headerCell = tableInstance ? tableInstance.column(regularFilterActiveColumnIndex).header() : null;
        applyColumnFilter(tableInstance, regularFilterActiveColumnIndex, valuesToApply, headerCell);
        closeRegularFilter();
      });

      regularFilterResetButton.addEventListener('click', () => {
        const tableInstance = regularTableInitialised && regularTable ? regularTable : null;
        clearAllColumnFilters(tableInstance);
        if (Number.isFinite(regularFilterActiveColumnIndex)) {
          syncRegularFilterSelectionFromFilters(regularFilterActiveColumnIndex);
          renderRegularFilterOptions();
        }
      });

      document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && regularFilterContainerElement.classList.contains('is-visible')) {
          closeRegularFilter();
        }
      });

      regularFilterInitialised = true;
      updateRegularFilterButtonState();

      const firstColumn = columns[0];
      if (firstColumn) {
        setRegularFilterColumn(firstColumn.index);
      }
    }

    function refreshRegularFilterOptions(augmentedDataset) {
      if (!regularFilterInitialised) {
        return;
      }
      const columns = Array.isArray(augmentedDataset?.columns)
        ? augmentedDataset.columns
            .map((title, index) => ({
              title: title || `Column ${index + 1}`,
              index,
              options: columnValueOptions[index] || [],
            }))
            .filter((entry) => entry.options.length > 0 && entry.options.length <= REGULAR_FILTER_MAX_UNIQUE_VALUES)
        : [];

      regularFilterEligibleColumns = columns;

      const filterTargets = regularFilterButtons.length
        ? regularFilterButtons
        : (regularFilterButtonElement ? [regularFilterButtonElement] : []);

      if (!columns.length) {
        filterTargets.forEach((button) => {
          if (button) {
            button.setAttribute('aria-disabled', 'true');
            button.disabled = true;
          }
        });
        if (regularFilterColumnSelect) {
          regularFilterColumnSelect.innerHTML = '';
        }
        regularFilterActiveColumnIndex = null;
        renderRegularFilterOptions();
        updateRegularFilterButtonState();
        return;
      }

      filterTargets.forEach((button) => {
        if (button) {
          button.removeAttribute('aria-disabled');
          button.disabled = false;
        }
      });

      if (regularFilterColumnSelect) {
        const previousValue = Number(regularFilterColumnSelect.value);
        const optionsMarkup = columns
          .map((entry) => `<option value="${entry.index}">${escapeHtml(entry.title)}</option>`)
          .join('');
        regularFilterColumnSelect.innerHTML = optionsMarkup;
        if (Number.isFinite(previousValue) && columns.some((entry) => entry.index === previousValue)) {
          regularFilterColumnSelect.value = String(previousValue);
          regularFilterActiveColumnIndex = previousValue;
        } else {
          regularFilterActiveColumnIndex = columns[0].index;
          regularFilterColumnSelect.value = String(regularFilterActiveColumnIndex);
        }
      } else {
        regularFilterActiveColumnIndex = columns[0].index;
      }

      if (Number.isFinite(regularFilterActiveColumnIndex)) {
        syncRegularFilterSelectionFromFilters(regularFilterActiveColumnIndex);
      }

      regularFilterSearchTerm = '';
      if (regularFilterSearchInput) {
        regularFilterSearchInput.value = '';
      }

      renderRegularFilterOptions();
      updateRegularFilterSelectAllState();
      updateRegularFilterButtonState();
    }

    function initializeMainFilterControls(augmentedDataset) {
      if (mainFilterInitialised) {
        return;
      }
      mainFilterButtonElement = document.getElementById('main-filter-button');
      mainFilterContainerElement = document.getElementById('main-filter');
      mainFilterColumnSelect = document.getElementById('main-filter-column');
      mainFilterSearchInput = document.getElementById('main-filter-search');
      mainFilterOptionsElement = document.getElementById('main-filter-options');
      mainFilterEmptyElement = document.getElementById('main-filter-empty');
      mainFilterSelectAllInput = document.getElementById('main-filter-select-all');
      mainFilterApplyButton = document.getElementById('main-filter-apply');
      mainFilterResetButton = document.getElementById('main-filter-reset');
      mainFilterCloseButton = mainFilterContainerElement
        ? mainFilterContainerElement.querySelector('.regular-filter__close')
        : null;
      mainFilterButtons = mainFilterButtonElement ? [mainFilterButtonElement] : [];
      mainFilterClearButtons = mainFilterClearButtonElement ? [mainFilterClearButtonElement] : [];

      const handleMainClear = () => {
        const tableInstance = mainTableInitialised && mainTable ? mainTable : null;
        clearAllColumnFilters(tableInstance, mainColumnFilters, handleMainFilterChange);
        closeMainFilter({ returnFocus: false });
      };
      mainFilterClearButtons.forEach((button) => {
        button.addEventListener('click', handleMainClear);
      });

      const elementsReady = [
        mainFilterButtonElement,
        mainFilterContainerElement,
        mainFilterColumnSelect,
        mainFilterSearchInput,
        mainFilterOptionsElement,
        mainFilterEmptyElement,
        mainFilterSelectAllInput,
        mainFilterApplyButton,
        mainFilterResetButton,
        mainFilterCloseButton,
      ].every(Boolean);

      if (!elementsReady) {
        return;
      }

      const filterTargets = mainFilterButtons.length
        ? mainFilterButtons
        : [mainFilterButtonElement];

      const registerFilterButton = (button) => {
        if (!button) {
          return;
        }
        button.addEventListener('click', () => {
          const isVisible = mainFilterContainerElement.classList.contains('is-visible');
          if (isVisible && activeMainFilterTrigger === button) {
            closeMainFilter();
          } else {
            openMainFilter(button);
          }
        });
      };

      filterTargets.forEach(registerFilterButton);

      const columns = Array.isArray(augmentedDataset?.columns)
        ? augmentedDataset.columns
            .map((title, index) => ({
              title: title || `Column ${index + 1}`,
              index,
              options: mainColumnValueOptions[index] || [],
            }))
            .filter((entry) => entry.options.length > 0 && entry.options.length <= REGULAR_FILTER_MAX_UNIQUE_VALUES)
        : [];

      if (!columns.length) {
        filterTargets.forEach((button) => {
          if (button) {
            button.setAttribute('aria-disabled', 'true');
            button.disabled = true;
          }
        });
        return;
      }

      filterTargets.forEach((button) => {
        if (button) {
          button.removeAttribute('aria-disabled');
          button.disabled = false;
        }
      });

      mainFilterEligibleColumns = columns;
      const optionsMarkup = columns
        .map((entry) => `<option value="${entry.index}">${escapeHtml(entry.title)}</option>`)
        .join('');
      mainFilterColumnSelect.innerHTML = optionsMarkup;

      mainFilterCloseButton.addEventListener('click', () => closeMainFilter());
      mainFilterContainerElement.addEventListener('click', (event) => {
        const target = event.target;
        if (target === mainFilterContainerElement || (target instanceof HTMLElement && target.classList.contains('regular-filter__backdrop'))) {
          closeMainFilter();
        }
      });

      mainFilterColumnSelect.addEventListener('change', (event) => {
        const selectedValue = Number(event.target.value);
        if (Number.isFinite(selectedValue)) {
          setMainFilterColumn(selectedValue);
        }
      });

      mainFilterSearchInput.addEventListener('input', (event) => {
        mainFilterSearchTerm = event.target.value || '';
        renderMainFilterOptions();
      });

      mainFilterOptionsElement.addEventListener('change', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') {
          return;
        }
        if (!mainFilterSelection) {
          mainFilterSelection = new Set();
        }
        if (target.checked) {
          mainFilterSelection.add(target.value);
        } else {
          mainFilterSelection.delete(target.value);
        }
        updateMainFilterSelectAllState();
      });

      mainFilterSelectAllInput.addEventListener('change', (event) => {
        if (!Number.isFinite(mainFilterActiveColumnIndex)) {
          return;
        }
        const selectAll = event.target.checked;
        const allOptions = mainColumnValueOptions[mainFilterActiveColumnIndex] || [];
        if (selectAll) {
          mainFilterSelection = new Set(allOptions);
        } else {
          mainFilterSelection = new Set();
        }
        renderMainFilterOptions();
      });

      mainFilterApplyButton.addEventListener('click', () => {
        if (!Number.isFinite(mainFilterActiveColumnIndex)) {
          return;
        }
        flashButtonBusy(mainFilterApplyButton);
        const tableInstance = mainTableInitialised && mainTable ? mainTable : null;
        const allOptions = mainColumnValueOptions[mainFilterActiveColumnIndex] || [];
        const selectedValues = Array.from(mainFilterSelection);
        const valuesToApply = selectedValues.length === allOptions.length ? [] : selectedValues;
        const headerCell = tableInstance ? tableInstance.column(mainFilterActiveColumnIndex).header() : null;
        applyColumnFilter(tableInstance, mainFilterActiveColumnIndex, valuesToApply, headerCell, {
          filters: mainColumnFilters,
          onChange: handleMainFilterChange,
        });
        closeMainFilter();
      });

      mainFilterResetButton.addEventListener('click', () => {
        const tableInstance = mainTableInitialised && mainTable ? mainTable : null;
        clearAllColumnFilters(tableInstance, mainColumnFilters, handleMainFilterChange);
        if (Number.isFinite(mainFilterActiveColumnIndex)) {
          syncMainFilterSelectionFromFilters(mainFilterActiveColumnIndex);
          renderMainFilterOptions();
        }
      });

      document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && mainFilterContainerElement.classList.contains('is-visible')) {
          closeMainFilter();
        }
      });

      mainFilterInitialised = true;
      updateMainFilterButtonState();

      const firstColumn = columns[0];
      if (firstColumn) {
        setMainFilterColumn(firstColumn.index);
      }
    }

    function ensureMainFilterSetup(dataset) {
      if (!dataset || !Array.isArray(dataset.columns)) {
        return;
      }
      if (!mainTableAugmentedDataset) {
        mainTableAugmentedDataset = augmentDatasetWithTotals(dataset);
      }
      const augmented = mainTableAugmentedDataset;
      if (!augmented || !Array.isArray(augmented.columns) || !augmented.columns.length) {
        return;
      }
      if (!Array.isArray(mainColumnValueOptions) || !mainColumnValueOptions.length) {
        mainColumnValueOptions = buildColumnOptions(augmented, { formatOptions: MAIN_TABLE_FORMAT_OPTIONS });
      }
      if (!mainFilterInitialised) {
        initializeMainFilterControls(augmented);
      }
    }

    function escapeRegex(value) {
      return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    function optionLabel(value) {
      const normalized = value === null || value === undefined ? '' : String(value);
      return normalized === '' ? '(Blank)' : normalized;
    }

    function escapeHtml(value) {
      return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function applyColumnFilter(table, columnIndex, values, headerCell, options = {}) {
      if (!Number.isFinite(columnIndex)) {
        return;
      }
      const safeValues = Array.isArray(values) ? values.slice() : [];
      const hasTable = table && typeof table.column === 'function';
      const filters = options.filters || columnFilters;
      const onChange = typeof options.onChange === 'function' ? options.onChange : handleFilterChange;
      if (safeValues.length === 0) {
        if (hasTable) {
          table.column(columnIndex).search('', false, false).draw();
        }
        if (filters && typeof filters === 'object') {
          delete filters[columnIndex];
        }
        if (headerCell) {
          headerCell.classList.remove('has-filter');
        } else if (hasTable) {
          const tableHeaderCell = table.column(columnIndex).header();
          if (tableHeaderCell) {
            tableHeaderCell.classList.remove('has-filter');
          }
        }
        onChange();
        return;
      }
      if (hasTable) {
        const regex = `^(${safeValues.map((value) => escapeRegex(value)).join('|')})$`;
        table.column(columnIndex).search(regex, true, false).draw();
      }
      if (filters && typeof filters === 'object') {
        filters[columnIndex] = safeValues;
      }
      if (headerCell) {
        headerCell.classList.add('has-filter');
      } else if (hasTable) {
        const tableHeaderCell = table.column(columnIndex).header();
        if (tableHeaderCell) {
          tableHeaderCell.classList.add('has-filter');
        }
      }
      onChange();
    }

    function positionHeaderMenu(headerCell) {
      if (!headerMenuElement) return;
      const rect = headerCell.getBoundingClientRect();
      const top = rect.bottom + window.scrollY + 8;
      const left = rect.left + window.scrollX;
      headerMenuElement.style.top = `${top}px`;
      headerMenuElement.style.left = `${left}px`;
    }

    function openHeaderMenu(headerCell, table, options = {}) {
      ensureHeaderMenu();
      if (!headerMenuElement) return;

      activeHeaderCell = headerCell;
      const dataTableIndexAttr = headerCell.getAttribute('data-dt-column');
      if (dataTableIndexAttr !== null && dataTableIndexAttr !== '') {
        activeColumnIndex = Number(dataTableIndexAttr);
      } else if (headerCell.dataset.columnIndex) {
        activeColumnIndex = Number(headerCell.dataset.columnIndex);
      } else {
        activeColumnIndex = headerCell.cellIndex ?? 0;
      }
      const columnTitle = headerCell.textContent.trim();
      const valueOptions = Array.isArray(options.valueOptions) ? options.valueOptions : columnValueOptions;
      const filters = options.filters || columnFilters;
      const onChange = typeof options.onChange === 'function' ? options.onChange : handleFilterChange;
      const optionsForColumn = Array.isArray(valueOptions) ? valueOptions[activeColumnIndex] || [] : [];
      const selectedValues = filters[activeColumnIndex] ? [...filters[activeColumnIndex]] : [];
      const sortingEnabled = options.allowSorting !== false;

      const optionsMarkup = optionsForColumn.map((value) => {
        const checked = selectedValues.includes(value) ? 'checked' : '';
        const rawLabel = optionLabel(value);
        const safeLabel = escapeHtml(rawLabel);
        const safeValueAttr = escapeHtml(value);
        const dataLabel = escapeHtml(rawLabel.toLowerCase());
        return `<label class="header-menu__option" data-label="${dataLabel}"><input type="checkbox" value="${safeValueAttr}" ${checked}>${safeLabel}</label>`;
      }).join('');
      const hasOptions = optionsForColumn.length > 0;

      const sortControlsMarkup = sortingEnabled
        ? `
        <div class="header-menu__section">
          <div class="header-menu__buttons">
            <button type="button" class="header-menu__button" data-sort="asc">Sort ascending</button>
            <button type="button" class="header-menu__button" data-sort="desc">Sort descending</button>
          </div>
        </div>`
        : '';

      headerMenuElement.innerHTML = `
        <div class="header-menu__header">
          <h3 class="header-menu__title">${columnTitle}</h3>
          <button type="button" class="header-menu__close" aria-label="Close menu">&times;</button>
        </div>
        ${sortControlsMarkup}
        <div class="header-menu__section">
          <label for="header-menu-search" class="sr-only">Search values</label>
          <input id="header-menu-search" class="header-menu__search" type="search" placeholder="Search values" autocomplete="off">
        </div>
        <div class="header-menu__section">
          <div class="header-menu__options" role="group" aria-label="Filter values">
            ${hasOptions ? optionsMarkup : ''}
          </div>
          ${hasOptions ? '<div class="header-menu__empty-message" hidden>No matches found</div>' : '<p style="margin:0.5rem 0 0;color:var(--muted);font-size:0.85rem;">No values available</p>'}
        </div>
        <div class="header-menu__footer">
          <button type="button" class="header-menu__clear">Clear</button>
          <button type="button" class="header-menu__apply">Apply</button>
        </div>
      `;

      animateOptionList(headerMenuElement.querySelector('.header-menu__options'), '.header-menu__option');

      headerMenuElement.classList.remove('hidden');
      positionHeaderMenu(headerCell);

      const closeButton = headerMenuElement.querySelector('.header-menu__close');
      closeButton?.addEventListener('click', () => closeHeaderMenu());

      const sortButtons = headerMenuElement.querySelectorAll('[data-sort]');
      sortButtons.forEach((button) => {
        button.addEventListener('click', (event) => {
          const direction = event.currentTarget.getAttribute('data-sort');
          table.order([activeColumnIndex, direction]).draw();
          closeHeaderMenu();
        });
      });

      const optionsContainer = headerMenuElement.querySelector('.header-menu__options');
      const searchInput = headerMenuElement.querySelector('#header-menu-search');
      const emptyMessage = headerMenuElement.querySelector('.header-menu__empty-message');
      if (!hasOptions && searchInput) {
        searchInput.disabled = true;
        searchInput.placeholder = 'No values available';
      }
      searchInput?.addEventListener('input', (event) => {
        const query = event.currentTarget.value.trim().toLowerCase();
        const labels = optionsContainer ? optionsContainer.querySelectorAll('.header-menu__option') : [];
        let visibleCount = 0;
        labels.forEach((labelEl) => {
          const labelValue = labelEl.getAttribute('data-label') || '';
          const isVisible = labelValue.includes(query);
          labelEl.style.display = isVisible ? 'flex' : 'none';
          if (isVisible) {
            visibleCount += 1;
          }
        });
        if (emptyMessage) {
          emptyMessage.hidden = visibleCount !== 0;
        }
      });

      const applyButton = headerMenuElement.querySelector('.header-menu__apply');
      applyButton?.addEventListener('click', () => {
        const checkedInputs = optionsContainer ? Array.from(optionsContainer.querySelectorAll('input[type="checkbox"]')).filter((input) => input.checked) : [];
        const values = checkedInputs.map((input) => input.value);
        applyColumnFilter(table, activeColumnIndex, values, headerCell, { filters, onChange });
        closeHeaderMenu();
      });

      const clearButton = headerMenuElement.querySelector('.header-menu__clear');
      clearButton?.addEventListener('click', () => {
        if (optionsContainer) {
          optionsContainer.querySelectorAll('input[type="checkbox"]').forEach((input) => {
            input.checked = false;
          });
        }
        applyColumnFilter(table, activeColumnIndex, [], headerCell, { filters, onChange });
        closeHeaderMenu();
      });
    }

    function buildColumnOptions(dataset, options = {}) {
      const formatOptions = options && typeof options === 'object' ? options.formatOptions : undefined;
      const sets = dataset.columns.map(() => new Set());
      dataset.rows.forEach((row) => {
        row.forEach((value, index) => {
          const columnName = dataset.columns[index];
          const formatted = formatCellValue(value, columnName, formatOptions);
          sets[index].add(formatted);
        });
      });
      return sets.map((set) => Array.from(set).sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' })));
    }

    function wireHeaderEvents(table, options = {}) {
      const container = table.table().container();
      const headerCells = Array.from(container.querySelectorAll('thead th'));

      headerCells.forEach((cell, index) => {
        if (!cell.dataset.columnIndex) {
          cell.dataset.columnIndex = String(index);
        }
        cell.classList.add('is-filterable');
        cell.classList.remove('is-static');
        cell.style.cursor = '';
        cell.removeAttribute('aria-disabled');
      });

      $(headerCells).off('click.DT keypress.DT');
      headerCells.forEach((cell, index) => {
        const existingHandler = headerClickHandlers.get(cell);
        if (existingHandler) {
          cell.removeEventListener('click', existingHandler);
          headerClickHandlers.delete(cell);
        }
        const totalIndex = Number.isFinite(options.totalIndex) ? options.totalIndex : totalColumnIndex;
        const isTotalColumn = Number.isFinite(totalIndex) && totalIndex >= 0 && index === totalIndex;
        if (isTotalColumn) {
          return;
        }
        const handler = (event) => {
          event.preventDefault();
          event.stopPropagation();
          openHeaderMenu(cell, table, options);
        };
        headerClickHandlers.set(cell, handler);
        cell.addEventListener('click', handler);
      });
    }

    function isPlaceholderValue(value) {
      if (value === null || value === undefined) {
        return true;
      }
      const normalized = typeof value === 'string' ? value.trim() : value;
      return normalized === '' || normalized === '-' || normalized === '--';
    }

    function parseNumericValue(value) {
      if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
      }
      if (typeof value === 'string') {
        const trimmed = value.trim();
        if (isPlaceholderValue(trimmed)) {
          return null;
        }
        const numeric = Number(trimmed.replace(/,/g, ''));
        return Number.isNaN(numeric) ? null : numeric;
      }
      return null;
    }

    function parseExcelSerialToDate(value) {
      if (typeof window.toYMD === 'function') {
        const ymd = window.toYMD(value);
        if (ymd) {
          const date = new Date(Date.UTC(ymd.y, ymd.m - 1, ymd.d));
          return Number.isNaN(date.getTime()) ? null : date;
        }
      }
      if (value instanceof Date && !Number.isNaN(value.getTime())) {
        const date = new Date(Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()));
        return Number.isNaN(date.getTime()) ? null : date;
      }
      return null;
    }

    function formatExcelSerialDate(value) {
      const formatted = formatDateValue(value);
      return formatted || null;
    }

    function detectNumericColumns(dataset) {
      return dataset.columns.reduce((accumulator, column, columnIndex) => {
        const normalizedColumn = column ? column.toUpperCase().trim() : '';
        if (NUMERIC_COLUMN_EXCLUSIONS.has(normalizedColumn)) {
          return accumulator;
        }
        let hasNumeric = false;
        const isNumericColumn = dataset.rows.every((row) => {
          const value = row[columnIndex];
          if (isPlaceholderValue(value)) {
            return true;
          }
          const numericValue = parseNumericValue(value);
          if (numericValue === null) {
            return false;
          }
          hasNumeric = true;
          return true;
        });
        if (isNumericColumn && hasNumeric) {
          accumulator.push(columnIndex);
        }
        return accumulator;
      }, []);
    }

    function formatCellValue(value, columnName, options = {}) {
      const effectiveOptions = options && typeof options === 'object' ? options : {};
      const { skipDateFormatting = false } = effectiveOptions;
      if (isPlaceholderValue(value)) {
        return typeof value === 'string' ? value.trim() : '';
      }
      const normalizedColumn = columnName ? columnName.trim().toLowerCase() : '';
      if (!skipDateFormatting && isDateColumnName(columnName)) {
        const formattedDate = formatDateValue(value);
        if (formattedDate) {
          return formattedDate;
        }
      }
      const numericValue = parseNumericValue(value);
      if (numericValue !== null) {
        const decimals = ZERO_DECIMAL_COLUMNS.has(normalizedColumn) ? 0 : 2;
        return numericValue.toFixed(decimals);
      }
      return typeof value === 'string' ? value : String(value ?? '');
    }

    const RATIO_EPSILON = 1e-9;

    function getRatioMultiplier(computedConfig) {
      if (!computedConfig || typeof computedConfig !== 'object') {
        return 100;
      }
      const { multiplier, asPercentage } = computedConfig;
      if (typeof multiplier === 'number' && Number.isFinite(multiplier)) {
        return multiplier;
      }
      if (asPercentage === false) {
        return 1;
      }
      return 100;
    }

    function formatDashboardRatioValue(metric, columnConfig) {
      if (!metric) {
        return '';
      }
      const numeratorHasValue = Boolean(metric.numeratorHasValue);
      const denominatorHasValue = Boolean(metric.denominatorHasValue);
      if (!numeratorHasValue && !denominatorHasValue) {
        return '';
      }
      const denominator = typeof metric.denominator === 'number' ? metric.denominator : 0;
      if (!denominatorHasValue || Math.abs(denominator) < RATIO_EPSILON) {
        return numeratorHasValue ? '#DIV/0!' : '';
      }
      const numerator = typeof metric.numerator === 'number' ? metric.numerator : 0;
      const multiplier = getRatioMultiplier(columnConfig?.computed);
      const ratioValue = (numerator / denominator) * multiplier;
      const columnName = columnConfig?.header || columnConfig?.source;
      return formatCellValue(ratioValue, columnName);
    }

    function augmentDatasetWithTotals(dataset) {
      if (!dataset || !Array.isArray(dataset.columns) || !Array.isArray(dataset.rows)) {
        return {
          columns: [],
          rows: [],
          totalsRow: [],
          numericColumnIndices: [],
          totalColumnIndex: -1,
        };
      }

      const baseColumns = dataset.columns.slice();
      const numericColumnIndices = detectNumericColumns(dataset);
      const totalsByColumn = new Map();
      numericColumnIndices.forEach((index) => totalsByColumn.set(index, 0));

      const augmentedRows = dataset.rows.map((row) => {
        const newRow = row.slice();
        numericColumnIndices.forEach((columnIndex) => {
          const numericValue = parseNumericValue(row[columnIndex]);
          if (numericValue === null) {
            return;
          }
          totalsByColumn.set(columnIndex, (totalsByColumn.get(columnIndex) ?? 0) + numericValue);
        });
        return newRow;
      });

      const totalsRow = new Array(baseColumns.length).fill('');
      if (totalsRow.length > 0) {
        totalsRow[0] = TOTAL_ROW_LABEL;
      }

      numericColumnIndices.forEach((columnIndex) => {
        const total = totalsByColumn.get(columnIndex);
        if (typeof total === 'number' && Number.isFinite(total)) {
          totalsRow[columnIndex] = total;
        } else {
          totalsRow[columnIndex] = 0;
        }
      });

      const augmented = {
        columns: baseColumns,
        rows: augmentedRows,
        totalsRow,
        numericColumnIndices,
        totalColumnIndex: -1,
      };
      if (dataset.headerPreamble && Array.isArray(dataset.headerPreamble.rows)) {
        augmented.headerPreamble = dataset.headerPreamble;
      }
      return augmented;
    }

    function buildFormattedFooterValues(augmentedDataset, options = {}) {
      if (!augmentedDataset || !Array.isArray(augmentedDataset.columns) || !Array.isArray(augmentedDataset.totalsRow)) {
        return [];
      }
      const numericColumns = new Set(augmentedDataset.numericColumnIndices || []);
      const formatOptions = options && typeof options === 'object' ? options.formatOptions : undefined;
      return augmentedDataset.totalsRow.map((value, index) => {
        if (index === 0) {
          const label = typeof value === 'string' && value.trim().length ? value : TOTAL_ROW_LABEL;
          return label;
        }
        if (numericColumns.has(index)) {
          return formatCellValue(value, augmentedDataset.columns[index], formatOptions);
        }
        if (value === null || value === undefined) {
          return '';
        }
        return typeof value === 'string' ? value : String(value);
      });
    }

    function calculateRegularTableFooterValues(table) {
      if (!table || !regularTableAugmentedDataset) {
        return regularTableFooterValues;
      }

      const columns = Array.isArray(regularTableAugmentedDataset.columns)
        ? regularTableAugmentedDataset.columns
        : [];
      const columnCount = columns.length;
      if (columnCount === 0) {
        return [];
      }

      const baseValues = buildFormattedFooterValues(regularTableAugmentedDataset);
      const values = Array.isArray(baseValues) && baseValues.length === columnCount
        ? baseValues.slice()
        : (() => {
            const fallback = new Array(columnCount).fill('');
            const totalsRow = Array.isArray(regularTableAugmentedDataset.totalsRow)
              ? regularTableAugmentedDataset.totalsRow
              : [];
            const labelValue = totalsRow[0];
            if (typeof labelValue === 'string' && labelValue.trim().length) {
              fallback[0] = labelValue;
            } else {
              fallback[0] = TOTAL_ROW_LABEL;
            }
            return fallback;
          })();

      if (!regularTableNumericColumnSet || regularTableNumericColumnSet.size === 0) {
        return values;
      }

      const filteredRows = table.rows({ search: 'applied' }).data().toArray();

      regularTableNumericColumnSet.forEach((columnIndex) => {
        if (typeof columnIndex !== 'number' || columnIndex < 0 || columnIndex >= columnCount) {
          return;
        }
        let sum = 0;
        let hasValue = false;
        filteredRows.forEach((row) => {
          if (!row || columnIndex >= row.length) {
            return;
          }
          const numericValue = parseNumericValue(row[columnIndex]);
          if (numericValue !== null) {
            sum += numericValue;
            hasValue = true;
          }
        });

        const formattedTotal = hasValue
          ? formatCellValue(sum, columns[columnIndex])
          : formatCellValue(0, columns[columnIndex]);
        values[columnIndex] = formattedTotal;
      });

      return values;
    }

    function updateRegularTableFooter(table) {
      if (!SHOW_REGULAR_TOTAL_ROW || !table) {
        return;
      }
      regularTableFooterValues = calculateRegularTableFooterValues(table);
      renderFooterRow(table, regularTableFooterValues, regularTableNumericColumnSet, totalColumnIndex);
    }

    function findColumnIndex(dataset, targetName) {
      if (!dataset || !Array.isArray(dataset.columns)) {
        return -1;
      }
      const normalizedTarget = targetName ? targetName.trim().toLowerCase() : '';
      return dataset.columns.findIndex((column) => (column || '').trim().toLowerCase() === normalizedTarget);
    }

    function formatTwoDecimal(value) {
      const numeric = Number.isFinite(value) ? value : 0;
      return numberFormatter.format(numeric);
    }

    function buildDailyPivot(dataset, dimensionColumnName, targetColumnName, options = {}) {
      if (!dataset || !Array.isArray(dataset.rows)) {
        return { loList: [], rows: [], totals: [], order: [], displayNames: new Map() };
      }

      const checkoutIndex = findColumnIndex(dataset, 'checkout');
      const dimensionIndex = findColumnIndex(dataset, dimensionColumnName);
      const valueIndex = findColumnIndex(dataset, targetColumnName);

      if (checkoutIndex < 0 || dimensionIndex < 0 || valueIndex < 0) {
        return { loList: [], rows: [], totals: [], order: [], displayNames: new Map() };
      }

      const normalizedOrder = Array.isArray(options.normalizedOrder) ? options.normalizedOrder : null;
      const normalizedDisplayOverrides = options.displayNameOverrides instanceof Map
        ? options.displayNameOverrides
        : (options.displayNameOverrides && typeof options.displayNameOverrides === 'object'
          ? new Map(Object.entries(options.displayNameOverrides))
          : new Map());
      const fallbackLabel = typeof options.fallbackLabel === 'string' && options.fallbackLabel.trim().length
        ? options.fallbackLabel.trim()
        : 'Unassigned';

      const dimensionLabels = new Map();
      const dimensionTotals = new Map();
      const dateMap = new Map();

      dataset.rows.forEach((row) => {
        const date = parseExcelSerialToDate(row[checkoutIndex]);
        if (!date) {
          return;
        }
        const isoKey = date.toISOString().slice(0, 10);
        const rawLabel = row[dimensionIndex];
        let label = '';
        if (typeof rawLabel === 'string') {
          label = rawLabel.trim();
        } else if (rawLabel === null || rawLabel === undefined) {
          label = '';
        } else {
          label = String(rawLabel).trim();
        }
        const effectiveLabel = label || fallbackLabel;
        const normalizedLabel = effectiveLabel.toLocaleLowerCase();
        const displayName = normalizedDisplayOverrides.get(normalizedLabel) ?? effectiveLabel;

        const rawValue = row[valueIndex];
        const numericValue = parseNumericValue(rawValue);
        const value = numericValue === null ? 0 : numericValue;

        if (!dimensionLabels.has(normalizedLabel)) {
          dimensionLabels.set(normalizedLabel, displayName);
        }
        dimensionTotals.set(normalizedLabel, (dimensionTotals.get(normalizedLabel) ?? 0) + value);
        if (!dateMap.has(isoKey)) {
          dateMap.set(isoKey, { date, values: new Map() });
        }
        const entry = dateMap.get(isoKey);
        entry.values.set(normalizedLabel, (entry.values.get(normalizedLabel) ?? 0) + value);
      });

      if (dimensionLabels.size === 0) {
        return { loList: [], rows: [], totals: [], order: [], displayNames: new Map() };
      }

      let dimensionOrder;
      if (normalizedOrder && normalizedOrder.length) {
        const orderSet = new Set();
        dimensionOrder = normalizedOrder.filter((key) => {
          if (!dimensionLabels.has(key) || orderSet.has(key)) {
            return false;
          }
          orderSet.add(key);
          return true;
        });
        const remainingDimensions = Array.from(dimensionLabels.keys()).filter((key) => !orderSet.has(key));
        remainingDimensions.sort((a, b) => {
          const totalDiff = (dimensionTotals.get(b) ?? 0) - (dimensionTotals.get(a) ?? 0);
          if (Math.abs(totalDiff) > Number.EPSILON) {
            return totalDiff;
          }
          const displayA = dimensionLabels.get(a) ?? '';
          const displayB = dimensionLabels.get(b) ?? '';
          return displayA.localeCompare(displayB, undefined, { sensitivity: 'base' });
        });
        dimensionOrder.push(...remainingDimensions);
      } else {
        dimensionOrder = Array.from(dimensionLabels.keys());
        dimensionOrder.sort((a, b) => {
          const totalDiff = (dimensionTotals.get(b) ?? 0) - (dimensionTotals.get(a) ?? 0);
          if (Math.abs(totalDiff) > Number.EPSILON) {
            return totalDiff;
          }
          const displayA = dimensionLabels.get(a) ?? '';
          const displayB = dimensionLabels.get(b) ?? '';
          return displayA.localeCompare(displayB, undefined, { sensitivity: 'base' });
        });
      }

      const loList = dimensionOrder.map((dimensionKey) => dimensionLabels.get(dimensionKey) ?? dimensionKey);
      const rows = Array.from(dateMap.values())
        .sort((a, b) => a.date - b.date)
        .map(({ date, values }) => {
          const displayDate = displayDateFormatter.format(date);
          const formattedValues = dimensionOrder.map((dimensionKey) => formatTwoDecimal(values.get(dimensionKey) ?? 0));
          return { displayDate, formattedValues };
        });

      const totals = dimensionOrder.map((dimensionKey) => formatTwoDecimal(dimensionTotals.get(dimensionKey) ?? 0));

      return {
        loList,
        rows,
        totals,
        order: dimensionOrder,
        displayNames: new Map(dimensionLabels),
      };
    }

    function buildLoPivot(dataset, targetColumnName, options = {}) {
      const mergedOptions = {
        ...options,
      };
      if (!Object.prototype.hasOwnProperty.call(mergedOptions, 'fallbackLabel')) {
        mergedOptions.fallbackLabel = 'Unassigned';
      }
      return buildDailyPivot(dataset, 'listing owner', targetColumnName, mergedOptions);
    }

    function renderLoTable(tableElement, pivotData) {
      if (!tableElement) {
        return;
      }
      const { loList, rows, totals } = pivotData;
      if (!loList.length || !rows.length) {
        const columnCount = Math.max(1, loList.length + 1);
        tableElement.innerHTML = `<tbody><tr><td class="cell-date" colspan="${columnCount}">No data available</td></tr></tbody>`;
        return;
      }

      let headerHtml = '<thead><tr><th scope="col" class="cell-date">Date</th>';
      loList.forEach((owner) => {
        headerHtml += `<th scope="col">${escapeHtml(owner)}</th>`;
      });
      headerHtml += '</tr></thead>';

      let bodyHtml = '<tbody>';
      rows.forEach(({ displayDate, formattedValues }) => {
        bodyHtml += `<tr><td class="cell-date">${escapeHtml(displayDate)}</td>`;
        formattedValues.forEach((value) => {
          bodyHtml += `<td>${value}</td>`;
        });
        bodyHtml += '</tr>';
      });
      bodyHtml += '</tbody>';

      let footerHtml = '';
      if (Array.isArray(totals) && totals.length === loList.length) {
        footerHtml = '<tfoot><tr>';
        footerHtml += '<th scope="row" class="cell-date">Total</th>';
        totals.forEach((value) => {
          footerHtml += `<td>${value}</td>`;
        });
        footerHtml += '</tr></tfoot>';
      }

      tableElement.innerHTML = `${headerHtml}${bodyHtml}${footerHtml}`;
    }

    function getSalesGapFilterConfigByDatasetKey(datasetKey) {
      return SALES_GAP_FILTER_CONFIGS.find((config) => config.datasetKey === datasetKey) || null;
    }

    function hasActiveSalesGapFilters(config) {
      if (!config || !config.filters) {
        return false;
      }
      return Object.values(config.filters).some((values) => Array.isArray(values) && values.length > 0);
    }

    function updateSalesGapFilterButtonState(config) {
      if (!config) {
        return;
      }
      const hasFilterableColumns = Array.isArray(config.availableColumns) && config.availableColumns.length > 0;
      const hasFilters = hasActiveSalesGapFilters(config);
      const button = config.filterButton || null;
      if (button) {
        if (hasFilterableColumns) {
          button.disabled = false;
          button.removeAttribute('aria-disabled');
        } else {
          button.disabled = true;
          button.setAttribute('aria-disabled', 'true');
        }
        button.setAttribute('data-active', hasFilters ? 'true' : 'false');
      }
      const clearButton = config.filterClearButton || null;
      if (clearButton) {
        clearButton.hidden = !hasFilters;
        clearButton.disabled = !hasFilters;
      }
    }

    function renderSalesGapFilterFieldButtons(config) {
      const container = config?.filterFieldOptionsElement;
      if (!container) {
        return;
      }
      const columns = Array.isArray(config.availableColumns) ? config.availableColumns : [];
      if (!columns.length) {
        container.innerHTML = '<p class="regular-filter__empty" style="margin:0">No filterable fields</p>';
        return;
      }
      const markup = columns
        .map((column) => {
          const pressed = column.index === config.activeColumnIndex ? ' aria-pressed="true"' : ' aria-pressed="false"';
          const label = escapeHtml(column.title || `Column ${column.index + 1}`);
          return `<button type="button" class="regular-filter__field-button" data-column-index="${column.index}"${pressed}>${label}</button>`;
        })
        .join('');
      container.innerHTML = markup;
      const buttons = Array.from(container.querySelectorAll('.regular-filter__field-button'));
      buttons.forEach((buttonElement) => {
        buttonElement.addEventListener('click', () => {
          const targetIndex = Number(buttonElement.dataset.columnIndex);
          setSalesGapFilterActiveColumn(config, targetIndex);
        });
      });
    }

    function syncSalesGapPendingSelections(config) {
      if (!config) {
        return;
      }
      if (!(config.pendingSelections instanceof Map)) {
        config.pendingSelections = new Map();
      }
      const pending = config.pendingSelections;
      const optionsSource = Array.isArray(config.columnValueOptions) ? config.columnValueOptions : [];
      if (config.filters && typeof config.filters === 'object') {
        Object.entries(config.filters).forEach(([key, values]) => {
          const columnIndex = Number(key);
          if (!Number.isFinite(columnIndex)) {
            return;
          }
          const availableValues = optionsSource[columnIndex] || [];
          const validValues = Array.isArray(values) ? values.filter((value) => availableValues.includes(value)) : [];
          if (validValues.length) {
            pending.set(columnIndex, new Set(validValues));
          }
        });
      }
      if (Number.isFinite(config.activeColumnIndex) && !pending.has(config.activeColumnIndex)) {
        pending.set(config.activeColumnIndex, new Set());
      }
    }

    function renderSalesGapFilterOptions(config) {
      const container = config?.filterOptionsElement;
      if (!container) {
        return;
      }
      const emptyMessage = config.filterEmptyElement || null;
      const columnIndex = Number.isFinite(config?.activeColumnIndex) ? config.activeColumnIndex : null;
      const optionsSource = Array.isArray(config.columnValueOptions) ? config.columnValueOptions : [];
      const options = Number.isFinite(columnIndex) ? optionsSource[columnIndex] || [] : [];
      syncSalesGapPendingSelections(config);
      const pending = config.pendingSelections instanceof Map ? config.pendingSelections : new Map();
      const selectionSet = Number.isFinite(columnIndex) ? pending.get(columnIndex) || new Set() : new Set();
      if (!Number.isFinite(columnIndex) || !options.length) {
        container.innerHTML = '';
        if (emptyMessage) {
          emptyMessage.hidden = false;
        }
        return;
      }
      const optionMarkup = options
        .map((value, index) => {
          const safeValue = escapeHtml(value);
          const label = escapeHtml(optionLabel(value));
          const checkboxId = `${config.id}-filter-option-${columnIndex}-${index}`;
          const checkedAttr = selectionSet.has(value) ? ' checked' : '';
          return `<label class="regular-filter__option" for="${checkboxId}"><input type="checkbox" id="${checkboxId}" value="${safeValue}"${checkedAttr}><span>${label}</span></label>`;
        })
        .join('');
      container.innerHTML = optionMarkup;
      animateOptionList(container, '.regular-filter__option');
      if (emptyMessage) {
        emptyMessage.hidden = optionMarkup.length > 0;
      }
    }

    function setSalesGapFilterActiveColumn(config, columnIndex) {
      if (!config || !Number.isFinite(columnIndex)) {
        return;
      }
      const columns = Array.isArray(config.availableColumns) ? config.availableColumns : [];
      if (!columns.some((column) => column.index === columnIndex)) {
        return;
      }
      config.activeColumnIndex = columnIndex;
      renderSalesGapFilterFieldButtons(config);
      renderSalesGapFilterOptions(config);
    }

    function closeSalesGapFilter(config, options = {}) {
      if (!config || !config.filterContainer) {
        return;
      }
      const { returnFocus = true } = options;
      hideFilterContainer(config.filterContainer);
      const triggers = [config.filterButton].filter(Boolean);
      triggers.forEach((button) => {
        if (button) {
          button.setAttribute('aria-expanded', 'false');
        }
      });
      const previousTrigger = config.activeTrigger;
      config.activeTrigger = null;
      if (returnFocus && previousTrigger && typeof previousTrigger.focus === 'function') {
        previousTrigger.focus();
      }
      if (activeSalesGapFilterConfig === config) {
        activeSalesGapFilterConfig = null;
      }
    }

    function openSalesGapFilter(config, triggerButton = null) {
      if (!config || !config.filterContainer || !config.filterInitialised) {
        return;
      }
      const hasColumns = Array.isArray(config.availableColumns) && config.availableColumns.length > 0;
      if (!hasColumns) {
        return;
      }
      if (!Number.isFinite(config.activeColumnIndex)) {
        config.activeColumnIndex = config.availableColumns[0].index;
      }
      config.pendingSelections = null;
      syncSalesGapPendingSelections(config);
      renderSalesGapFilterFieldButtons(config);
      renderSalesGapFilterOptions(config);
      showFilterContainer(config.filterContainer);
      const triggers = [config.filterButton].filter(Boolean);
      if (triggerButton) {
        config.activeTrigger = triggerButton;
      } else if (!config.activeTrigger && triggers.length) {
        config.activeTrigger = triggers[0];
      }
      triggers.forEach((button) => {
        if (button) {
          const expanded = button === config.activeTrigger;
          button.setAttribute('aria-expanded', expanded ? 'true' : 'false');
        }
      });
      activeSalesGapFilterConfig = config;
      requestAnimationFrame(() => {
        const firstButton = config.filterFieldOptionsElement?.querySelector('.regular-filter__field-button[aria-pressed="true"]');
        if (firstButton instanceof HTMLElement) {
          firstButton.focus();
        }
      });
    }

    function resetSalesGapFilters(config) {
      if (!config) {
        return;
      }
      config.filters = {};
      if (config.pendingSelections instanceof Map) {
        config.pendingSelections.clear();
      } else {
        config.pendingSelections = new Map();
      }
      renderSalesGapFilterOptions(config);
      updateSalesGapFilterButtonState(config);
      refreshSalesGapTables();
    }

    function clearSalesGapFilters(config) {
      if (!config || !hasActiveSalesGapFilters(config)) {
        return;
      }
      config.filters = {};
      if (config.pendingSelections instanceof Map) {
        config.pendingSelections.clear();
      }
      if (config.filterContainer && config.filterContainer.classList.contains('is-visible')) {
        renderSalesGapFilterOptions(config);
      }
      updateSalesGapFilterButtonState(config);
      refreshSalesGapTables();
    }

    function applySalesGapFilterSelections(config) {
      if (!config) {
        return;
      }
      if (!(config.pendingSelections instanceof Map)) {
        config.pendingSelections = new Map();
      }
      const filters = {};
      const optionsSource = Array.isArray(config.columnValueOptions) ? config.columnValueOptions : [];
      config.pendingSelections.forEach((selectionSet, key) => {
        const columnIndex = Number(key);
        if (!Number.isFinite(columnIndex) || !(selectionSet instanceof Set)) {
          return;
        }
        const availableValues = optionsSource[columnIndex] || [];
        const selectedValues = Array.from(selectionSet).filter((value) => availableValues.includes(value));
        if (selectedValues.length === 0 || selectedValues.length === availableValues.length) {
          return;
        }
        filters[columnIndex] = selectedValues;
      });
      config.filters = filters;
      config.pendingSelections = null;
      updateSalesGapFilterButtonState(config);
      refreshSalesGapTables();
    }

    function ensureSalesGapFiltersSetup() {
      SALES_GAP_FILTER_CONFIGS.forEach((config) => {
        if (!config) {
          return;
        }
        config.filterButton = config.filterButton || document.getElementById(config.filterButtonId);
        config.filterClearButton = config.filterClearButton || document.getElementById(config.filterClearButtonId);
        config.filterContainer = config.filterContainer || document.getElementById(config.filterContainerId);
        config.filterFieldOptionsElement = config.filterFieldOptionsElement || document.getElementById(config.filterFieldOptionsId);
        config.filterOptionsElement = config.filterOptionsElement || document.getElementById(config.filterOptionsId);
        config.filterEmptyElement = config.filterEmptyElement || document.getElementById(config.filterEmptyId);
        config.filterApplyButton = config.filterApplyButton || document.getElementById(config.filterApplyId);
        config.filterResetButton = config.filterResetButton || document.getElementById(config.filterResetId);
        if (!config.filterCloseButton && config.filterContainer) {
          config.filterCloseButton = config.filterContainer.querySelector('.regular-filter__close');
        }
        if (!config.filters) {
          config.filters = {};
        }
        const readyElements = [
          config.filterButton,
          config.filterContainer,
          config.filterFieldOptionsElement,
          config.filterOptionsElement,
          config.filterApplyButton,
          config.filterResetButton,
          config.filterCloseButton,
        ].every(Boolean);
        if (!config.filterInitialised && readyElements) {
          config.filterButton.addEventListener('click', () => {
            const isVisible = config.filterContainer.classList.contains('is-visible');
            if (isVisible) {
              closeSalesGapFilter(config);
            } else {
              openSalesGapFilter(config, config.filterButton);
            }
          });
          if (config.filterClearButton) {
            config.filterClearButton.addEventListener('click', () => clearSalesGapFilters(config));
          }
          config.filterCloseButton.addEventListener('click', () => closeSalesGapFilter(config));
          config.filterContainer.addEventListener('click', (event) => {
            const target = event.target;
            if (target === config.filterContainer || (target instanceof HTMLElement && target.classList.contains('regular-filter__backdrop'))) {
              closeSalesGapFilter(config);
            }
          });
          config.filterOptionsElement.addEventListener('change', (event) => {
            const target = event.target;
            if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') {
              return;
            }
            if (!(config.pendingSelections instanceof Map)) {
              config.pendingSelections = new Map();
            }
            const columnIndex = config.activeColumnIndex;
            if (!Number.isFinite(columnIndex)) {
              return;
            }
            let selection = config.pendingSelections.get(columnIndex);
            if (!(selection instanceof Set)) {
              selection = new Set(config.filters?.[columnIndex] || []);
            }
            if (target.checked) {
              selection.add(target.value);
            } else {
              selection.delete(target.value);
            }
            if (selection.size) {
              config.pendingSelections.set(columnIndex, selection);
            } else {
              config.pendingSelections.delete(columnIndex);
            }
          });
          config.filterApplyButton.addEventListener('click', () => {
            flashButtonBusy(config.filterApplyButton);
            applySalesGapFilterSelections(config);
            closeSalesGapFilter(config, { returnFocus: false });
          });
          config.filterResetButton.addEventListener('click', () => resetSalesGapFilters(config));
          config.filterInitialised = true;
        }
        updateSalesGapFilterButtonState(config);
      });

      if (!salesGapFilterKeyListenerRegistered) {
        document.addEventListener('keydown', (event) => {
          if (event.key === 'Escape' && activeSalesGapFilterConfig && activeSalesGapFilterConfig.filterContainer && activeSalesGapFilterConfig.filterContainer.classList.contains('is-visible')) {
            closeSalesGapFilter(activeSalesGapFilterConfig);
          }
        });
        salesGapFilterKeyListenerRegistered = true;
      }
    }

    function updateSalesGapFilterDataset(config, dataset) {
      if (!config) {
        return;
      }
      const baseDataset = dataset && Array.isArray(dataset.columns) ? dataset : { columns: [], rows: [] };
      config.dataset = baseDataset;
      config.columnValueOptions = Array.isArray(baseDataset.columns) && baseDataset.columns.length
        ? buildColumnOptions(baseDataset)
        : [];
      const columns = Array.isArray(baseDataset.columns)
        ? baseDataset.columns
            .map((title, index) => ({
              title: title || `Column ${index + 1}`,
              index,
              options: config.columnValueOptions[index] || [],
            }))
            .filter((entry) => entry.options.length > 0 && entry.options.length <= REGULAR_FILTER_MAX_UNIQUE_VALUES)
        : [];
      config.availableColumns = columns;
      const sanitizedFilters = {};
      if (config.filters && typeof config.filters === 'object') {
        Object.entries(config.filters).forEach(([key, values]) => {
          const columnIndex = Number(key);
          if (!Number.isFinite(columnIndex)) {
            return;
          }
          if (!columns.some((column) => column.index === columnIndex)) {
            return;
          }
          const availableValues = config.columnValueOptions[columnIndex] || [];
          const validValues = Array.isArray(values) ? values.filter((value) => availableValues.includes(value)) : [];
          if (validValues.length) {
            sanitizedFilters[columnIndex] = validValues;
          }
        });
      }
      config.filters = sanitizedFilters;
      if (config.pendingSelections instanceof Map) {
        Array.from(config.pendingSelections.keys()).forEach((key) => {
          const columnIndex = Number(key);
          const availableValues = config.columnValueOptions[columnIndex] || [];
          const selection = config.pendingSelections.get(key);
          if (!(selection instanceof Set)) {
            config.pendingSelections.delete(key);
            return;
          }
          const validValues = Array.from(selection).filter((value) => availableValues.includes(value));
          if (validValues.length) {
            config.pendingSelections.set(columnIndex, new Set(validValues));
          } else {
            config.pendingSelections.delete(key);
          }
        });
      }
      if (!columns.length) {
        config.activeColumnIndex = null;
      } else if (!Number.isFinite(config.activeColumnIndex) || !columns.some((column) => column.index === config.activeColumnIndex)) {
        config.activeColumnIndex = columns[0].index;
      }
      if (config.filterInitialised) {
        renderSalesGapFilterFieldButtons(config);
        if (config.filterContainer && config.filterContainer.classList.contains('is-visible')) {
          renderSalesGapFilterOptions(config);
        }
      }
      updateSalesGapFilterButtonState(config);
    }

    function applySalesGapFiltersToDataset(dataset, config) {
      if (!dataset || !config) {
        return dataset;
      }
      return buildFilteredDataset(dataset, config.filters || {});
    }

    function refreshSalesGapTables() {
      if (!salesGapDatasetCache) {
        return;
      }
      setTabPanelLoading('sales-gap', true, 'Applying filters…');
      requestAnimationFrame(() => {
        renderSalesGapSection(salesGapDatasetCache);
        setTabPanelLoading('sales-gap', false);
      });
    }

    function renderDatasetTable(dataset, options = {}) {
      const tableId = typeof options.tableId === 'string' && options.tableId.trim().length
        ? options.tableId.trim()
        : 'sales-gap-table';
      const tableElement = document.getElementById(tableId);
      if (!tableElement) {
        return;
      }
      const emptyClass = typeof options.emptyClass === 'string' && options.emptyClass.trim().length
        ? options.emptyClass.trim()
        : 'sales-gap-table__empty';
      const totalRowClass = typeof options.totalRowClass === 'string' && options.totalRowClass.trim().length
        ? options.totalRowClass.trim()
        : 'sales-gap-row--total';
      const totalRowLabel = typeof options.totalRowLabel === 'string' && options.totalRowLabel.trim().length
        ? options.totalRowLabel
        : TOTAL_ROW_LABEL;

      if (!dataset || !Array.isArray(dataset.columns) || dataset.columns.length === 0) {
        tableElement.innerHTML = `<tbody><tr><td class="${emptyClass}">No data available</td></tr></tbody>`;
        return;
      }

      const rows = Array.isArray(dataset.rows) ? dataset.rows : [];
      if (!rows.length) {
        tableElement.innerHTML = `<tbody><tr><td class="${emptyClass}">No data available</td></tr></tbody>`;
        return;
      }

      tableElement.innerHTML = '';
      applyHeaderPreambleToTable(tableElement, dataset.headerPreamble || null, dataset.columns);

      const numericColumns = detectNumericColumns(dataset);
      const numericColumnSet = new Set(numericColumns);
      const tbody = document.createElement('tbody');
      const normalizedTotal = totalRowLabel.toLowerCase();

      rows.forEach((row) => {
        const sourceRow = Array.isArray(row) ? row : [];
        const tr = document.createElement('tr');
        const firstCell = sourceRow[0];
        if (typeof firstCell === 'string' && firstCell.trim().toLowerCase() === normalizedTotal) {
          tr.classList.add(totalRowClass);
        }
        dataset.columns.forEach((columnName, columnIndex) => {
          const td = document.createElement('td');
          const value = columnIndex < sourceRow.length ? sourceRow[columnIndex] : '';
          const formatted = formatCellValue(value, columnName);
          if (columnIndex === 0) {
            td.textContent = formatted;
          } else {
            if (numericColumnSet.has(columnIndex)) {
              td.classList.add('cell-numeric');
            }
            td.textContent = formatted;
          }
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });

      tableElement.appendChild(tbody);
    }

    function renderSalesGapSection(datasets) {
      const source = datasets && typeof datasets === 'object' ? datasets : {};
      const loDataset = source.loDataset && typeof source.loDataset === 'object'
        ? source.loDataset
        : { columns: [], rows: [] };
      const skuDataset = source.skuDataset && typeof source.skuDataset === 'object'
        ? source.skuDataset
        : { columns: [], rows: [] };
      ensureSalesGapFiltersSetup();
      const loFilterConfig = getSalesGapFilterConfigByDatasetKey('loDataset');
      const skuFilterConfig = getSalesGapFilterConfigByDatasetKey('skuDataset');
      updateSalesGapFilterDataset(loFilterConfig, loDataset);
      updateSalesGapFilterDataset(skuFilterConfig, skuDataset);
      const filteredLoDataset = loFilterConfig ? applySalesGapFiltersToDataset(loDataset, loFilterConfig) : loDataset;
      const filteredSkuDataset = skuFilterConfig ? applySalesGapFiltersToDataset(skuDataset, skuFilterConfig) : skuDataset;
      renderDatasetTable(filteredLoDataset, {
        tableId: 'sales-gap-table',
        emptyClass: 'sales-gap-table__empty',
        totalRowClass: 'sales-gap-row--total',
      });
      renderDatasetTable(filteredSkuDataset, {
        tableId: 'sku-gap-table',
        emptyClass: 'sku-gap-table__empty',
        totalRowClass: 'sku-gap-row--total',
      });
    }

    function loadSalesGapTable() {
      if (salesGapInitialised && salesGapDatasetCache) {
        return Promise.resolve();
      }
      if (salesGapDatasetCache && !salesGapInitialised) {
        renderSalesGapSection(salesGapDatasetCache);
        salesGapInitialised = true;
        return Promise.resolve();
      }
      setTabPanelLoading('sales-gap', true, salesGapInitialised ? 'Refreshing gap data…' : 'Loading gap data…');
      return fetchSalesGapDataset()
        .then((datasets) => {
          renderSalesGapSection(datasets);
          salesGapInitialised = true;
        })
        .catch((error) => {
          const salesMessage = typeof error?.message === 'string' && error.message.trim().length
            ? error.message
            : 'Unable to load Sales GAP data';
          const skuMessage = 'Unable to load SKU GAP data';
          const targets = [
            { id: 'sales-gap-table', emptyClass: 'sales-gap-table__empty', message: salesMessage },
            { id: 'sku-gap-table', emptyClass: 'sku-gap-table__empty', message: skuMessage },
          ];
          targets.forEach((target) => {
            const tableElement = document.getElementById(target.id);
            if (!tableElement) {
              return;
            }
            const safeClass = target.emptyClass || 'sales-gap-table__empty';
            const text = target.message || salesMessage;
            tableElement.innerHTML = `<tbody><tr><td class="${safeClass}">${escapeHtml(text)}</td></tr></tbody>`;
          });
          ensureSalesGapFiltersSetup();
          SALES_GAP_FILTER_CONFIGS.forEach((config) => updateSalesGapFilterDataset(config, { columns: [], rows: [] }));
        })
        .finally(() => {
          setTabPanelLoading('sales-gap', false);
        });
    }

    function buildSpendScaling(actualPivot, referencePivot) {
      if (!actualPivot || !referencePivot) {
        return null;
      }
      const ownerKeys = Array.isArray(actualPivot.loList)
        ? actualPivot.loList.map((label) => normalizeOwnerKey(label))
        : [];
      if (!ownerKeys.length) {
        return null;
      }
      const scaling = {
        ownerKeys,
        byDate: new Map(),
        totals: new Map(),
      };
      const referenceRowMap = new Map();
      if (Array.isArray(referencePivot.rows)) {
        referencePivot.rows.forEach((row) => {
          referenceRowMap.set(row.displayDate, row);
        });
      }
      if (Array.isArray(actualPivot.rows)) {
        actualPivot.rows.forEach((row) => {
          const referenceRow = referenceRowMap.get(row.displayDate);
          const valueMap = new Map();
          ownerKeys.forEach((ownerKey, index) => {
            const actualValue = parseNumericValue(row.formattedValues?.[index]) ?? 0;
            const referenceValue = referenceRow
              ? parseNumericValue(referenceRow.formattedValues?.[index]) ?? 0
              : 0;
            valueMap.set(ownerKey, { actualValue, referenceValue });
          });
          scaling.byDate.set(row.displayDate, valueMap);
        });
      }
      const referenceTotals = Array.isArray(referencePivot.totals) ? referencePivot.totals : [];
      const actualTotals = Array.isArray(actualPivot.totals) ? actualPivot.totals : [];
      ownerKeys.forEach((ownerKey, index) => {
        const actualValue = parseNumericValue(actualTotals[index]) ?? 0;
        const referenceValue = parseNumericValue(referenceTotals[index]) ?? 0;
        scaling.totals.set(ownerKey, { actualValue, referenceValue });
      });
      return scaling;
    }

    function applySpendScaling(basePivot, scaling, filtersActive) {
      if (!basePivot || !scaling) {
        return basePivot;
      }
      const ownerKeys = Array.isArray(scaling.ownerKeys) ? scaling.ownerKeys : [];
      const scaledRows = Array.isArray(basePivot.rows)
        ? basePivot.rows.map((row) => {
            const dateMap = scaling.byDate.get(row.displayDate);
            const formattedValues = Array.isArray(row.formattedValues)
              ? row.formattedValues.map((value, index) => {
                  const ownerKey = ownerKeys[index];
                  const reference = dateMap ? dateMap.get(ownerKey) : null;
                  const numericValue = parseNumericValue(value) ?? 0;
                  let scaledNumeric = numericValue;
                  if (reference) {
                    if (Math.abs(reference.referenceValue) > 1e-6) {
                      const factor = reference.actualValue / reference.referenceValue;
                      scaledNumeric = numericValue * factor;
                    } else if (!filtersActive) {
                      scaledNumeric = reference.actualValue;
                    }
                  }
                  return formatTwoDecimal(scaledNumeric);
                })
              : [];
            return { displayDate: row.displayDate, formattedValues };
          })
        : [];
      const scaledTotals = Array.isArray(basePivot.totals)
        ? basePivot.totals.map((value, index) => {
            const ownerKey = ownerKeys[index];
            const reference = scaling.totals.get(ownerKey);
            const numericValue = parseNumericValue(value) ?? 0;
            let scaledNumeric = numericValue;
            if (reference) {
              if (Math.abs(reference.referenceValue) > 1e-6) {
                const factor = reference.actualValue / reference.referenceValue;
                scaledNumeric = numericValue * factor;
              } else if (!filtersActive) {
                scaledNumeric = reference.actualValue;
              }
            }
            return formatTwoDecimal(scaledNumeric);
          })
        : [];
      return {
        loList: Array.isArray(basePivot.loList) ? basePivot.loList.slice() : [],
        rows: scaledRows,
        totals: scaledTotals,
      };
    }

    function updateLoTablesWithDataset(dataset) {
      const salesTableElement = document.getElementById('lo-sales-table');
      const spendTableElement = document.getElementById('lo-spend-table');
      if (!salesTableElement || !spendTableElement) {
        return;
      }
      const filtersActive = hasActiveColumnFilters();
      const baseOptions = {};
      if (Array.isArray(loSalesOrderCache) && loSalesOrderCache.length) {
        baseOptions.normalizedOrder = loSalesOrderCache;
      }
      if (loDisplayNameOverridesCache instanceof Map && loDisplayNameOverridesCache.size) {
        baseOptions.displayNameOverrides = loDisplayNameOverridesCache;
      }
      const salesPivot = buildLoPivot(dataset, 'total revenue', baseOptions);
      if (!Array.isArray(loSalesOrderCache) || loSalesOrderCache.length === 0) {
        loSalesOrderCache = Array.isArray(salesPivot.order) ? salesPivot.order.slice() : [];
      }
      if (salesPivot.displayNames instanceof Map) {
        loDisplayNameOverridesCache = new Map(salesPivot.displayNames);
      }
      renderLoTable(salesTableElement, salesPivot);

      let spendPivot;
      if (!filtersActive && loBaselineSpendPivot) {
        spendPivot = loBaselineSpendPivot;
      } else {
        const spendOptions = {};
        if (Array.isArray(loSalesOrderCache) && loSalesOrderCache.length) {
          spendOptions.normalizedOrder = loSalesOrderCache;
        }
        if (loDisplayNameOverridesCache instanceof Map && loDisplayNameOverridesCache.size) {
          spendOptions.displayNameOverrides = loDisplayNameOverridesCache;
        }
        const adSpendPivot = buildLoPivot(dataset, 'Ad Spend', spendOptions);
        if (!loBaselineAdSpendPivot) {
          loBaselineAdSpendPivot = adSpendPivot;
        }
        if (loSpendScalingData) {
          spendPivot = applySpendScaling(adSpendPivot, loSpendScalingData, filtersActive);
        } else {
          spendPivot = adSpendPivot;
        }
      }
      renderLoTable(spendTableElement, spendPivot);
      requestAnimationFrame(() => resizeLoTableContainers());
    }

    function updatePlatformTablesWithDataset(dataset) {
      const platformSalesTableElement = document.getElementById('platform-sales-table');
      const platformNetTableElement = document.getElementById('platform-net-table');
      if (!platformSalesTableElement || !platformNetTableElement) {
        return;
      }

      const platformSalesPivot = buildDailyPivot(dataset, 'Platform', 'total revenue', {
        fallbackLabel: 'Unassigned Platform',
      });
      renderLoTable(platformSalesTableElement, platformSalesPivot);

      const platformNetPivot = buildDailyPivot(dataset, 'Platform', 'NET', {
        fallbackLabel: 'Unassigned Platform',
      });
      renderLoTable(platformNetTableElement, platformNetPivot);

      requestAnimationFrame(() => resizeLoTableContainers());
    }

    function normalizeOwnerKey(label) {
      if (typeof label !== 'string') {
        return '';
      }
      return label.trim().toLocaleLowerCase();
    }

    function alignPivotToReference(pivotData, referencePivot) {
      if (!pivotData || !Array.isArray(pivotData.loList) || !Array.isArray(pivotData.rows)) {
        return { loList: [], rows: [], totals: [] };
      }
      const referenceLabels = Array.isArray(referencePivot?.loList) ? referencePivot.loList : [];
      const referenceKeys = referenceLabels.map((label) => normalizeOwnerKey(label));
      const pivotKeys = pivotData.loList.map((label) => normalizeOwnerKey(label));

      const labelMap = new Map();
      pivotKeys.forEach((key, index) => {
        if (!labelMap.has(key)) {
          labelMap.set(key, pivotData.loList[index] ?? '');
        }
      });

      const columnIndexMap = new Map();
      pivotKeys.forEach((key, index) => {
        if (!columnIndexMap.has(key)) {
          columnIndexMap.set(key, index);
        }
      });

      const orderedKeys = [];
      referenceKeys.forEach((key) => {
        if (columnIndexMap.has(key) && !orderedKeys.includes(key)) {
          orderedKeys.push(key);
        }
      });
      pivotKeys.forEach((key) => {
        if (!orderedKeys.includes(key)) {
          orderedKeys.push(key);
        }
      });

      const loList = orderedKeys.map((key) => labelMap.get(key) ?? '');
      const totalsAccumulator = new Array(orderedKeys.length).fill(0);

      const rows = pivotData.rows.map((row) => {
        const displayDate = typeof row.displayDate === 'string' ? row.displayDate : '';
        const formattedValuesSource = Array.isArray(row.formattedValues) ? row.formattedValues : [];
        const formattedValues = orderedKeys.map((key, targetIndex) => {
          const columnIndex = columnIndexMap.get(key);
          if (typeof columnIndex !== 'number') {
            return '0.00';
          }
          const value = formattedValuesSource[columnIndex];
          const numericValue = parseNumericValue(value);
          if (numericValue !== null) {
            totalsAccumulator[targetIndex] += numericValue;
          }
          if (typeof value === 'string') {
            return value;
          }
          if (typeof value === 'number' && Number.isFinite(value)) {
            return value.toFixed(2);
          }
          return '0.00';
        });
        return { displayDate, formattedValues };
      });

      const totalsSource = Array.isArray(pivotData.totals) ? pivotData.totals : [];
      const totals = orderedKeys.map((key, targetIndex) => {
        const columnIndex = columnIndexMap.get(key);
        if (typeof columnIndex !== 'number') {
          const computedFallback = totalsAccumulator[targetIndex] ?? 0;
          return formatTwoDecimal(computedFallback);
        }
        const value = totalsSource[columnIndex];
        const numericValue = parseNumericValue(value);
        if (numericValue !== null) {
          return formatTwoDecimal(numericValue);
        }
        if (typeof value === 'string') {
          const trimmed = value.trim();
          if (trimmed.length) {
            return trimmed;
          }
        }
        const computedFallback = totalsAccumulator[targetIndex] ?? 0;
        return formatTwoDecimal(computedFallback);
      });

      return { loList, rows, totals };
    }

    function formatSkuSummaryValue(value, column) {
      if (value === null || value === undefined || value === '') {
        return '';
      }
      const columnType = column && typeof column.type === 'string' ? column.type : 'decimal';
      const numeric = typeof value === 'number' ? value : Number(value);
      if (Number.isFinite(numeric)) {
        if (columnType === 'integer') {
          return integerFormatter.format(Math.round(numeric));
        }
        return numberFormatter.format(numeric);
      }
      return escapeHtml(String(value));
    }

    function buildSkuSummaryPivotFromDataset(dataset) {
      if (!dataset || !Array.isArray(dataset.columns) || !Array.isArray(dataset.rows)) {
        return { columns: [], rows: [], totals: null };
      }

      const resolveColumnIndex = (...candidates) => {
        for (const candidate of candidates) {
          if (!candidate) {
            continue;
          }
          const index = findColumnIndex(dataset, candidate);
          if (index !== -1) {
            return index;
          }
        }
        return -1;
      };

      const skuIndex = resolveColumnIndex('SKU_2', 'SKU2', 'SKU 2', 'SKU');
      const salePriceIndex = resolveColumnIndex('Sale Price + Shipping');
      const carrierFeeIndex = resolveColumnIndex('CARRIER FEE');
      const marketplaceFeeIndex = resolveColumnIndex('Ebay/ Amazon');
      const costIndex = resolveColumnIndex('P.COST');
      const netPerQtyIndex = resolveColumnIndex('NET/Q');
      const netIndex = resolveColumnIndex('NET');
      const finalNetIndex = resolveColumnIndex('Final Net');
      const revenueIndex = resolveColumnIndex('Total Revenue');
      const qtyIndex = resolveColumnIndex('Qty');
      const requiredIndices = [
        skuIndex,
        salePriceIndex,
        carrierFeeIndex,
        marketplaceFeeIndex,
        costIndex,
        netPerQtyIndex,
        netIndex,
        finalNetIndex,
        revenueIndex,
        qtyIndex,
      ];
      if (requiredIndices.some((index) => index === -1)) {
        return { columns: [], rows: [], totals: null };
      }

      const summary = new Map();
      const totalsAccumulator = {
        salePriceSum: 0,
        salePriceCount: 0,
        carrierFeeSum: 0,
        carrierFeeCount: 0,
        marketplaceFeeSum: 0,
        marketplaceFeeCount: 0,
        costSum: 0,
        costCount: 0,
        netPerQtySum: 0,
        netPerQtyCount: 0,
        netSum: 0,
        finalNetSum: 0,
        revenueSum: 0,
        qtySum: 0,
      };

      dataset.rows.forEach((row) => {
        const rawSku = row[skuIndex];
        const sku = rawSku === null || rawSku === undefined ? '' : String(rawSku).trim();
        if (!sku || sku === '-') {
          return;
        }
        if (!summary.has(sku)) {
          summary.set(sku, {
            sku,
            salePriceSum: 0,
            salePriceCount: 0,
            carrierFeeSum: 0,
            carrierFeeCount: 0,
            marketplaceFeeSum: 0,
            marketplaceFeeCount: 0,
            costSum: 0,
            costCount: 0,
            netPerQtySum: 0,
            netPerQtyCount: 0,
            netSum: 0,
            finalNetSum: 0,
            revenueSum: 0,
            qtySum: 0,
          });
        }
        const entry = summary.get(sku);

        const salePriceValue = parseNumericValue(row[salePriceIndex]);
        if (salePriceValue !== null) {
          entry.salePriceSum += salePriceValue;
          entry.salePriceCount += 1;
          totalsAccumulator.salePriceSum += salePriceValue;
          totalsAccumulator.salePriceCount += 1;
        }

        const carrierFeeValue = parseNumericValue(row[carrierFeeIndex]);
        if (carrierFeeValue !== null) {
          entry.carrierFeeSum += carrierFeeValue;
          entry.carrierFeeCount += 1;
          totalsAccumulator.carrierFeeSum += carrierFeeValue;
          totalsAccumulator.carrierFeeCount += 1;
        }

        const marketplaceFeeValue = parseNumericValue(row[marketplaceFeeIndex]);
        if (marketplaceFeeValue !== null) {
          entry.marketplaceFeeSum += marketplaceFeeValue;
          entry.marketplaceFeeCount += 1;
          totalsAccumulator.marketplaceFeeSum += marketplaceFeeValue;
          totalsAccumulator.marketplaceFeeCount += 1;
        }

        const costValue = parseNumericValue(row[costIndex]);
        if (costValue !== null) {
          entry.costSum += costValue;
          entry.costCount += 1;
          totalsAccumulator.costSum += costValue;
          totalsAccumulator.costCount += 1;
        }

        const netPerQtyValue = parseNumericValue(row[netPerQtyIndex]);
        if (netPerQtyValue !== null) {
          entry.netPerQtySum += netPerQtyValue;
          entry.netPerQtyCount += 1;
          totalsAccumulator.netPerQtySum += netPerQtyValue;
          totalsAccumulator.netPerQtyCount += 1;
        }

        const netValue = parseNumericValue(row[netIndex]);
        if (netValue !== null) {
          entry.netSum += netValue;
          totalsAccumulator.netSum += netValue;
        }

        const finalNetValue = parseNumericValue(row[finalNetIndex]);
        if (finalNetValue !== null) {
          entry.finalNetSum += finalNetValue;
          totalsAccumulator.finalNetSum += finalNetValue;
        }

        const revenueValue = parseNumericValue(row[revenueIndex]);
        if (revenueValue !== null) {
          entry.revenueSum += revenueValue;
          totalsAccumulator.revenueSum += revenueValue;
        }

        const qtyValue = parseNumericValue(row[qtyIndex]);
        if (qtyValue !== null) {
          entry.qtySum += qtyValue;
          totalsAccumulator.qtySum += qtyValue;
        }
      });

      const rows = Array.from(summary.values())
        .map((entry) => ({
          sku: entry.sku,
          average_of_sale_price_shipping: entry.salePriceCount ? entry.salePriceSum / entry.salePriceCount : null,
          average_of_carrier_fee: entry.carrierFeeCount ? entry.carrierFeeSum / entry.carrierFeeCount : null,
          average_of_ebay_amazon: entry.marketplaceFeeCount ? entry.marketplaceFeeSum / entry.marketplaceFeeCount : null,
          average_of_p_cost: entry.costCount ? entry.costSum / entry.costCount : null,
          average_of_net_q: entry.netPerQtyCount ? entry.netPerQtySum / entry.netPerQtyCount : null,
          sum_of_net: entry.netSum,
          sum_of_final_net: entry.finalNetSum,
          sum_of_total_revenue: entry.revenueSum,
          sum_of_qty: entry.qtySum,
        }))
        .sort((a, b) => a.sku.localeCompare(b.sku, undefined, { numeric: true, sensitivity: 'base' }));

      const totals = {
        sku: 'Grand Total',
        average_of_sale_price_shipping: totalsAccumulator.salePriceCount
          ? totalsAccumulator.salePriceSum / totalsAccumulator.salePriceCount
          : null,
        average_of_carrier_fee: totalsAccumulator.carrierFeeCount
          ? totalsAccumulator.carrierFeeSum / totalsAccumulator.carrierFeeCount
          : null,
        average_of_ebay_amazon: totalsAccumulator.marketplaceFeeCount
          ? totalsAccumulator.marketplaceFeeSum / totalsAccumulator.marketplaceFeeCount
          : null,
        average_of_p_cost: totalsAccumulator.costCount
          ? totalsAccumulator.costSum / totalsAccumulator.costCount
          : null,
        average_of_net_q: totalsAccumulator.netPerQtyCount
          ? totalsAccumulator.netPerQtySum / totalsAccumulator.netPerQtyCount
          : null,
        sum_of_net: totalsAccumulator.netSum,
        sum_of_final_net: totalsAccumulator.finalNetSum,
        sum_of_total_revenue: totalsAccumulator.revenueSum,
        sum_of_qty: totalsAccumulator.qtySum,
      };

      const columns = [
        { key: 'sku', label: 'SKU', type: 'string' },
        { key: 'average_of_sale_price_shipping', label: 'Average of Sale Price + Shipping', type: 'decimal' },
        { key: 'average_of_carrier_fee', label: 'Average of CARRIER FEE', type: 'decimal' },
        { key: 'average_of_ebay_amazon', label: 'Average of Ebay/ Amazon', type: 'decimal' },
        { key: 'average_of_p_cost', label: 'Average of P.COST', type: 'decimal' },
        { key: 'average_of_net_q', label: 'Average of NET/Q', type: 'decimal' },
        { key: 'sum_of_net', label: 'Sum of NET', type: 'decimal' },
        { key: 'sum_of_total_revenue', label: 'Sum of Total Revenue', type: 'decimal' },
        { key: 'sum_of_qty', label: 'Sum of Qty', type: 'integer' },
        { key: 'sum_of_final_net', label: 'Final Net (Sum)', type: 'decimal' },
      ];

      return { columns, rows, totals };
    }

    function updateSkuSummaryWithDataset(dataset) {
      const pivot = buildSkuSummaryPivotFromDataset(dataset);
      skuSummaryPivotCache = pivot;
      skuSummaryCurrentPage = 1;
      skuSummaryTotalRows = Array.isArray(pivot.rows) ? pivot.rows.length : 0;
      renderSkuSummaryTable(pivot, { page: 1 });
    }

    function renderSkuSummaryMessage(message, columnCount = 2) {
      const tableElement = document.getElementById('sku-summary-table');
      if (!tableElement) {
        return;
      }
      const span = Math.max(1, columnCount);
      const content = escapeHtml(message);
      tableElement.innerHTML = `<tbody><tr><td class="sku-table__message" colspan="${span}">${content}</td></tr></tbody>`;
      skuSummaryTotalRows = 0;
      skuSummaryCurrentPage = 1;
      renderSkuSummaryPagination(0, skuSummaryPageSize, 1);
    }

    function renderSkuSummaryPagination(totalRows, pageSize, currentPage) {
      const paginationElement = document.getElementById('sku-summary-pagination');
      if (!paginationElement) {
        return;
      }

      const toolbarElement = paginationElement.parentElement;
      const safePageSize = Number.isFinite(pageSize) && pageSize > 0 ? Math.floor(pageSize) : 25;

      if (!Number.isFinite(totalRows) || totalRows <= safePageSize) {
        paginationElement.textContent = '';
        paginationElement.setAttribute('aria-hidden', 'true');
        if (toolbarElement && toolbarElement.classList && toolbarElement.classList.contains('sku-card__toolbar')) {
          toolbarElement.setAttribute('data-active', 'false');
        }
        return;
      }

      const totalPages = Math.max(1, Math.ceil(totalRows / safePageSize));
      const safeCurrentPage = Math.min(Math.max(1, currentPage || 1), totalPages);
      const start = (safeCurrentPage - 1) * safePageSize + 1;
      const end = Math.min(totalRows, start + safePageSize - 1);

      const previousDisabled = safeCurrentPage <= 1 ? ' disabled' : '';
      const nextDisabled = safeCurrentPage >= totalPages ? ' disabled' : '';

      paginationElement.setAttribute('aria-hidden', 'false');
      if (toolbarElement && toolbarElement.classList && toolbarElement.classList.contains('sku-card__toolbar')) {
        toolbarElement.setAttribute('data-active', 'true');
      }
      paginationElement.innerHTML = `
        <button type="button" class="sku-card__pagination-button" data-action="previous"${previousDisabled}>Previous</button>
        <span class="sku-card__pagination-info">Showing ${start}&ndash;${end} of ${totalRows}</span>
        <button type="button" class="sku-card__pagination-button" data-action="next"${nextDisabled}>Next</button>
      `;
    }

    function renderSkuSummaryTable(pivotData, options = {}) {
      const tableElement = document.getElementById('sku-summary-table');
      if (!tableElement) {
        return;
      }
      const columns = Array.isArray(pivotData?.columns) ? pivotData.columns : [];
      const rows = Array.isArray(pivotData?.rows) ? pivotData.rows : [];
      if (!columns.length) {
        renderSkuSummaryMessage('No data available');
        return;
      }
      const pageSizeOption = Number.isFinite(options.pageSize) && options.pageSize > 0 ? Math.floor(options.pageSize) : skuSummaryPageSize;
      const safePageSize = pageSizeOption > 0 ? pageSizeOption : 25;
      const totalRows = rows.length;
      const totalPages = totalRows > 0 ? Math.ceil(totalRows / safePageSize) : 1;
      const requestedPage = Number.isFinite(options.page) ? Math.floor(options.page) : skuSummaryCurrentPage;
      const safePage = Math.min(Math.max(1, requestedPage), Math.max(1, totalPages));
      skuSummaryPageSize = safePageSize;
      skuSummaryCurrentPage = safePage;
      skuSummaryTotalRows = totalRows;
      const startIndex = (safePage - 1) * safePageSize;
      const paginatedRows = totalRows > 0 ? rows.slice(startIndex, startIndex + safePageSize) : [];

      const columnCount = columns.length;
      let headerHtml = '<thead><tr>';
      columns.forEach((column) => {
        const label = typeof column.label === 'string' ? column.label : '';
        headerHtml += `<th scope="col">${escapeHtml(label)}</th>`;
      });
      headerHtml += '</tr></thead>';

      let bodyHtml = '<tbody>';
      if (!paginatedRows.length) {
        bodyHtml += `<tr><td class="sku-table__message" colspan="${columnCount}">No data available</td></tr>`;
      } else {
        paginatedRows.forEach((row) => {
          bodyHtml += '<tr>';
          columns.forEach((column, index) => {
            const key = column.key;
            const value = row ? row[key] : null;
            if (index === 0) {
              const text = value === null || value === undefined ? '' : String(value);
              bodyHtml += `<td>${escapeHtml(text)}</td>`;
            } else {
              bodyHtml += `<td>${formatSkuSummaryValue(value, column)}</td>`;
            }
          });
          bodyHtml += '</tr>';
        });
      }
      bodyHtml += '</tbody>';

      let footerHtml = '';
      const totals = pivotData && typeof pivotData.totals === 'object' && pivotData.totals !== null
        ? pivotData.totals
        : null;
      if (totals) {
        footerHtml = '<tfoot><tr>';
        columns.forEach((column, index) => {
          const key = column.key;
          const value = totals[key];
          if (index === 0) {
            const text = value === null || value === undefined ? 'Grand Total' : String(value);
            footerHtml += `<th scope="row">${escapeHtml(text)}</th>`;
          } else {
            footerHtml += `<td>${formatSkuSummaryValue(value, column)}</td>`;
          }
        });
        footerHtml += '</tr></tfoot>';
      }

      tableElement.innerHTML = `${headerHtml}${bodyHtml}${footerHtml}`;
      renderSkuSummaryPagination(totalRows, safePageSize, safePage);
    }

    function loadSkuSummaryTable() {
      if (skuSummaryInitialised) {
        return Promise.resolve();
      }

      setTabPanelLoading('sku-summary', true, 'Preparing SKU summary…');
      renderSkuSummaryMessage('Loading data…');

      const datasetPromise = fetchRegularDataset()
        .then((dataset) => {
          if (!regularFilterInitialised) {
            const augmentedForFilters = augmentDatasetWithTotals(dataset);
            columnValueOptions = buildColumnOptions(augmentedForFilters);
            initializeRegularFilterControls(augmentedForFilters);
          }
          return dataset;
        })
        .catch((datasetError) => {
          console.error('Failed to load dataset for SKU summary:', datasetError);
          return null;
        });

      if (hasActiveColumnFilters()) {
        return datasetPromise
          .then((dataset) => {
            if (!dataset) {
              throw new Error('Dataset is unavailable');
            }
            const effectiveDataset = buildFilteredDataset(dataset, columnFilters);
            const pivotData = buildSkuSummaryPivotFromDataset(effectiveDataset);
            skuSummaryPivotCache = pivotData;
            skuSummaryCurrentPage = 1;
            skuSummaryTotalRows = Array.isArray(pivotData?.rows) ? pivotData.rows.length : 0;
            renderSkuSummaryTable(pivotData, { page: 1 });
            skuSummaryInitialised = true;
          })
          .catch((error) => {
            const message = error && error.message ? error.message : 'Unable to load SKU summary';
            renderSkuSummaryMessage(message);
          })
          .finally(() => {
            setTabPanelLoading('sku-summary', false);
          });
      }

      return fetchSkuSummaryPivot()
        .then((pivotData) => {
          if (!pivotData || !Array.isArray(pivotData.columns) || !pivotData.columns.length) {
            throw new Error('SKU summary pivot is unavailable');
          }
          skuSummaryPivotCache = pivotData;
          skuSummaryCurrentPage = 1;
          skuSummaryTotalRows = Array.isArray(pivotData?.rows) ? pivotData.rows.length : 0;
          renderSkuSummaryTable(pivotData, { page: 1 });
          skuSummaryInitialised = true;
        })
        .catch(() => {
          return datasetPromise
            .then((dataset) => {
              if (!dataset) {
                throw new Error('Dataset is unavailable');
              }
              const effectiveDataset = buildFilteredDataset(dataset, columnFilters);
              const pivotData = buildSkuSummaryPivotFromDataset(effectiveDataset);
              skuSummaryPivotCache = pivotData;
              skuSummaryCurrentPage = 1;
              skuSummaryTotalRows = Array.isArray(pivotData?.rows) ? pivotData.rows.length : 0;
              renderSkuSummaryTable(pivotData, { page: 1 });
              skuSummaryInitialised = true;
            })
            .catch((error) => {
              const message = error && error.message ? error.message : 'Unable to load SKU summary';
              renderSkuSummaryMessage(message);
            });
        })
        .finally(() => {
          setTabPanelLoading('sku-summary', false);
        });
    }

    function updateSkuSummaryPage(direction) {
      if (!skuSummaryPivotCache) {
        return;
      }
      const totalPages = Math.max(1, Math.ceil((skuSummaryTotalRows || 0) / (skuSummaryPageSize || 1)));
      if (direction === 'previous') {
        if (skuSummaryCurrentPage <= 1) {
          return;
        }
        const targetPage = skuSummaryCurrentPage - 1;
        renderSkuSummaryTable(skuSummaryPivotCache, { page: targetPage });
      } else if (direction === 'next') {
        if (skuSummaryCurrentPage >= totalPages) {
          return;
        }
        const targetPage = skuSummaryCurrentPage + 1;
        renderSkuSummaryTable(skuSummaryPivotCache, { page: targetPage });
      }
    }

    function updateDashboardPivotFilterVisualState(state) {
      if (!state) {
        return;
      }
      updateDashboardPivotRowFilterButtonState(state);
    }

    function renderBaseDashboardPivot(state, config) {
      if (!state || !config) {
        return false;
      }
      const baseDataset = state.dataset || dashboardPivotSourceDataset || null;
      const pivotMap = mainDashboardPivotCache instanceof Map ? mainDashboardPivotCache : null;
      let basePivot = pivotMap ? pivotMap.get(config.id) : null;
      if (!basePivot || basePivot.error) {
        try {
          basePivot = buildDashboardPivot(baseDataset, config);
        } catch (error) {
          console.error(`Failed to rebuild dashboard pivot for ${config.id}:`, error);
          basePivot = null;
        }
      }
      if (!basePivot) {
        return false;
      }
      renderDashboardPivotTable(config, basePivot, {
        datasetOverride: state.activeDataset || baseDataset,
        skipFilterApplication: true,
      });
      return true;
    }

    function applyDashboardPivotFilter(config) {
      if (!config) {
        return;
      }
      const state = dashboardPivotFilterState.get(config.id);
      if (!state) {
        return;
      }
      const rowFilterSelection = state.rowFilterSelection instanceof Set ? state.rowFilterSelection : null;
      const activeFieldId = state.activeSelectionFieldId || null;
      const hasRowFilter = rowFilterSelection instanceof Set && rowFilterSelection.size > 0 && activeFieldId;
      const fieldDefinition = hasRowFilter
        ? state.fieldDefinitions.find((definition) => definition.id === activeFieldId)
        : null;
      const columnIndex = fieldDefinition && Number.isInteger(fieldDefinition.columnIndex) ? fieldDefinition.columnIndex : null;
      const isColumnFilter = Boolean(
        hasRowFilter
          && fieldDefinition
          && fieldDefinition.type === 'column'
          && columnIndex !== null
      );
      let tableElement = document.getElementById(config.tableId);
      if (!tableElement) {
        updateDashboardPivotFilterVisualState(state);
        return;
      }
      if (!hasRowFilter) {
        if (state.filteredDataset) {
          state.filteredDataset = null;
          state.activeDataset = state.dataset || dashboardPivotSourceDataset || null;
          renderBaseDashboardPivot(state, config);
        }
        updateDashboardPivotFilterVisualState(state);
        return;
      }
      if (isColumnFilter) {
        const baseDataset = state.dataset || dashboardPivotSourceDataset || null;
        const filteredDataset = filterDashboardDatasetBySelection(baseDataset, columnIndex, rowFilterSelection);
        state.filteredDataset = filteredDataset;
        state.activeDataset = filteredDataset;
        let filteredPivot = null;
        try {
          filteredPivot = buildDashboardPivot(filteredDataset, config);
        } catch (error) {
          console.error(`Failed to apply dashboard filter for ${config.id}:`, error);
          filteredPivot = { columns: [], rows: [], totalRow: [] };
        }
        renderDashboardPivotTable(config, filteredPivot, {
          datasetOverride: filteredDataset,
          skipFilterApplication: true,
        });
        updateDashboardPivotFilterVisualState(state);
        return;
      }
      const hadFilteredDataset = Boolean(state.filteredDataset);
      state.filteredDataset = null;
      state.activeDataset = state.dataset || dashboardPivotSourceDataset || null;
      if (hadFilteredDataset) {
        renderBaseDashboardPivot(state, config);
        tableElement = document.getElementById(config.tableId);
        if (!tableElement) {
          updateDashboardPivotFilterVisualState(state);
          return;
        }
      }
      const tbody = tableElement.tBodies && tableElement.tBodies.length ? tableElement.tBodies[0] : null;
      let visibleRowCount = 0;
      let dataRowCount = 0;
      const rowAttributes = Array.isArray(state.rowAttributes) ? state.rowAttributes : [];
      const dataset = state.activeDataset || state.dataset || dashboardPivotSourceDataset;
      if (tbody) {
        Array.from(tbody.rows).forEach((row, rowIndex) => {
          const cellCount = row.cells ? row.cells.length : 0;
          const messageCell = cellCount === 1 ? row.cells[0] : null;
          const isMessageRow = Boolean(messageCell && messageCell.classList.contains('dashboard-table__message'));
          if (isMessageRow) {
            row.hidden = false;
            return;
          }
          if (!cellCount) {
            row.hidden = Boolean(hasRowFilter);
            return;
          }
          dataRowCount += 1;
          const labelCell = row.cells[0];
          const labelText = labelCell && labelCell.textContent ? labelCell.textContent : '';
          let shouldShow = true;
          if (hasRowFilter) {
            if (activeFieldId === 'row') {
              const normalizedLabel = normalizeDashboardPivotLabel(labelText);
              shouldShow = rowFilterSelection.has(normalizedLabel);
            } else {
              const metadata = rowAttributes[rowIndex] || null;
              if (!metadata || !dataset || columnIndex === null) {
                shouldShow = false;
              } else {
                shouldShow = doesDashboardPivotRowMatchDatasetFilter(metadata, dataset, columnIndex, rowFilterSelection);
              }
            }
          }
          row.hidden = !shouldShow;
          if (shouldShow) {
            visibleRowCount += 1;
          }
        });
      }
      const tfoot = tableElement.tFoot;
      if (tfoot) {
        if (!hasRowFilter || visibleRowCount > 0) {
          tfoot.style.removeProperty('display');
        } else {
          tfoot.style.display = 'none';
        }
      }
      updateDashboardPivotFilterVisualState(state);
    }

    function initializeDashboardPivotFilters() {
      if (dashboardPivotFiltersInitialised) {
        return;
      }
      initializeDashboardPivotRowFilterDialog();
      DASHBOARD_PIVOT_CONFIGS.forEach((config) => {
        const rowFilterButton = document.getElementById(`dashboard-${config.id}-row-filter-button`);
        const rowFilterClearButton = document.getElementById(`dashboard-${config.id}-row-filter-clear`);
        const definitionsSource = Array.isArray(config.filterFieldDefinitions) && config.filterFieldDefinitions.length
          ? config.filterFieldDefinitions
          : DASHBOARD_PIVOT_FILTER_FIELDS;
        const fieldDefinitions = definitionsSource.map((definition) => ({ ...definition }));
        const state = {
          id: config.id,
          config,
          rowFilterSelection: null,
          activeSelectionFieldId: null,
          fieldSelections: new Map(),
          fieldValueMap: new Map(),
          rowAttributes: [],
          dataset: null,
          activeDataset: null,
          filteredDataset: null,
          rowFilterButton: rowFilterButton || null,
          rowFilterClearButton: rowFilterClearButton || null,
          fieldDefinitions,
          activeFieldId: fieldDefinitions.length ? fieldDefinitions[0].id : null,
        };
        dashboardPivotFilterState.set(config.id, state);
        dashboardPivotFilterTableMap.set(config.tableId, state);
        if (state.rowFilterButton) {
          state.rowFilterButton.addEventListener('click', () => {
            openDashboardPivotRowFilter(state, state.rowFilterButton);
          });
        }
        if (state.rowFilterClearButton) {
          state.rowFilterClearButton.hidden = true;
          state.rowFilterClearButton.setAttribute('disabled', 'true');
          state.rowFilterClearButton.addEventListener('click', () => {
            state.rowFilterSelection = null;
            state.activeSelectionFieldId = null;
            if (state.fieldSelections instanceof Map) {
              state.fieldSelections.clear();
            }
            if (activeDashboardPivotRowFilterState === state) {
              closeDashboardPivotRowFilter({ returnFocus: false });
            }
            dashboardPivotRowFilterPendingSelection = null;
            applyDashboardPivotFilter(config);
          });
        }
        updateDashboardPivotRowFilterButtonState(state);
      });
      dashboardPivotFiltersInitialised = true;
    }

    function setDashboardTableMessage(tableId, message, columnCount = 2) {
      const tableElement = document.getElementById(tableId);
      if (!tableElement) {
        return;
      }
      const span = Math.max(1, columnCount);
      tableElement.innerHTML = '';
      const tbody = tableElement.createTBody();
      const row = document.createElement('tr');
      const cell = document.createElement('td');
      cell.colSpan = span;
      cell.className = 'dashboard-table__message';
      cell.textContent = message;
      row.appendChild(cell);
      tbody.appendChild(row);
      const pivotFilterState = dashboardPivotFilterTableMap.get(tableId);
      if (pivotFilterState && pivotFilterState.config) {
        pivotFilterState.fieldValueMap = new Map();
        pivotFilterState.fieldSelections = new Map();
        pivotFilterState.rowAttributes = [];
        pivotFilterState.dataset = null;
        pivotFilterState.rowFilterSelection = null;
        pivotFilterState.activeSelectionFieldId = null;
        if (activeDashboardPivotRowFilterState === pivotFilterState) {
          closeDashboardPivotRowFilter({ returnFocus: false });
        }
        updateDashboardPivotRowFilterButtonState(pivotFilterState);
        applyDashboardPivotFilter(pivotFilterState.config);
      }
    }

    function renderDashboardPivotTable(config, pivot, options = {}) {
      const { datasetOverride = null, skipFilterApplication = false } = options;
      const tableElement = document.getElementById(config.tableId);
      if (!tableElement) {
        return;
      }
      const columns = Array.isArray(pivot?.columns) ? pivot.columns : [];
      if (!columns.length) {
        setDashboardTableMessage(config.tableId, 'No data available', (config.columns?.length || 0) + 1);
        return;
      }
      const rows = Array.isArray(pivot?.rows) ? pivot.rows : [];
      const totalRow = Array.isArray(pivot?.totalRow) ? pivot.totalRow : null;

      tableElement.innerHTML = '';
      const thead = tableElement.createTHead();
      const headRow = document.createElement('tr');
      columns.forEach((label) => {
        const th = document.createElement('th');
        th.scope = 'col';
        th.textContent = typeof label === 'string' ? label : '';
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);

      const tbody = tableElement.createTBody();
      if (!rows.length) {
        const emptyRow = document.createElement('tr');
        const emptyCell = document.createElement('td');
        emptyCell.colSpan = columns.length;
        emptyCell.className = 'dashboard-table__message';
        emptyCell.textContent = 'No data available';
        emptyRow.appendChild(emptyCell);
        tbody.appendChild(emptyRow);
      } else {
        rows.forEach((row) => {
          const tr = document.createElement('tr');
          row.forEach((value, index) => {
            const cellTag = index === 0 ? 'th' : 'td';
            const cell = document.createElement(cellTag);
            if (index === 0) {
              cell.scope = 'row';
            } else if (typeof value === 'string' && value.trim().startsWith('#')) {
              cell.classList.add('dashboard-table__value--error');
            }
            cell.textContent = value === null || value === undefined ? '' : String(value);
            tr.appendChild(cell);
          });
          tbody.appendChild(tr);
        });
      }

      const pivotFilterState = dashboardPivotFilterState.get(config.id);
      if (pivotFilterState) {
        const valueMap = new Map();
        rows.forEach((row) => {
          if (!Array.isArray(row) || !row.length) {
            return;
          }
          const rawLabel = row[0];
          const label = rawLabel === null || rawLabel === undefined ? '' : String(rawLabel);
          const key = normalizeDashboardPivotLabel(label);
          if (!valueMap.has(key)) {
            valueMap.set(key, { key, label, lower: label.toLowerCase() });
          }
        });
        const availableValues = Array.from(valueMap.values());
        setDashboardPivotFieldOptions(pivotFilterState, 'row', availableValues);
        if (!(pivotFilterState.fieldSelections instanceof Map)) {
          pivotFilterState.fieldSelections = new Map();
        }
        const sanitizedRowSelection = sanitizeDashboardPivotFieldSelection(pivotFilterState, 'row');
        if (sanitizedRowSelection instanceof Set) {
          pivotFilterState.fieldSelections.set('row', new Set(sanitizedRowSelection));
          if (pivotFilterState.activeSelectionFieldId === 'row') {
            pivotFilterState.rowFilterSelection = new Set(sanitizedRowSelection);
          }
        } else {
          pivotFilterState.fieldSelections.set('row', null);
          if (pivotFilterState.activeSelectionFieldId === 'row') {
            pivotFilterState.activeSelectionFieldId = null;
            pivotFilterState.rowFilterSelection = null;
          }
        }
        pivotFilterState.rowAttributes = Array.isArray(pivot?.rowAttributes)
          ? pivot.rowAttributes.map((entry) => ({ ...entry }))
          : [];
        const baseDataset = dashboardPivotSourceDataset || null;
        pivotFilterState.dataset = baseDataset;
        if (datasetOverride) {
          pivotFilterState.activeDataset = datasetOverride;
        } else if (!pivotFilterState.filteredDataset) {
          pivotFilterState.activeDataset = baseDataset;
        }
        updateDashboardPivotRowFilterButtonState(pivotFilterState);
        if (activeDashboardPivotRowFilterState === pivotFilterState) {
          if (dashboardPivotRowFilterPendingSelection instanceof Set) {
            const entry = getDashboardPivotFieldEntry(pivotFilterState, pivotFilterState.activeFieldId || 'row');
            const keySet = entry.keySet instanceof Set
              ? entry.keySet
              : new Set(entry.options.map((option) => option.key));
            const sanitizedPending = new Set();
            dashboardPivotRowFilterPendingSelection.forEach((key) => {
              if (keySet.has(key)) {
                sanitizedPending.add(key);
              }
            });
            if (keySet.size > 0 && sanitizedPending.size >= keySet.size) {
              dashboardPivotRowFilterPendingSelection = null;
            } else {
              dashboardPivotRowFilterPendingSelection = sanitizedPending;
            }
          } else if (dashboardPivotRowFilterPendingSelection !== null) {
            dashboardPivotRowFilterPendingSelection = null;
          }
          renderDashboardPivotRowFilterFieldButtons(pivotFilterState);
          renderDashboardPivotRowFilterOptions();
        }
      }

      if (totalRow && totalRow.length === columns.length) {
        const tfoot = tableElement.createTFoot();
        const tr = document.createElement('tr');
        tr.className = 'dashboard-table__total';
        totalRow.forEach((value, index) => {
          const cellTag = index === 0 ? 'th' : 'td';
          const cell = document.createElement(cellTag);
          if (index === 0) {
            cell.scope = 'row';
          } else if (typeof value === 'string' && value.trim().startsWith('#')) {
            cell.classList.add('dashboard-table__value--error');
          }
          cell.textContent = value === null || value === undefined ? '' : String(value);
          tr.appendChild(cell);
        });
        tfoot.appendChild(tr);
      }
      if (skipFilterApplication) {
        if (pivotFilterState) {
          updateDashboardPivotFilterVisualState(pivotFilterState);
        }
      } else {
        applyDashboardPivotFilter(config);
      }
    }

    function buildDashboardPivot(dataset, config) {
      if (!dataset || !Array.isArray(dataset.columns) || !Array.isArray(dataset.rows)) {
        return { columns: [], rows: [], totalRow: [] };
      }
      const normalise = (value) => (typeof value === 'string' ? value.trim().toLowerCase() : '');
      const columnLookup = new Map();
      dataset.columns.forEach((column, index) => {
        const key = normalise(column);
        if (key && !columnLookup.has(key)) {
          columnLookup.set(key, index);
        }
      });
      const groupIndex = columnLookup.get(normalise(config.groupColumn));
      if (groupIndex === undefined) {
        throw new Error(`Column "${config.groupColumn}" not found in Main dataset`);
      }
      const missingColumns = [];
      const includedColumns = [];
      const columnDescriptors = [];
      config.columns.forEach((column) => {
        if (!column) {
          return;
        }
        if (column.computed && column.computed.type === 'ratio') {
          const numeratorName = column.computed.numerator;
          const denominatorName = column.computed.denominator;
          const numeratorIndex = columnLookup.get(normalise(numeratorName));
          if (numeratorIndex === undefined) {
            missingColumns.push(
              `Column "${numeratorName}" not found in Main dataset (required for computed column "${column.source || column.header}")`
            );
            return;
          }
          const denominatorIndex = columnLookup.get(normalise(denominatorName));
          if (denominatorIndex === undefined) {
            missingColumns.push(
              `Column "${denominatorName}" not found in Main dataset (required for computed column "${column.source || column.header}")`
            );
            return;
          }
          includedColumns.push(column);
          columnDescriptors.push({
            type: 'ratio',
            numeratorIndex,
            denominatorIndex,
            numeratorSource: numeratorName,
            denominatorSource: denominatorName,
          });
          return;
        }
        const index = columnLookup.get(normalise(column.source));
        if (index === undefined) {
          missingColumns.push(`Column "${column.source}" not found in Main dataset`);
          return;
        }
        includedColumns.push(column);
        columnDescriptors.push({ type: 'direct', datasetIndex: index });
      });
      if (missingColumns.length) {
        const label = config.displayLabel || config.id || 'pivot';
        console.warn(
          `Skipped ${missingColumns.length} column(s) while building ${label} pivot:\n${missingColumns.join('\n')}`
        );
      }
      if (!includedColumns.length) {
        return { columns: ['Row Labels'], rows: [], totalRow: [] };
      }
      const groups = [];
      const groupLookup = new Map();
      const normalizedTotal = TOTAL_ROW_LABEL.toLowerCase();

      dataset.rows.forEach((row, rowIndex) => {
        const rawLabel = row[groupIndex];
        if (isPlaceholderValue(rawLabel)) {
          return;
        }
        const label = String(rawLabel).trim();
        if (!label || label.toLowerCase() === normalizedTotal) {
          return;
        }
        const key = label.toLowerCase();
        let entry = groupLookup.get(key);
        if (!entry) {
          entry = {
            label,
            metrics: columnDescriptors.map((descriptor) => {
              if (descriptor && descriptor.type === 'ratio') {
                return {
                  numerator: 0,
                  denominator: 0,
                  numeratorHasValue: false,
                  denominatorHasValue: false,
                };
              }
              return { sum: 0, hasValue: false, hasError: false, errorValue: '', textValue: '' };
            }),
            rowIndexes: new Set(),
          };
          groupLookup.set(key, entry);
          groups.push(entry);
        }
        entry.rowIndexes.add(rowIndex);
        includedColumns.forEach((column, columnIndex) => {
          const descriptor = columnDescriptors[columnIndex];
          if (!descriptor) {
            return;
          }
          const metric = entry.metrics[columnIndex];
          if (!metric) {
            return;
          }
          if (descriptor.type === 'ratio') {
            const numeratorRaw = descriptor.numeratorIndex < row.length ? row[descriptor.numeratorIndex] : null;
            const denominatorRaw = descriptor.denominatorIndex < row.length ? row[descriptor.denominatorIndex] : null;
            const numeratorValue = parseNumericValue(numeratorRaw);
            if (numeratorValue !== null) {
              metric.numerator += numeratorValue;
              metric.numeratorHasValue = true;
            }
            const denominatorValue = parseNumericValue(denominatorRaw);
            if (denominatorValue !== null) {
              metric.denominator += denominatorValue;
              metric.denominatorHasValue = true;
            }
            return;
          }
          const datasetColumnIndex = descriptor.datasetIndex;
          if (datasetColumnIndex === undefined || datasetColumnIndex >= row.length) {
            return;
          }
          const rawValue = row[datasetColumnIndex];
          if (isPlaceholderValue(rawValue)) {
            return;
          }
          const numericValue = parseNumericValue(rawValue);
          if (numericValue !== null) {
            metric.sum += numericValue;
            metric.hasValue = true;
          } else if (typeof rawValue === 'string') {
            const trimmed = rawValue.trim();
            if (!trimmed) {
              return;
            }
            if (trimmed.startsWith('#')) {
              metric.hasError = true;
              metric.errorValue = trimmed;
            } else if (!metric.textValue) {
              metric.textValue = trimmed;
            }
          }
        });
      });

      const columns = ['Row Labels', ...includedColumns.map((column) => column.header)];
      const rows = groups.map((entry) => {
        const values = [entry.label];
        entry.metrics.forEach((metric, columnIndex) => {
          const descriptor = columnDescriptors[columnIndex];
          const column = includedColumns[columnIndex];
          if (descriptor && descriptor.type === 'ratio') {
            values.push(formatDashboardRatioValue(metric, column));
            return;
          }
          if (metric.hasValue) {
            values.push(formatCellValue(metric.sum, column.source));
          } else if (metric.hasError) {
            values.push(metric.errorValue || '#DIV/0!');
          } else if (metric.textValue) {
            values.push(metric.textValue);
          } else {
            values.push('');
          }
        });
        return values;
      });

      let totalRow = null;
      if (rows.length) {
        totalRow = ['Grand Total'];
        includedColumns.forEach((column, columnIndex) => {
          const descriptor = columnDescriptors[columnIndex];
          if (descriptor && descriptor.type === 'ratio') {
            let numeratorSum = 0;
            let denominatorSum = 0;
            let numeratorHasValue = false;
            let denominatorHasValue = false;
            groups.forEach((entry) => {
              const metric = entry.metrics[columnIndex];
              if (!metric) {
                return;
              }
              if (metric.numeratorHasValue) {
                numeratorHasValue = true;
                numeratorSum += metric.numerator;
              }
              if (metric.denominatorHasValue) {
                denominatorHasValue = true;
                denominatorSum += metric.denominator;
              }
            });
            const aggregateMetric = {
              numerator: numeratorSum,
              denominator: denominatorSum,
              numeratorHasValue,
              denominatorHasValue,
            };
            totalRow.push(formatDashboardRatioValue(aggregateMetric, column));
            return;
          }
          let aggregate = 0;
          let hasValue = false;
          let errorValue = '';
          let textValue = '';
          groups.forEach((entry) => {
            const metric = entry.metrics[columnIndex];
            if (!metric) {
              return;
            }
            if (metric.hasValue) {
              aggregate += metric.sum;
              hasValue = true;
              return;
            }
            if (!errorValue && metric.hasError) {
              errorValue = metric.errorValue || '#DIV/0!';
            }
            if (!textValue && metric.textValue) {
              textValue = metric.textValue;
            }
          });
          if (hasValue) {
            totalRow.push(formatCellValue(aggregate, column.source));
          } else if (errorValue) {
            totalRow.push(errorValue);
          } else if (textValue) {
            totalRow.push(textValue);
          } else {
            totalRow.push('');
          }
        });
      }

      const rowAttributes = groups.map((entry) => ({
        label: entry.label,
        key: entry.label ? entry.label.toLowerCase() : '',
        rowIndexes: entry.rowIndexes instanceof Set ? Array.from(entry.rowIndexes) : [],
      }));

      return { columns, rows, totalRow, rowAttributes };
    }

    function filterDashboardDatasetBySelection(dataset, columnIndex, selectionSet) {
      if (!dataset || !Array.isArray(dataset.columns) || !Array.isArray(dataset.rows)) {
        return dataset;
      }
      if (!(selectionSet instanceof Set) || selectionSet.size === 0) {
        return dataset;
      }
      if (!Number.isInteger(columnIndex) || columnIndex < 0 || columnIndex >= dataset.columns.length) {
        return dataset;
      }
      const normalizedSelection = new Set();
      selectionSet.forEach((value) => {
        const normalizedValue = normalizeDashboardFilterValue(value);
        normalizedSelection.add(normalizedValue);
      });
      if (!normalizedSelection.size) {
        return dataset;
      }
      const columnName = dataset.columns[columnIndex] || '';
      const filteredRows = dataset.rows.filter((row) => {
        if (!Array.isArray(row)) {
          return false;
        }
        const rawValue = columnIndex < row.length ? row[columnIndex] : '';
        const formattedValue = formatCellValue(rawValue, columnName);
        const normalizedValue = normalizeDashboardFilterValue(formattedValue);
        return normalizedSelection.has(normalizedValue);
      });
      return {
        ...dataset,
        rows: filteredRows,
      };
    }

    function buildDashboardPivotResultsFromDataset(dataset) {
      dashboardPivotSourceDataset = dataset || null;
      updateDashboardPivotFilterModel(dataset || null);
      const results = new Map();
      DASHBOARD_PIVOT_CONFIGS.forEach((config) => {
        try {
          results.set(config.id, buildDashboardPivot(dataset, config));
        } catch (error) {
          const message = error && error.message ? error.message : 'Unable to build dashboard view';
          results.set(config.id, { error: message });
        }
      });
      return results;
    }

    function renderMainDashboard(pivotMap) {
      DASHBOARD_PIVOT_CONFIGS.forEach((config) => {
        const pivot = pivotMap instanceof Map ? pivotMap.get(config.id) : null;
        if (pivot && !pivot.error) {
          renderDashboardPivotTable(config, pivot);
        } else {
          const message = pivot && pivot.error ? pivot.error : 'No data available';
          setDashboardTableMessage(config.tableId, message, (config.columns?.length || 0) + 1);
        }
      });
    }

    function loadMainDashboard() {
      const filtersInitiallyActive = hasActiveColumnFilters(mainColumnFilters);
      if (mainDashboardInitialised && mainDashboardPivotCache instanceof Map) {
        renderMainDashboard(mainDashboardPivotCache);
        updateDashboardSummaryTable();
        setTabPanelLoading('dashboard', false);
        return;
      }
      const loadingMessage = filtersInitiallyActive ? 'Updating dashboard…' : 'Loading dashboard…';
      setTabPanelLoading('dashboard', true, loadingMessage);
      DASHBOARD_PIVOT_CONFIGS.forEach((config) => {
        setDashboardTableMessage(config.tableId, 'Loading data…', (config.columns?.length || 0) + 1);
      });
      updateDashboardSummaryTable();

      const loadFromDataset = () => {
        const datasetPromise = mainDatasetCache ? Promise.resolve(mainDatasetCache) : fetchMainDataset();
        return datasetPromise.then((dataset) => {
          if (!dataset) {
            throw new Error('Dataset is unavailable');
          }
          ensureMainFilterSetup(dataset);
          const filtersActive = hasActiveColumnFilters(mainColumnFilters);
          const effectiveDataset = filtersActive
            ? buildFilteredDataset(dataset, mainColumnFilters, { formatOptions: MAIN_TABLE_FORMAT_OPTIONS })
            : dataset;
          const results = buildDashboardPivotResultsFromDataset(effectiveDataset);
          return { results, filtersActiveAtBuild: filtersActive };
        });
      };

      const sourcePromise = filtersInitiallyActive
        ? loadFromDataset()
        : fetchDashboardPivotSections().then((results) => ({ results, filtersActiveAtBuild: false }));

      return sourcePromise
        .then(({ results, filtersActiveAtBuild }) => {
          if (!(results instanceof Map)) {
            throw new Error('Dashboard data is unavailable');
          }
          if (hasActiveColumnFilters(mainColumnFilters) !== filtersActiveAtBuild) {
            return;
          }
          mainDashboardPivotCache = results;
          mainDashboardInitialised = true;
          renderMainDashboard(results);
        })
        .catch((dashboardError) => {
          console.error('Failed to build dashboard pivots from Main worksheet:', dashboardError);
          return loadFromDataset()
            .then(({ results, filtersActiveAtBuild }) => {
              if (!(results instanceof Map)) {
                throw new Error('Dashboard data is unavailable');
              }
              if (hasActiveColumnFilters(mainColumnFilters) !== filtersActiveAtBuild) {
                return;
              }
              mainDashboardPivotCache = results;
              mainDashboardInitialised = true;
              renderMainDashboard(results);
            })
            .catch((error) => {
              const message = error && error.message ? error.message : 'Unable to load data';
              DASHBOARD_PIVOT_CONFIGS.forEach((config) => {
                setDashboardTableMessage(config.tableId, message, (config.columns?.length || 0) + 1);
              });
              const mainFilterButton = document.getElementById('main-filter-button');
              if (mainFilterButton) {
                mainFilterButton.setAttribute('disabled', 'true');
                mainFilterButton.setAttribute('aria-hidden', 'true');
                mainFilterButton.setAttribute('aria-expanded', 'false');
                mainFilterButton.dataset.active = 'false';
              }
            });
        })
        .finally(() => {
          setTabPanelLoading('dashboard', false);
        });
    }

    const skuSummaryPaginationElement = document.getElementById('sku-summary-pagination');
    if (skuSummaryPaginationElement) {
      skuSummaryPaginationElement.addEventListener('click', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLElement)) {
          return;
        }
        if (target.matches('button[data-action]')) {
          const action = target.getAttribute('data-action');
          if (action === 'previous' || action === 'next') {
            updateSkuSummaryPage(action);
          }
        }
      });
    }

    function renderLoMessage(message) {
      const escaped = escapeHtml(message);
      const markup = `<tbody><tr><td class="cell-date" colspan="1">${escaped}</td></tr></tbody>`;
      const salesTable = document.getElementById('lo-sales-table');
      const spendTable = document.getElementById('lo-spend-table');
      const platformSalesTable = document.getElementById('platform-sales-table');
      const platformNetTable = document.getElementById('platform-net-table');
      if (salesTable) {
        salesTable.innerHTML = markup;
      }
      if (spendTable) {
        spendTable.innerHTML = markup;
      }
      if (platformSalesTable) {
        platformSalesTable.innerHTML = markup;
      }
      if (platformNetTable) {
        platformNetTable.innerHTML = markup;
      }
      requestAnimationFrame(() => resizeLoTableContainers());
    }

    function initializePlatformTables(dataset) {
      if (!dataset || !Array.isArray(dataset.rows)) {
        updatePlatformTablesWithDataset({ columns: [], rows: [] });
        platformTablesInitialised = true;
        return;
      }
      const effectiveDataset = buildFilteredDataset(dataset, columnFilters);
      updatePlatformTablesWithDataset(effectiveDataset);
      platformTablesInitialised = true;
    }

    function initializeLoTables(dataset) {
      if (!regularFilterInitialised) {
        const augmentedForFilters = augmentDatasetWithTotals(dataset);
        columnValueOptions = buildColumnOptions(augmentedForFilters);
        initializeRegularFilterControls(augmentedForFilters);
      }
      if (loTablesInitialised) {
        const effectiveDataset = buildFilteredDataset(dataset, columnFilters);
        updateLoTablesWithDataset(effectiveDataset);
        if (platformTablesInitialised) {
          updatePlatformTablesWithDataset(effectiveDataset);
        }
        return;
      }
      const salesTable = document.getElementById('lo-sales-table');
      const spendTable = document.getElementById('lo-spend-table');
      const salesPivot = buildLoPivot(dataset, 'total revenue');
      loSalesOrderCache = Array.isArray(salesPivot.order) ? salesPivot.order.slice() : [];
      loDisplayNameOverridesCache = salesPivot.displayNames instanceof Map
        ? new Map(salesPivot.displayNames)
        : new Map();
      renderLoTable(salesTable, salesPivot);
      requestAnimationFrame(() => resizeLoTableContainers());
      if (spendTable) {
        const loadingColumns = Math.max(1, (salesPivot?.loList?.length ?? 0) + 1);
        spendTable.innerHTML = `<tbody><tr><td class="cell-date" colspan="${loadingColumns}">Loading…</td></tr></tbody>`;
      }
      const adSpendPivot = buildLoPivot(dataset, 'Ad Spend', {
        normalizedOrder: loSalesOrderCache,
        displayNameOverrides: loDisplayNameOverridesCache,
      });
      loBaselineAdSpendPivot = adSpendPivot;
      fetchSpendPivot()
        .then((pivotData) => {
          const alignedPivot = alignPivotToReference(pivotData, salesPivot);
          loBaselineSpendPivot = alignedPivot;
          loSpendScalingData = buildSpendScaling(alignedPivot, adSpendPivot);
          renderLoTable(spendTable, alignedPivot);
          requestAnimationFrame(() => resizeLoTableContainers());
        })
        .catch((error) => {
          if (!spendTable) {
            return;
          }
          loSpendScalingData = null;
          if (adSpendPivot && Array.isArray(adSpendPivot.rows) && adSpendPivot.rows.length) {
            loBaselineSpendPivot = adSpendPivot;
            renderLoTable(spendTable, adSpendPivot);
          } else {
            const baseColumnCount = Array.isArray(salesPivot?.loList) ? salesPivot.loList.length + 1 : 1;
            const message = escapeHtml(error.message || 'Unable to load spend pivot');
            spendTable.innerHTML = `<tbody><tr><td class="cell-date" colspan="${baseColumnCount}">${message}</td></tr></tbody>`;
          }
          requestAnimationFrame(() => resizeLoTableContainers());
        });
      loTablesInitialised = true;
      initializePlatformTables(dataset);
    }

    function loadRegularTable() {
      const isRefresh = regularTableInitialised;
      const loadingMessage = isRefresh ? 'Refreshing regular data…' : 'Loading regular data…';
      setTabPanelLoading('regular', true, loadingMessage);
      return fetchRegularDataset({ forceReload: isRefresh })
        .then((dataset) => {
          updateStickyOffset();

          const augmentedDataset = augmentDatasetWithTotals(dataset);
          regularTableAugmentedDataset = augmentedDataset;
          totalColumnIndex = augmentedDataset.totalColumnIndex;
          regularCheckoutColumnIndex = augmentedDataset.columns.findIndex((column) => column && column.trim().toLowerCase() === 'checkout');

          const columns = augmentedDataset.columns.map((title, columnIndex) => ({
            title,
            data: columnIndex,
            render(data, type) {
              const datasetRef = regularTableAugmentedDataset || augmentedDataset;
              const value = data === undefined || data === null ? '' : data;
              const columnName = datasetRef.columns[columnIndex];
              const isCheckoutColumn = columnIndex === regularCheckoutColumnIndex;
              const isDateColumn = isCheckoutColumn || isDateColumnName(columnName);
              if (type === 'sort') {
                if (isDateColumn) {
                  if (typeof window.sortKeyYYYYMMDD === 'function') {
                    return window.sortKeyYYYYMMDD(value);
                  }
                  const parsed = parseDateValue(value);
                  return parsed ? parsed.toISOString() : '';
                }
                const numericValue = parseNumericValue(value);
                if (numericValue !== null) {
                  return numericValue;
                }
                return typeof value === 'string' ? value.toLowerCase() : value ?? '';
              }
              if (type === 'display' || type === 'filter') {
                if (isDateColumn) {
                  return formatDateValue(value);
                }
                return formatCellValue(value, columnName);
              }
              return value;
            },
          }));

          const tableData = augmentedDataset.rows;
          const productColumnIndex = augmentedDataset.columns.indexOf('Product');
          const quantityColumnIndex = augmentedDataset.columns.findIndex((column) => column && column.trim().toLowerCase() === 'qty');
          const numericColumnIndices = augmentedDataset.numericColumnIndices;

          regularTableFooterValues = SHOW_REGULAR_TOTAL_ROW
            ? buildFormattedFooterValues(augmentedDataset)
            : [];
          regularTableNumericColumnSet = SHOW_REGULAR_TOTAL_ROW
            ? new Set(numericColumnIndices)
            : new Set();

          if (SHOW_REGULAR_TOTAL_ROW) {
            const tableElement = document.getElementById('regularTable');
            if (tableElement) {
              ensureTableFooter(
                tableElement,
                augmentedDataset.columns.length,
                regularTableFooterValues,
                regularTableNumericColumnSet
              );
            }
          }

          const ensureCheckoutAssertion = () => {
            if (!regularTable || regularCheckoutColumnIndex < 0) {
              return;
            }
            const tableNode = typeof regularTable.table === 'function' ? regularTable.table().node() : null;
            if (!tableNode) {
              return;
            }
            const rows = tableNode.querySelectorAll('tbody tr');
            if (!rows || rows.length === 0) {
              return;
            }
            const firstRow = rows[0];
            const checkoutCell = firstRow && regularCheckoutColumnIndex < firstRow.cells.length
              ? firstRow.cells[regularCheckoutColumnIndex]
              : null;
            if (!checkoutCell) {
              return;
            }
            const text = (checkoutCell.textContent || '').trim();
            console.assert(/^[0-9]{2}-[0-9]{2}-[0-9]{4}$/.test(text), 'Checkout not dd-mm-yyyy');
          };

          columnValueOptions = buildColumnOptions(augmentedDataset);
          if (!regularFilterInitialised) {
            initializeRegularFilterControls(augmentedDataset);
          } else {
            refreshRegularFilterOptions(augmentedDataset);
          }

          if (!regularTableInitialised) {
            const columnClassMap = new Map();
            const addColumnClass = (columnIndex, className) => {
              if (columnIndex < 0) {
                return;
              }
              if (!columnClassMap.has(columnIndex)) {
                columnClassMap.set(columnIndex, new Set());
              }
              columnClassMap.get(columnIndex).add(className);
            };

            addColumnClass(productColumnIndex, 'cell-product');
            addColumnClass(quantityColumnIndex, 'cell-qty');
            numericColumnIndices.forEach((columnIndex) => addColumnClass(columnIndex, 'cell-numeric'));

            const columnDefs = Array.from(columnClassMap.entries()).map(([columnIndex, classSet]) => ({
              targets: Number(columnIndex),
              className: Array.from(classSet).join(' '),
            }));

            const initialRowCount = Math.min(augmentedDataset.rows.length, REGULAR_TABLE_PAGE_LENGTH);
            const initialReservedSpace = calculateRegularTableReservedSpace();
            const initialScrollHeight = `${calculateScrollBodyHeight(initialRowCount, undefined, initialReservedSpace)}px`;
            regularTable = $('#regularTable').DataTable({
              data: tableData,
              columns,
              columnDefs,
              scrollX: true,
              scrollY: initialScrollHeight,
              scrollCollapse: true,
              pageLength: REGULAR_TABLE_PAGE_LENGTH,
              lengthChange: false,
              order: [],
              info: false,
              language: {
                emptyTable: 'No regular data available',
              },
              dom: 't<"regular-table__footer"p>'
            });
            wireHeaderEvents(regularTable, { allowSorting: true });
            applyTableHeight(regularTable);
            if (SHOW_REGULAR_TOTAL_ROW) {
              updateRegularTableFooter(regularTable);
            }
            regularTable.on('draw.dt', () => {
              wireHeaderEvents(regularTable, { allowSorting: true });
              applyTableHeight(regularTable);
              if (SHOW_REGULAR_TOTAL_ROW) {
                updateRegularTableFooter(regularTable);
              }
              moveRegularTablePagination();
              ensureCheckoutAssertion();
            });

            regularTableInitialised = true;
            requestAnimationFrame(() => refreshRegularTableLayout());
            console.assert($('#regularTable').length, 'Regular table not found');
            moveRegularTablePagination();
          } else if (regularTable) {
            regularTable.clear();
            regularTable.rows.add(tableData);
            augmentedDataset.columns.forEach((title, index) => {
              const headerCell = regularTable.column(index).header();
              if (headerCell) {
                headerCell.textContent = title || `Column ${index + 1}`;
              }
            });
            regularTable.columns.adjust();
            regularTable.draw(false);
            if (SHOW_REGULAR_TOTAL_ROW) {
              updateRegularTableFooter(regularTable);
            }
            requestAnimationFrame(() => refreshRegularTableLayout());
          }

          ensureCheckoutAssertion();
          if (!isRefresh) {
            setTimeout(ensureCheckoutAssertion, 500);
          }
        })
        .catch((error) => {
          const tableElement = document.getElementById('regularTable');
          if (!regularTableInitialised && tableElement) {
            tableElement.outerHTML = `<p style="color: var(--muted);">${error.message}</p>`;
          }
          console.error('Regular tab load failed:', error);
        })
        .finally(() => {
          setTabPanelLoading('regular', false);
        });
    }

    async function initRegularTab() {
      try {
        await loadRegularTable();
      } catch (error) {
        console.error('Regular tab load failed:', error);
      }
    }

    function loadMainTable() {
      if (mainTableInitialised) {
        return Promise.resolve();
      }
      setTabPanelLoading('main', true, 'Loading main data…');
      return fetchMainDataset()
        .then((dataset) => {
          updateStickyOffset();
          const augmentedDataset = augmentDatasetWithTotals(dataset);
          mainTableAugmentedDataset = augmentedDataset;
          mainTotalColumnIndex = augmentedDataset.totalColumnIndex;
          const tableElement = document.getElementById('main-table');
          applyHeaderPreambleToTable(tableElement, dataset.headerPreamble || null, augmentedDataset.columns);
          mainTillDateColumnIndices = [];
          mainTillDateHeaders = [];
          augmentedDataset.columns.forEach((columnName, columnIndex) => {
            if (typeof columnName === 'string' && TILL_DATE_HEADER_PATTERN.test(columnName)) {
              mainTillDateColumnIndices.push(columnIndex);
              mainTillDateHeaders.push(columnName);
            }
          });
          const columns = augmentedDataset.columns.map((title) => ({ title }));
          const formattedRows = augmentedDataset.rows.map((row) => row.map((value, index) => formatCellValue(
            value,
            augmentedDataset.columns[index],
            MAIN_TABLE_FORMAT_OPTIONS
          )));
          const productColumnIndex = augmentedDataset.columns.indexOf('Product');
          const quantityColumnIndex = augmentedDataset.columns.findIndex((column) => column && column.trim().toLowerCase() === 'qty');
          const numericColumnIndices = augmentedDataset.numericColumnIndices;

          mainTableFooterValues = SHOW_REGULAR_TOTAL_ROW
            ? buildFormattedFooterValues(augmentedDataset, { formatOptions: MAIN_TABLE_FORMAT_OPTIONS })
            : [];
          mainTableNumericColumnSet = SHOW_REGULAR_TOTAL_ROW
            ? new Set(numericColumnIndices)
            : new Set();

          if (SHOW_REGULAR_TOTAL_ROW) {
            const tableElement = document.getElementById('main-table');
            if (tableElement) {
              ensureTableFooter(
                tableElement,
                augmentedDataset.columns.length,
                mainTableFooterValues,
                mainTableNumericColumnSet,
                mainTotalColumnIndex
              );
            }
          }

          const columnClassMap = new Map();
          const addColumnClass = (columnIndex, className) => {
            if (columnIndex < 0) {
              return;
            }
            if (!columnClassMap.has(columnIndex)) {
              columnClassMap.set(columnIndex, new Set());
            }
            columnClassMap.get(columnIndex).add(className);
          };

          addColumnClass(productColumnIndex, 'cell-product');
          addColumnClass(quantityColumnIndex, 'cell-qty');
          numericColumnIndices.forEach((columnIndex) => addColumnClass(columnIndex, 'cell-numeric'));

          const columnDefs = Array.from(columnClassMap.entries()).map(([columnIndex, classSet]) => ({
            targets: Number(columnIndex),
            className: Array.from(classSet).join(' '),
          }));

          const initialRowCount = Math.min(augmentedDataset.rows.length, REGULAR_TABLE_PAGE_LENGTH);
          const initialReservedSpace = calculateRegularTableReservedSpace();
          const initialScrollHeight = `${calculateScrollBodyHeight(initialRowCount, undefined, initialReservedSpace)}px`;
          mainTable = $('#main-table').DataTable({
            data: formattedRows,
            columns,
            columnDefs,
            scrollX: true,
            scrollY: initialScrollHeight,
            scrollCollapse: true,
            deferRender: true,
            autoWidth: true,
            order: [],
            paging: true,
            pageLength: REGULAR_TABLE_PAGE_LENGTH,
            lengthChange: false,
            info: false,
            dom: 't<"main-table__footer"p>'
          });

          moveMainTablePagination();
          mainColumnValueOptions = buildColumnOptions(augmentedDataset, { formatOptions: MAIN_TABLE_FORMAT_OPTIONS });
          initializeMainFilterControls(augmentedDataset);
          wireHeaderEvents(mainTable, {
            valueOptions: mainColumnValueOptions,
            filters: mainColumnFilters,
            onChange: handleMainFilterChange,
            totalIndex: mainTotalColumnIndex,
          });
          applyTableHeight(mainTable);
          if (SHOW_REGULAR_TOTAL_ROW) {
            updateMainTableFooter(mainTable);
          }
          mainTable.on('draw.dt', () => {
            wireHeaderEvents(mainTable, {
              valueOptions: mainColumnValueOptions,
              filters: mainColumnFilters,
              onChange: handleMainFilterChange,
              totalIndex: mainTotalColumnIndex,
            });
            applyTableHeight(mainTable);
            if (SHOW_REGULAR_TOTAL_ROW) {
              updateMainTableFooter(mainTable);
            }
            moveMainTablePagination();
            verifyMainTillDateCells(mainTable);
          });

          mainTableInitialised = true;
          requestAnimationFrame(() => refreshMainTableLayout());
          logMainTillDateHeaders();
          verifyMainTillDateCells(mainTable);
        })
        .catch((error) => {
          console.error('Failed to initialise Main table:', error);
          const tableElement = document.getElementById('main-table');
          if (tableElement) {
            tableElement.outerHTML = `<p style="color: var(--muted);">${error.message}</p>`;
          }
          const filterButton = document.getElementById('main-filter-button');
          if (filterButton) {
            filterButton.setAttribute('disabled', 'true');
            filterButton.setAttribute('aria-hidden', 'true');
            filterButton.setAttribute('aria-expanded', 'false');
            filterButton.dataset.active = 'false';
          }
          if (mainFilterContainerElement) {
            closeMainFilter({ returnFocus: false });
            mainFilterContainerElement.setAttribute('hidden', '');
            mainFilterContainerElement.setAttribute('aria-hidden', 'true');
          }
        })
        .finally(() => {
          setTabPanelLoading('main', false);
        });
    }

    if (typeof document !== 'undefined') {
      if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initRegularTab, { once: true });
      } else {
        initRegularTab();
      }
    }
