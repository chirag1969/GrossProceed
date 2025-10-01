"""Analyze REGULAR SEP-25 sheet from the Gross Proceed workbook.

This script extracts the data from the Excel file without relying on
third-party dependencies such as pandas. It parses the XML that backs
the XLSX file, normalises the header names, loads the data into an
in-memory SQLite database and computes a set of KPIs alongside the data
series needed for charts.

Outputs:
* analysis/regular_sep25_metrics.json -- KPI summary and aggregated series
* analysis/index.html                 -- Standalone HTML dashboard with charts
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET

EXCEL_EPOCH = datetime(1899, 12, 30)


@dataclass
class ColumnSpec:
    header_key: str
    sql_name: str
    sql_type: str
    converter: callable


BAD_VALUES = {None, "", "-", "#N/A", "#REF!", "#VALUE!", "#DIV/0!"}
BLANK_ROW_LIMIT = 2000


def clean_text(value) -> Optional[str]:
    if value in BAD_VALUES:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return str(value)


def clean_numeric(value) -> Optional[float]:
    if value in BAD_VALUES:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def excel_serial_to_date(value) -> Optional[str]:
    number = clean_numeric(value)
    if number is None:
        return None
    try:
        days = int(number)
    except (TypeError, ValueError):
        return None
    return (EXCEL_EPOCH + timedelta(days=days)).date().isoformat()


def column_letters_to_index(value: str) -> int:
    result = 0
    for char in value:
        result = result * 26 + (ord(char) - 64)
    return result - 1


COLUMN_SPECS: Sequence[ColumnSpec] = [
    ColumnSpec("ORDER NO", "order_no", "TEXT", clean_text),
    ColumnSpec("Plain Order No", "plain_order_no", "TEXT", clean_text),
    ColumnSpec("Checkout", "checkout_date", "TEXT", excel_serial_to_date),
    ColumnSpec("Checkout", "checkout_serial", "REAL", clean_numeric),
    ColumnSpec("Platform", "platform", "TEXT", clean_text),
    ColumnSpec("Product", "product", "TEXT", clean_text),
    ColumnSpec("CATEGORY", "category", "TEXT", clean_text),
    ColumnSpec("SKU", "sku_parent", "TEXT", clean_text),
    ColumnSpec("SKU_2", "sku", "TEXT", clean_text),
    ColumnSpec("Qty", "quantity", "REAL", clean_numeric),
    ColumnSpec("Sale Price", "sale_price", "REAL", clean_numeric),
    ColumnSpec("Sale Price + Shipping", "sale_price_plus_shipping", "REAL", clean_numeric),
    ColumnSpec("Net Sale Price", "net_sale_price", "REAL", clean_numeric),
    ColumnSpec("Shipping PAID", "shipping_paid", "REAL", clean_numeric),
    ColumnSpec("Tax", "tax", "REAL", clean_numeric),
    ColumnSpec("Total Revenue", "total_revenue", "REAL", clean_numeric),
    ColumnSpec("CARRIER FEE", "carrier_fee", "REAL", clean_numeric),
    ColumnSpec("Ebay/ Amazon", "marketplace_fee", "REAL", clean_numeric),
    ColumnSpec("Gross Proceed", "gross_proceed", "REAL", clean_numeric),
    ColumnSpec("P.COST", "product_cost", "REAL", clean_numeric),
    ColumnSpec("T.P.COST", "total_product_cost", "REAL", clean_numeric),
    ColumnSpec("NET", "net", "REAL", clean_numeric),
    ColumnSpec("NET/Q", "net_per_qty", "REAL", clean_numeric),
    ColumnSpec("TYPE", "order_type", "TEXT", clean_text),
    ColumnSpec("T.Net/T.Rev", "net_to_revenue_ratio", "REAL", clean_numeric),
    ColumnSpec("N2R/Q", "net_to_revenue_per_qty", "REAL", clean_numeric),
    ColumnSpec("Customer City", "customer_city", "TEXT", clean_text),
    ColumnSpec("Customer State", "customer_state", "TEXT", clean_text),
    ColumnSpec("Customer Zip", "customer_zip", "TEXT", clean_text),
    ColumnSpec("Warehouse", "warehouse", "TEXT", clean_text),
    ColumnSpec("Data From", "data_from", "TEXT", clean_text),
    ColumnSpec("LISTING OWNER", "listing_owner", "TEXT", clean_text),
    ColumnSpec("AVG. SALES/MONTH", "avg_sales_per_month", "REAL", clean_numeric),
    ColumnSpec("RANK", "rank", "REAL", clean_numeric),
    ColumnSpec("selling fees", "selling_fees", "REAL", clean_numeric),
    ColumnSpec("fba fees", "fba_fees", "REAL", clean_numeric),
    ColumnSpec("FREIGHT CHARGES", "freight_charges", "REAL", clean_numeric),
    ColumnSpec("T.FREIGHT", "total_freight", "REAL", clean_numeric),
    ColumnSpec("promotional rebates", "promotional_rebates", "REAL", clean_numeric),
    ColumnSpec("Carrier Name", "carrier_name", "TEXT", clean_text),
    ColumnSpec("State Sort", "state_sort", "TEXT", clean_text),
    ColumnSpec("Country", "country", "TEXT", clean_text),
    ColumnSpec("C/R", "customer_type", "TEXT", clean_text),
    ColumnSpec("Price-Promo", "price_promo", "REAL", clean_numeric),
    ColumnSpec("Min Price", "min_price", "REAL", clean_numeric),
    ColumnSpec("NEW/OLD", "condition_flag", "TEXT", clean_text),
    ColumnSpec("REFUND", "refund", "REAL", clean_numeric),
    ColumnSpec("SHIPPING TAX", "shipping_tax", "REAL", clean_numeric),
    ColumnSpec("GIFT WRAP TAX", "gift_wrap_tax", "REAL", clean_numeric),
    ColumnSpec("Count", "line_count", "REAL", clean_numeric),
    ColumnSpec("Coupon Fee", "coupon_fee", "REAL", clean_numeric),
    ColumnSpec("Replacement", "replacement", "TEXT", clean_text),
    ColumnSpec("Ad Spend", "ad_spend", "REAL", clean_numeric),
    ColumnSpec("Strorage Fees", "storage_fees", "REAL", clean_numeric),
    ColumnSpec("Final Net", "final_net", "REAL", clean_numeric),
    ColumnSpec("Final Net%", "final_net_pct", "REAL", clean_numeric),
    ColumnSpec("Filter Store", "filter_store", "TEXT", clean_text),
    ColumnSpec("Org Sale Price", "original_sale_price", "REAL", clean_numeric),
    ColumnSpec("Conv. Rate", "conversion_rate", "REAL", clean_numeric),
    ColumnSpec("Aged Inv.", "aged_inventory", "TEXT", clean_text),
    ColumnSpec("Late Delivery", "late_delivery", "TEXT", clean_text),
]


def parse_sheet(path: Path, sheet_rel_path: str = "xl/worksheets/sheet13.xml") -> Tuple[List[str], List[Dict[str, object]]]:
    with zipfile.ZipFile(path) as archive:
        shared_strings = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for si in root:
                text = "".join(t.text or "" for t in si.iter() if t.tag.endswith("}t"))
                shared_strings.append(text)

        sheet_stream = BytesIO(archive.read(sheet_rel_path))

    cell_ref_pattern = re.compile(r"([A-Z]+)(\d+)")

    def col_to_index(col_letters: str) -> int:
        exp = 0
        for c in col_letters:
            exp = exp * 26 + (ord(c) - 64)
        return exp - 1

    header: Optional[List[str]] = None
    header_unique: Optional[List[str]] = None
    rows: List[Dict[str, object]] = []

    current_cells: Dict[int, object] = {}
    current_cell: Optional[Dict[str, object]] = None
    current_row_has_data = False
    blank_run = 0

    for event, elem in ET.iterparse(sheet_stream, events=("start", "end")):
        tag = elem.tag.split("}")[-1]
        if event == "start":
            if tag == "row":
                current_cells = {}
                current_row_has_data = False
            elif tag == "c":
                current_cell = {
                    "r": elem.attrib.get("r"),
                    "t": elem.attrib.get("t"),
                    "value": None,
                    "inline": None,
                }
        elif event == "end":
            if tag == "v":
                if current_cell is not None:
                    current_cell["value"] = elem.text
            elif tag == "is":
                if current_cell is not None:
                    current_cell["inline"] = "".join(
                        t.text or "" for t in elem.iter() if t.tag.endswith("}t")
                    )
            elif tag == "c":
                if current_cell is not None and current_cell["r"]:
                    match = cell_ref_pattern.match(current_cell["r"])
                    if match:
                        column_index = col_to_index(match.group(1))
                        cell_type = current_cell.get("t")
                        text = current_cell.get("value")
                        value = None
                        if cell_type == "s" and text is not None:
                            value = shared_strings[int(text)]
                        elif cell_type == "b" and text is not None:
                            value = bool(int(text))
                        elif cell_type == "str" and text is not None:
                            value = text
                        elif text is not None:
                            txt = str(text)
                            try:
                                if any(ch in txt for ch in (".", "E", "e")):
                                    value = float(txt)
                                else:
                                    value = int(txt)
                            except ValueError:
                                value = txt
                        elif current_cell.get("inline") is not None:
                            value = current_cell["inline"]
                        current_cells[column_index] = value
                        current_row_has_data = True
                current_cell = None
                elem.clear()
            elif tag == "row":
                if current_row_has_data:
                    max_col = max(current_cells) if current_cells else -1
                    row_values = [current_cells.get(i) for i in range(max_col + 1)] if max_col >= 0 else []
                    if header is None:
                        if any(val == "ORDER NO" for val in row_values):
                            header = row_values
                            counts: Dict[str, int] = {}
                            header_unique = []
                            for name in header:
                                base = name or "Column"
                                count = counts.get(base, 0) + 1
                                counts[base] = count
                                if count == 1 and name:
                                    header_unique.append(base)
                                else:
                                    header_unique.append(f"{base}_{count}")
                            blank_run = 0
                        else:
                            blank_run += 1
                    elif header_unique is not None:
                        if len(row_values) < len(header_unique):
                            row_values += [None] * (len(header_unique) - len(row_values))
                        record = {
                            header_unique[i]: row_values[i] if i < len(row_values) else None
                            for i in range(len(header_unique))
                        }
                        key_candidates = (
                            record.get("ORDER NO"),
                            record.get("Order No"),
                            record.get("Plain Order No"),
                        )
                        if any(val not in (None, "", "-") for val in key_candidates):
                            rows.append(record)
                            blank_run = 0
                        else:
                            blank_run += 1
                else:
                    blank_run += 1
                if header is not None and blank_run > BLANK_ROW_LIMIT:
                    break
                elem.clear()

    if header_unique is None:
        raise ValueError("Failed to locate header row with 'ORDER NO'.")

    return header_unique, rows


def extract_lo_spend_pivot(path: Path, sheet_rel_path: str = "xl/worksheets/sheet9.xml") -> Dict[str, object]:
    with zipfile.ZipFile(path) as archive:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for si in root:
                text = "".join(t.text or "" for t in si.iter() if t.tag.endswith("}t"))
                shared_strings.append(text)
        sheet_root = ET.fromstring(archive.read(sheet_rel_path))

    ns = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows: List[Dict[str, object]] = []
    for row in sheet_root.findall("ns:sheetData/ns:row", ns):
        row_data: Dict[str, object] = {}
        for cell in row.findall("ns:c", ns):
            ref = cell.attrib.get("r")
            if not ref:
                continue
            match = re.match(r"([A-Z]+)(\d+)", ref)
            if not match:
                continue
            column_letters = match.group(1)
            cell_type = cell.attrib.get("t")
            value: Optional[object] = None
            text_value = None
            node = cell.find("ns:v", ns)
            if node is not None:
                text_value = node.text
            if cell_type == "s" and text_value is not None:
                try:
                    value = shared_strings[int(text_value)]
                except (ValueError, IndexError):
                    value = None
            elif text_value is not None:
                try:
                    value = float(text_value)
                except (TypeError, ValueError):
                    value = text_value
            else:
                inline = cell.find("ns:is/ns:t", ns)
                value = inline.text if inline is not None else None
            row_data[column_letters] = value
        rows.append(row_data)

    if len(rows) <= 4:
        return {"loList": [], "rows": []}

    header_row = rows[3]
    owner_columns: List[Tuple[str, str]] = []
    start_index = column_letters_to_index("AA")
    for column_letters, raw_label in header_row.items():
        index = column_letters_to_index(column_letters)
        if index < start_index:
            continue
        if not isinstance(raw_label, str):
            continue
        label = raw_label.strip()
        if not label or label.lower() in {"row labels", "grand total"} or label == "(blank)":
            continue
        owner_columns.append((column_letters, label))

    owner_columns.sort(key=lambda item: column_letters_to_index(item[0]))
    owner_labels = [label for _, label in owner_columns]

    rows_data: List[Dict[str, object]] = []
    for row in rows[4:]:
        pivot_key = row.get("Y")
        if isinstance(pivot_key, str) and pivot_key.strip().lower() == "grand total":
            break
        pivot_numeric = clean_numeric(pivot_key)
        if pivot_numeric is None:
            continue
        iso_date = excel_serial_to_date(pivot_numeric)
        if not iso_date:
            continue
        display_date = iso_date.split("-")[-1]
        formatted_values: List[str] = []
        for column_letters, _ in owner_columns:
            raw_value = row.get(column_letters)
            numeric_value = clean_numeric(raw_value) or 0.0
            formatted_values.append(f"{numeric_value:,.2f}")
        rows_data.append({"displayDate": display_date, "formattedValues": formatted_values})

    return {"loList": owner_labels, "rows": rows_data}


def normalise_key(label: str, existing: set[str]) -> str:
    base = re.sub(r"[^0-9a-zA-Z]+", "_", label.strip().lower()).strip("_")
    if not base:
        base = "value"
    key = base
    counter = 2
    while key in existing:
        key = f"{base}_{counter}"
        counter += 1
    existing.add(key)
    return key


def infer_column_type(label: str) -> str:
    normalised = label.strip().lower()
    if "qty" in normalised or "quantity" in normalised:
        return "integer"
    return "decimal"


def extract_sku_summary_pivot(
    path: Path, sheet_rel_path: str = "xl/worksheets/sheet11.xml"
) -> Dict[str, object]:
    with zipfile.ZipFile(path) as archive:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for si in root:
                text = "".join(t.text or "" for t in si.iter() if t.tag.endswith("}t"))
                shared_strings.append(text)
        sheet_root = ET.fromstring(archive.read(sheet_rel_path))

    ns = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

    def cell_value(cell: ET.Element) -> Optional[object]:
        cell_type = cell.attrib.get("t")
        node = cell.find("ns:v", ns)
        text_value = node.text if node is not None else None
        if cell_type == "s" and text_value is not None:
            try:
                return shared_strings[int(text_value)]
            except (ValueError, IndexError):
                return None
        if text_value is not None:
            try:
                return float(text_value)
            except (TypeError, ValueError):
                return text_value
        inline = cell.find("ns:is/ns:t", ns)
        return inline.text if inline is not None else None

    header_columns: List[Dict[str, object]] = []
    header_keys: set[str] = set()
    rows_data: List[Dict[str, object]] = []
    totals: Dict[str, object] = {}

    header_row_index = 4

    for row in sheet_root.findall("ns:sheetData/ns:row", ns):
        row_index = int(row.attrib.get("r", "0"))
        cell_map: Dict[str, object] = {}
        for cell in row.findall("ns:c", ns):
            ref = cell.attrib.get("r")
            if not ref:
                continue
            match = re.match(r"([A-Z]+)(\d+)", ref)
            if not match:
                continue
            column_letters = match.group(1)
            cell_map[column_letters] = cell_value(cell)

        if row_index < header_row_index:
            continue

        if row_index == header_row_index:
            for column_letters, raw_label in sorted(
                cell_map.items(), key=lambda item: column_letters_to_index(item[0])
            ):
                if column_letters == "A":
                    continue
                if not isinstance(raw_label, str):
                    continue
                label = raw_label.strip()
                if not label:
                    continue
                key = normalise_key(label, header_keys)
                header_columns.append(
                    {
                        "letters": column_letters,
                        "label": label,
                        "key": key,
                        "type": infer_column_type(label),
                    }
                )
            continue

        row_label = cell_map.get("A")
        if isinstance(row_label, str) and row_label.strip().lower() == "grand total":
            totals = {"sku": "Grand Total"}
            for column in header_columns:
                raw_value = cell_map.get(column["letters"])
                totals[column["key"]] = clean_numeric(raw_value)
            break

        if row_label is None:
            continue

        sku_label = str(row_label).strip()
        if not header_columns:
            continue

        row_entry: Dict[str, object] = {"sku": sku_label}
        has_numeric = False
        for column in header_columns:
            raw_value = cell_map.get(column["letters"])
            numeric_value = clean_numeric(raw_value)
            row_entry[column["key"]] = numeric_value
            if numeric_value not in (None, 0.0):
                has_numeric = True
        if not has_numeric and all(row_entry.get(column["key"]) is None for column in header_columns):
            continue
        rows_data.append(row_entry)

    columns = [
        {"key": "sku", "label": "SKU", "type": "string"},
    ] + [
        {"key": column["key"], "label": column["label"], "type": column["type"]}
        for column in header_columns
    ]

    if totals and "sku" not in totals:
        totals["sku"] = "Grand Total"

    return {"columns": columns, "rows": rows_data, "totals": totals}


def build_sqlite(records: Iterable[Dict[str, object]]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    columns_sql = ", ".join(f"{spec.sql_name} {spec.sql_type}" for spec in COLUMN_SPECS)
    cursor.execute(f"CREATE TABLE regular_sales ({columns_sql})")

    insert_sql = f"INSERT INTO regular_sales ({', '.join(spec.sql_name for spec in COLUMN_SPECS)}) VALUES ({', '.join('?' for _ in COLUMN_SPECS)})"

    for record in records:
        row = []
        for spec in COLUMN_SPECS:
            raw_value = record.get(spec.header_key)
            row.append(spec.converter(raw_value))
        cursor.execute(insert_sql, row)

    conn.commit()
    return conn


def fetch_metrics(conn: sqlite3.Connection) -> Dict[str, object]:
    cursor = conn.cursor()
    metrics: Dict[str, object] = {}

    def scalar(query: str) -> float:
        value = cursor.execute(query).fetchone()[0]
        return value or 0.0

    total_orders = scalar("SELECT COUNT(DISTINCT order_no) FROM regular_sales")
    total_units = scalar("SELECT SUM(quantity) FROM regular_sales")
    total_revenue = scalar("SELECT SUM(total_revenue) FROM regular_sales")
    gross_proceed = scalar("SELECT SUM(gross_proceed) FROM regular_sales")
    final_net = scalar("SELECT SUM(final_net) FROM regular_sales")
    ad_spend = scalar("SELECT SUM(ad_spend) FROM regular_sales")

    average_order_value = total_revenue / total_orders if total_orders else 0.0
    net_margin = final_net / total_revenue if total_revenue else 0.0
    gross_margin = gross_proceed / total_revenue if total_revenue else 0.0

    metrics.update(
        total_orders=int(total_orders),
        total_units=float(total_units),
        total_revenue=total_revenue,
        gross_proceed=gross_proceed,
        final_net=final_net,
        ad_spend=ad_spend,
        average_order_value=average_order_value,
        net_margin=net_margin,
        gross_margin=gross_margin,
    )
    return metrics


def fetch_series(conn: sqlite3.Connection) -> Dict[str, List[Dict[str, object]]]:
    cursor = conn.cursor()

    daily = cursor.execute(
        """
        SELECT checkout_date, SUM(total_revenue) AS revenue, SUM(final_net) AS final_net
        FROM regular_sales
        WHERE checkout_date IS NOT NULL
        GROUP BY checkout_date
        ORDER BY checkout_date
        """
    ).fetchall()

    categories = cursor.execute(
        """
        SELECT category, SUM(final_net) AS final_net
        FROM regular_sales
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY final_net DESC
        LIMIT 10
        """
    ).fetchall()

    top_skus = cursor.execute(
        """
        SELECT sku, SUM(quantity) AS units, SUM(final_net) AS final_net
        FROM regular_sales
        WHERE sku IS NOT NULL
        GROUP BY sku
        ORDER BY final_net DESC
        LIMIT 10
        """
    ).fetchall()

    stores = cursor.execute(
        """
        SELECT filter_store, SUM(total_revenue) AS revenue, SUM(final_net) AS final_net
        FROM regular_sales
        WHERE filter_store IS NOT NULL
        GROUP BY filter_store
        ORDER BY revenue DESC
        """
    ).fetchall()

    return {
        "daily_performance": [
            {"date": row[0], "revenue": row[1], "final_net": row[2]} for row in daily
        ],
        "category_final_net": [
            {"category": row[0], "final_net": row[1]} for row in categories
        ],
        "top_sku_final_net": [
            {"sku": row[0], "units": row[1], "final_net": row[2]} for row in top_skus
        ],
        "store_mix": [
            {"store": row[0], "revenue": row[1], "final_net": row[2]} for row in stores
        ],
    }


def render_dashboard(metrics: Dict[str, object], series: Dict[str, List[Dict[str, object]]], output_path: Path) -> None:
    import html

    daily = series["daily_performance"]
    categories = series["category_final_net"]

    def json_dumps(obj) -> str:
        return json.dumps(obj, separators=(",", ":"))

    html_content = f"""
<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>REGULAR SEP-25 Performance Dashboard</title>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>
  <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap\" rel=\"stylesheet\">
  <script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.6/dist/chart.umd.min.js\"></script>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4f6fb;
      --card-bg: #fff;
      --text: #1b1e28;
      --muted: #5f677b;
      --accent: #145afc;
      font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
    }}
    header {{
      padding: 2.5rem 3rem 1rem;
    }}
    h1 {{
      margin: 0 0 0.5rem;
      font-size: 2rem;
      font-weight: 700;
    }}
    p.lead {{
      margin: 0;
      color: var(--muted);
    }}
    .grid {{
      display: grid;
      gap: 1.5rem;
      padding: 0 3rem 3rem;
    }}
    .kpi-grid {{
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    }}
    .card {{
      background: var(--card-bg);
      border-radius: 1rem;
      padding: 1.5rem;
      box-shadow: 0 10px 30px rgba(16, 24, 40, 0.08);
    }}
    .kpi-title {{
      color: var(--muted);
      font-size: 0.9rem;
      margin-bottom: 0.35rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .kpi-value {{
      font-size: 1.75rem;
      font-weight: 600;
    }}
    canvas {{
      width: 100% !important;
      height: auto !important;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 0.5rem;
    }}
    th, td {{
      text-align: left;
      padding: 0.5rem 0.75rem;
    }}
    th {{
      color: var(--muted);
      font-size: 0.85rem;
      font-weight: 600;
      text-transform: uppercase;
    }}
    tr:nth-child(even) td {{
      background: rgba(20, 90, 252, 0.05);
    }}
  </style>
</head>
<body>
  <header>
    <h1>REGULAR SEP-25 Performance Dashboard</h1>
    <p class=\"lead\">Automated analysis generated from the Gross Proceed workbook.</p>
  </header>
  <section class=\"grid kpi-grid\">
    {''.join(render_kpi_card(label, metrics[key]) for label, key in [
        ("Total Orders", "total_orders"),
        ("Total Units", "total_units"),
        ("Total Revenue", "total_revenue"),
        ("Gross Proceed", "gross_proceed"),
        ("Final Net", "final_net"),
        ("Ad Spend", "ad_spend"),
        ("Average Order Value", "average_order_value"),
        ("Net Margin", "net_margin"),
    ])}
  </section>
  <section class=\"grid\">
    <div class=\"card\">
      <h2 style=\"margin-top:0\">Revenue &amp; Profit Trend</h2>
      <canvas id=\"daily-chart\"></canvas>
    </div>
    <div class=\"card\">
      <h2 style=\"margin-top:0\">Top Categories by Final Net</h2>
      <canvas id=\"category-chart\"></canvas>
    </div>
    <div class=\"card\">
      <h2 style=\"margin-top:0\">Store Mix</h2>
      {render_table(series['store_mix'], ['store', 'revenue', 'final_net'])}
    </div>
    <div class=\"card\">
      <h2 style=\"margin-top:0\">Top SKUs by Final Net</h2>
      {render_table(series['top_sku_final_net'], ['sku', 'units', 'final_net'])}
    </div>
  </section>
  <script>
    const dailySeries = {json_dumps(daily)};
    const categorySeries = {json_dumps(categories)};

    const fmtCurrency = (value) => new Intl.NumberFormat('en-US', {{ style: 'currency', currency: 'USD' }}).format(value);
    const fmtPercent = (value) => `${{(value * 100).toFixed(1)}}%`;

    const dailyCtx = document.getElementById('daily-chart');
    if (dailySeries.length > 0) {{
      new Chart(dailyCtx, {{
        type: 'line',
        data: {{
          labels: dailySeries.map(item => item.date),
          datasets: [
            {{
              label: 'Revenue',
              data: dailySeries.map(item => item.revenue),
              borderColor: '#145afc',
              backgroundColor: 'rgba(20, 90, 252, 0.15)',
              tension: 0.25,
              fill: true,
            }},
            {{
              label: 'Final Net',
              data: dailySeries.map(item => item.final_net),
              borderColor: '#ff7d4f',
              backgroundColor: 'rgba(255, 125, 79, 0.15)',
              tension: 0.25,
              fill: true,
            }}
          ]
        }},
        options: {{
          responsive: true,
          interaction: {{ mode: 'index', intersect: false }},
          stacked: false,
          scales: {{
            y: {{
              ticks: {{
                callback: (value) => fmtCurrency(value)
              }}
            }}
          }}
        }}
      }});
    }}

    const categoryCtx = document.getElementById('category-chart');
    if (categorySeries.length > 0) {{
      new Chart(categoryCtx, {{
        type: 'bar',
        data: {{
          labels: categorySeries.map(item => item.category),
          datasets: [{{
            label: 'Final Net',
            data: categorySeries.map(item => item.final_net),
            backgroundColor: '#145afc'
          }}]
        }},
        options: {{
          responsive: true,
          scales: {{
            y: {{
              ticks: {{ callback: (value) => fmtCurrency(value) }}
            }}
          }}
        }}
      }});
    }}
  </script>
</body>
</html>
"""

    output_path.write_text(html_content, encoding="utf-8")


def render_kpi_card(label: str, value: object) -> str:
    if isinstance(value, (int, float)):
        if "Margin" in label:
            display = f"{value * 100:.1f}%"
        elif label in {"Total Orders", "Total Units"}:
            display = f"{value:,.0f}"
        elif label in {"Total Revenue", "Gross Proceed", "Final Net", "Ad Spend", "Average Order Value"}:
            display = f"${value:,.2f}"
        else:
            display = f"{value:,.2f}"
    else:
        display = str(value)
    return f"""
    <article class=\"card\">
      <div class=\"kpi-title\">{label}</div>
      <div class=\"kpi-value\">{display}</div>
    </article>
    """


def render_table(rows: List[Dict[str, object]], keys: Sequence[str]) -> str:
    if not rows:
        return "<p>No data available.</p>"
    header = "".join(f"<th>{key.replace('_', ' ').title()}</th>" for key in keys)
    body_rows = []
    for row in rows:
        cells = []
        for key in keys:
            value = row.get(key)
            if isinstance(value, (int, float)) and key != "units":
                cells.append(f"<td>${value:,.2f}</td>")
            elif isinstance(value, float) and key == "units":
                cells.append(f"<td>{value:,.1f}</td>")
            else:
                cells.append(f"<td>{value if value is not None else ''}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"""
    <table>
      <thead><tr>{header}</tr></thead>
      <tbody>
        {''.join(body_rows)}
      </tbody>
    </table>
    """


def main() -> None:
    workbook_path = Path("09 GROSS PROCEED SEP-25 COMBINE.xlsx")
    _header, records = parse_sheet(workbook_path)
    conn = build_sqlite(records)
    metrics = fetch_metrics(conn)
    series = fetch_series(conn)

    output_dir = Path(__file__).resolve().parent
    metrics_path = output_dir / "regular_sep25_metrics.json"
    metrics_path.write_text(json.dumps({"metrics": metrics, "series": series}, indent=2), encoding="utf-8")

    spend_pivot_path = output_dir / "lo_spend_pivot.json"
    try:
        spend_pivot = extract_lo_spend_pivot(workbook_path)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Failed to extract LO spend pivot: {exc}")
    else:
        spend_pivot_path.write_text(json.dumps(spend_pivot, indent=2), encoding="utf-8")
        print(f"Saved LO spend pivot to {spend_pivot_path}")

    sku_pivot_path = output_dir / "sku_summary_pivot.json"
    try:
        sku_pivot = extract_sku_summary_pivot(workbook_path)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Failed to extract SKU summary pivot: {exc}")
    else:
        sku_pivot_path.write_text(json.dumps(sku_pivot, indent=2), encoding="utf-8")
        print(f"Saved SKU summary pivot to {sku_pivot_path}")

    dashboard_path = output_dir / "index.html"
    render_dashboard(metrics, series, dashboard_path)

    print(f"Saved metrics to {metrics_path}")
    print(f"Saved dashboard to {dashboard_path}")


if __name__ == "__main__":
    main()
