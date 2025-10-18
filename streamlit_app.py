"""Streamlit dashboard for exploring Gross Proceed workbook data."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import pandas as pd
import streamlit as st

from analysis.regular_sep25_analysis import (
    WORKBOOK_GLOB_PATTERN,
    find_workbook_path,
    parse_sheet,
)


st.set_page_config(page_title="Gross Proceed Explorer", layout="wide")


@st.cache_data(show_spinner=False)
def load_sales_data(search_root: Path) -> Dict[str, object]:
    """Load the regular sales worksheet and return the parsed data."""
    workbook_path = find_workbook_path(search_root, WORKBOOK_GLOB_PATTERN)
    header_keys, rows, sheet_name = parse_sheet(workbook_path)
    return {
        "workbook_path": workbook_path,
        "sheet_name": sheet_name,
        "header_keys": header_keys,
        "rows": rows,
    }


@st.cache_data(show_spinner=False)
def build_dataframe(rows: Iterable[Dict[str, object]], columns: Sequence[str]) -> pd.DataFrame:
    """Build a pandas DataFrame from the selected rows and columns."""
    dataframe = pd.DataFrame(list(rows))
    if columns:
        missing = [column for column in columns if column not in dataframe.columns]
        if missing:
            raise KeyError(f"Columns missing from dataframe: {missing}")
        dataframe = dataframe.loc[:, list(columns)]
    return dataframe


def format_column_label(key: str) -> str:
    """Return a human-friendly label for the given column key."""
    return key.replace("_", " ").title()


def main() -> None:
    st.title("Regular Sales Overview")
    st.caption(
        "Interactive explorer for the REGULAR SEP-25 worksheet from the Gross Proceed workbook."
    )

    try:
        data = load_sales_data(Path("."))
    except FileNotFoundError as exc:
        st.error(str(exc))
        return
    except Exception as exc:  # pylint: disable=broad-except
        st.exception(exc)
        return

    workbook_path: Path = data["workbook_path"]
    sheet_name: str | None = data.get("sheet_name")
    header_keys: List[str] = data["header_keys"]
    rows: List[Dict[str, object]] = data["rows"]

    st.markdown(
        (
            f"**Workbook:** `{workbook_path.name}`<br>"
            f"**Worksheet:** `{sheet_name or 'REGULAR SEP-25'}`"
        ),
        unsafe_allow_html=True,
    )

    default_columns = [
        key
        for key in (
            "ORDER NO",
            "CHECKOUT",
            "PLATFORM",
            "PRODUCT",
            "SKU_2",
            "QUANTITY",
            "TOTAL REVENUE",
            "GROSS PROCEED",
            "FINAL NET",
        )
        if key in header_keys
    ]

    with st.expander("Display options", expanded=True):
        selected_columns = st.multiselect(
            "Columns to display",
            options=header_keys,
            default=default_columns if default_columns else header_keys,
            format_func=format_column_label,
        )

        available_rows = len(rows) or 1
        max_slider_limit = max(1, min(5000, available_rows))
        default_slider_value = min(500, max_slider_limit)
        slider_step = 10 if max_slider_limit <= 200 else 50
        max_rows = st.slider(
            "Maximum rows to show",
            min_value=1,
            max_value=max_slider_limit,
            value=default_slider_value,
            step=slider_step,
        )

    filters_container = st.container()
    with filters_container:
        columns = st.columns(2)
        with columns[0]:
            platform_options = sorted(
                {
                    str(value)
                    for value in (row.get("PLATFORM") for row in rows)
                    if value not in (None, "")
                }
            )
            selected_platforms = st.multiselect(
                "Platform",
                options=platform_options,
                default=[],
            )
        with columns[1]:
            order_type_options = sorted(
                {
                    str(value)
                    for value in (row.get("TYPE") for row in rows)
                    if value not in (None, "")
                }
            )
            selected_order_types = st.multiselect(
                "Order type",
                options=order_type_options,
                default=[],
            )

    filtered_rows: List[Dict[str, object]] = []
    for row in rows:
        platform_value = row.get("PLATFORM")
        order_type_value = row.get("TYPE")
        platform = str(platform_value) if platform_value not in (None, "") else None
        order_type = str(order_type_value) if order_type_value not in (None, "") else None
        if selected_platforms and platform not in selected_platforms:
            continue
        if selected_order_types and order_type not in selected_order_types:
            continue
        filtered_rows.append(row)

    total_rows = len(filtered_rows)
    st.write(
        f"Displaying {min(max_rows, total_rows):,} of {total_rows:,} matching rows"
    )

    dataframe = build_dataframe(filtered_rows[:max_rows], selected_columns)
    st.dataframe(dataframe, use_container_width=True)

    csv_data = dataframe.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download visible rows as CSV",
        data=csv_data,
        file_name="regular_sales_filtered.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
