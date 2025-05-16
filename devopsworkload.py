import base64
import os
import sqlite3
import time
import urllib.parse

import pandas as pd
import requests
import streamlit as st

st.set_page_config(layout="wide")

st.markdown(
    """
    <style>
        .dataframe-container {
            max-width: 100% !important;
            width: 100% !important;
            overflow-x: auto;
        }
        .stDataFrame div[data-testid="stHorizontalBlock"] {
            max-width: 100vw;
        }
    </style>
""",
    unsafe_allow_html=True,
)

DB_FILE = "work_items.db"

ORGANIZATION_URL = "https://dev.azure.com/CNH-Data-Platform"
PROJECT_NAME = "Parts and Services Global"
PAT = "F8RUmUzIitICq7HYE50IJUCFma1JvK68532cSEHt7PGJcDdHwJ4SJQQJ99BEACAAAAALMXRYAAASAZDO29Om"
FIELDS_TO_DISPLAY = [
    "System.Id",
    "System.WorkItemType",
    "System.Title",
    "System.AssignedTo",
    "System.State",
    "System.Tags",
    "Microsoft.VSTS.Scheduling.StartDate",
    "Microsoft.VSTS.Scheduling.TargetDate",
]


def initialize_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS work_items (
                Id INTEGER PRIMARY KEY,
                WorkItemType TEXT,
                Title TEXT,
                AssignedTo TEXT,
                State TEXT,
                Tags TEXT,
                StartDate TEXT,
                TargetDate TEXT
            )
        """)


def load_data_from_db():
    with sqlite3.connect(DB_FILE) as conn:
        return pd.read_sql_query("SELECT * FROM work_items", conn)


def load_ids_from_db():
    with sqlite3.connect(DB_FILE) as conn:
        result = conn.execute("SELECT Id FROM work_items")
        return set(row[0] for row in result.fetchall())


def delete_ids_from_db(ids_to_delete):
    if not ids_to_delete:
        return
    with sqlite3.connect(DB_FILE) as conn:
        conn.executemany(
            "DELETE FROM work_items WHERE Id = ?", [(i,) for i in ids_to_delete]
        )
        conn.commit()


def save_data_to_db(df):
    with sqlite3.connect(DB_FILE) as conn:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT OR REPLACE INTO work_items (Id, WorkItemType, Title, AssignedTo, State, Tags, StartDate, TargetDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    row["Id"],
                    row["WorkItemType"],
                    row["Title"],
                    row["AssignedTo"],
                    row["State"],
                    row["Tags"],
                    row["StartDate"],
                    row["TargetDate"],
                ),
            )
        conn.commit()


def get_work_item_ids_from_devops():
    encoded_pat = base64.b64encode(f":{PAT}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_pat}",
    }
    your_display_name_for_query = "MORETTI Cristian (CNH Industrial)"
    wiql_query = f"""
    SELECT [System.Id]
    FROM WorkItems
    WHERE [System.TeamProject] = '{PROJECT_NAME}'
    AND [System.WorkItemType] IN ('Product Backlog Item')
    AND [System.AssignedTo] = '{your_display_name_for_query}'
    """
    query_payload = {"query": wiql_query}
    url = f"{ORGANIZATION_URL}/{urllib.parse.quote(PROJECT_NAME)}/_apis/wit/wiql?api-version=6.0"
    try:
        response = requests.post(url, headers=headers, json=query_payload)
        response.raise_for_status()
        work_item_ids = [item["id"] for item in response.json()["workItems"]]
        return work_item_ids
    except Exception as e:
        st.error(f"Error fetching work item IDs from DevOps: {e}")
        return []


def get_work_items_details_from_devops(work_item_ids):
    encoded_pat = base64.b64encode(f":{PAT}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_pat}",
    }
    work_items_data = []
    for work_item_id in work_item_ids:
        try:
            work_item_url = f"{ORGANIZATION_URL}/{urllib.parse.quote(PROJECT_NAME)}/_apis/wit/workitems/{work_item_id}?$expand=all&api-version=6.0"
            work_item_response = requests.get(work_item_url, headers=headers)
            work_item_response.raise_for_status()
            item_fields = work_item_response.json().get("fields", {})
            row_data = {}
            for field in FIELDS_TO_DISPLAY:
                value = item_fields.get(field)
                if (
                    field == "System.AssignedTo"
                    and isinstance(value, dict)
                    and "displayName" in value
                ):
                    row_data["AssignedTo"] = value["displayName"]
                else:
                    row_data[field.split(".")[-1]] = value
            work_items_data.append(row_data)
        except Exception as e:
            st.warning(f"Error fetching work item {work_item_id}: {e}")
    return pd.DataFrame(work_items_data)


def get_updated_work_items():
    all_ids = get_work_item_ids_from_devops()
    if not all_ids:
        return pd.DataFrame()
    existing_ids = load_ids_from_db()

    # Remove deleted items from DB
    ids_to_delete = existing_ids - set(all_ids)
    delete_ids_from_db(ids_to_delete)

    new_ids = [wid for wid in all_ids if wid not in existing_ids]

    message_placeholder = st.empty()

    if not new_ids:
        message_placeholder.info("No new work items to download.")
        time.sleep(4)
        message_placeholder.empty()
        return load_data_from_db()

    message_placeholder.info(f"Downloading {len(new_ids)} new work items...")
    new_data = get_work_items_details_from_devops(new_ids)
    if new_data.empty:
        message_placeholder.empty()
        return load_data_from_db()
    old_data = load_data_from_db()
    combined_data = pd.concat([old_data, new_data], ignore_index=True).drop_duplicates(
        subset=["Id"]
    )
    save_data_to_db(combined_data)
    message_placeholder.empty()
    return combined_data


def convert_date_column(df, col_name):
    if col_name in df.columns:
        return pd.to_datetime(df[col_name], errors="coerce")
    else:
        return pd.Series(dtype="datetime64[ns]")


def apply_filters(df):
    st.sidebar.header("Filters")
    filtered_df = df.copy()

    # Dropdown minimal for AssignedTo
    assigned_to_vals = sorted(df["AssignedTo"].dropna().unique())
    assigned_to_selection = st.sidebar.selectbox(
        "Filter by Assigned To", options=["All"] + assigned_to_vals
    )
    if assigned_to_selection != "All":
        filtered_df = filtered_df[filtered_df["AssignedTo"] == assigned_to_selection]

    # Dropdown minimal for State
    state_vals = sorted(df["State"].dropna().unique())
    state_selection = st.sidebar.selectbox(
        "Filter by State", options=["All"] + state_vals
    )
    if state_selection != "All":
        filtered_df = filtered_df[filtered_df["State"] == state_selection]

    # Remove the message "No completed tasks assigned to you." if state != "Done"
    if state_selection != "Done":
        # Optional: Remove or suppress the message from elsewhere in your code
        pass  # Just here to clarify no message should show

    # Convert date columns to datetime
    filtered_df["StartDate_dt"] = convert_date_column(filtered_df, "StartDate")
    filtered_df["TargetDate_dt"] = convert_date_column(filtered_df, "TargetDate")

    # Start Date filter
    min_start = filtered_df["StartDate_dt"].min()
    max_start = filtered_df["StartDate_dt"].max()

    if pd.notnull(min_start) and pd.notnull(max_start):
        min_start_val = (
            min_start.to_pydatetime()
            if hasattr(min_start, "to_pydatetime")
            else min_start
        )
        max_start_val = (
            max_start.to_pydatetime()
            if hasattr(max_start, "to_pydatetime")
            else max_start
        )

        if min_start_val == max_start_val:
            st.sidebar.write(f"Only one Start Date available: {min_start_val.date()}")
        else:
            start_date_filter = st.sidebar.slider(
                "Filter by Start Date",
                value=(min_start_val, max_start_val),
                min_value=min_start_val,
                max_value=max_start_val,
                format="YYYY-MM-DD",
            )
            filtered_df = filtered_df[
                (
                    (filtered_df["StartDate_dt"] >= start_date_filter[0])
                    & (filtered_df["StartDate_dt"] <= start_date_filter[1])
                )
                | (filtered_df["StartDate_dt"].isna())
            ]

    # Target Date filter
    min_target = filtered_df["TargetDate_dt"].min()
    max_target = filtered_df["TargetDate_dt"].max()

    if pd.notnull(min_target) and pd.notnull(max_target):
        min_target_val = (
            min_target.to_pydatetime()
            if hasattr(min_target, "to_pydatetime")
            else min_target
        )
        max_target_val = (
            max_target.to_pydatetime()
            if hasattr(max_target, "to_pydatetime")
            else max_target
        )

        if min_target_val == max_target_val:
            st.sidebar.write(f"Only one Target Date available: {min_target_val.date()}")
        else:
            target_date_filter = st.sidebar.slider(
                "Filter by Target Date",
                value=(min_target_val, max_target_val),
                min_value=min_target_val,
                max_value=max_target_val,
                format="YYYY-MM-DD",
            )
            filtered_df = filtered_df[
                (
                    (filtered_df["TargetDate_dt"] >= target_date_filter[0])
                    & (filtered_df["TargetDate_dt"] <= target_date_filter[1])
                )
                | (filtered_df["TargetDate_dt"].isna())
            ]

    filtered_df = filtered_df.drop(columns=["StartDate_dt", "TargetDate_dt"])

    return filtered_df


# --- App start ---

initialize_db()

st.title("DevOps Backlog Item Dashboard")

refresh = st.button("🔁 Refresh Data")

if refresh:
    with st.spinner("📡 Checking and downloading new data from Azure DevOps..."):
        data = get_updated_work_items()
elif not os.path.exists(DB_FILE) or load_data_from_db().empty:
    with st.spinner("📡 No local data found, downloading from Azure DevOps..."):
        data = get_updated_work_items()
else:
    with st.spinner("💾 Loading data from local database..."):
        data = load_data_from_db()

if not data.empty:
    data = apply_filters(data)

empty_col1, content_col, empty_col2 = st.columns([0.01, 0.98, 0.01])

with content_col:
    if not data.empty:
        completed_by_you = data[data["State"] == "Done"].reset_index(drop=True)
        if not completed_by_you.empty:
            st.subheader("✅ Completed Tasks")
            st.dataframe(completed_by_you, use_container_width=True)
        else:
            st.info("No completed tasks assigned to you.")

        to_be_closed_by_you = data[data["State"] != "Done"].reset_index(drop=True)
        if not to_be_closed_by_you.empty:
            st.subheader("🚧 Tasks To Close")
            st.dataframe(to_be_closed_by_you, use_container_width=True)
        else:
            st.info("No tasks to close assigned to you.")
    else:
        st.error("No data available.")
