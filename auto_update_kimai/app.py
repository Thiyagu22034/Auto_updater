import json
import os
import threading
import time
from datetime import date, datetime, timedelta
import psycopg2
import requests
import streamlit as st

# ---------------- PERSISTENT CONFIG FILE ----------------
CONFIG_FILE = "kimai_settings.json"

KIMAI_MASTER_PROJECTS = [
    "Spokes Person",
    "Orion",
    "Quarterbytes",
    "Ophex",
    "Ticker - revamp",
    "Wise",
]

KIMAI_MASTER_ACTIVITIES = [
    "Testing",
    "Scraping",
    "Deployment",
    "Development",
    "Code Debug",
    "Code Review",
    "Design",
    "Requirement analysis",
    "Screening & Shortlisting",
    "Sourcing Profiles",
    "Support",
    "Client meeting",
]

DEFAULT_SETTINGS = {
    "api_base_url": "https://in-timetracking.euroland.com/api",
    "api_user": "Thiyagu.Sekar@euroland.com",
    "api_token": "5d8a6dd33e5362b465a2dcc12",
    "projects": [{"id": None, "name": "Spokes Person"}],
    "activities": [
        {"id": None, "name": "Testing", "prefix": "I was tested the code"},
        {"id": None, "name": "Scraping", "prefix": ""},
        {
            "id": None,
            "name": "Deployment",
            "prefix": "I was change the code and deployment",
        },
        {
            "id": None,
            "name": "Development",
            "prefix": "I was code create and changes the code",
        },
        {"id": None, "name": "Code Debug", "prefix": ""},
    ],
    "shift_schedules": [
        {
            "name": "Shift 1 (9 AM - 6 PM)",
            "slot1_start": "09:00",
            "slot1_end": "13:00",
            "slot2_start": "14:00",
            "slot2_end": "18:00",
        },
        {
            "name": "Shift 2 (10 AM - 7 PM)",
            "slot1_start": "10:00",
            "slot1_end": "14:00",
            "slot2_start": "15:00",
            "slot2_end": "19:00",
        },
    ],
}


def load_settings():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                data = json.load(f)
                for k, v in DEFAULT_SETTINGS.items():
                    if k not in data:
                        data[k] = v
                if data.get("api_base_url", "").startswith("http://"):
                    data["api_base_url"] = "https://" + data["api_base_url"][7:]
                return data
        except Exception:
            pass
    return DEFAULT_SETTINGS.copy()


def save_settings():
    with open(CONFIG_FILE, "w") as f:
        json.dump(st.session_state.settings, f, indent=4)


if "settings" not in st.session_state:
    st.session_state.settings = load_settings()

DB_CONFIG = {
    "host": "192.168.100.81",
    "dbname": "spokesperson",
    "user": "qa_user",
    "password": "StrongPassword",
    "port": 5432,
}

COMPANY_HOLIDAYS_2026 = {
    "2026-01-01": "New Year's Day",
    "2026-01-14": "Pongal Holidays",
    "2026-01-15": "Pongal Holidays",
    "2026-01-16": "Pongal Holidays",
    "2026-01-26": "Republic Day",
    "2026-04-03": "Good Friday",
    "2026-04-14": "Tamil New Year's Day",
    "2026-05-01": "May Day",
    "2026-09-14": "Vinayagar Chathurthi",
    "2026-10-02": "Gandhi Jayanthi",
    "2026-10-19": "Ayudha Pooja",
    "2026-11-09": "Deepavali",
    "2026-12-25": "Christmas",
}


def is_working_day(check_date: date) -> tuple[bool, str]:
    if check_date.weekday() == 5:
        return False, "Saturday"
    if check_date.weekday() == 6:
        return False, "Sunday"
    date_str = check_date.isoformat()
    if date_str in COMPANY_HOLIDAYS_2026:
        return False, f"Holiday: {COMPANY_HOLIDAYS_2026[date_str]}"
    return True, "Working Day"


# ---------------- HTTP CLIENT (HTTPS ENFORCED) ----------------
def kimai_request(method: str, endpoint: str, settings: dict, json_payload=None):
    base_url = settings.get("api_base_url", "").strip().rstrip("/")
    if base_url.startswith("http://"):
        base_url = "https://" + base_url[7:]

    url = f"{base_url}{endpoint}"
    user = settings.get("api_user", "").strip()
    token = settings.get("api_token", "").strip()

    header_candidates = [
        {
            "X-AUTH-USER": user,
            "X-AUTH-TOKEN": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    ]

    last_resp = None
    for headers in header_candidates:
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                json=json_payload,
                timeout=12,
                allow_redirects=True,
            )

            content_type = resp.headers.get("Content-Type", "")
            if resp.status_code in (200, 201):
                if "application/json" in content_type:
                    return resp, None
                return None, "Server returned HTML login instead of JSON (Check credentials/auth)."

            last_resp = resp
        except Exception as err:
            return None, str(err)

    if last_resp is not None:
        return None, f"HTTP {last_resp.status_code}: {last_resp.text[:180]}"
    return None, "Connection failed to Kimai API"


def fetch_all_kimai_projects(settings):
    resp, err = kimai_request("GET", "/projects", settings)
    if resp:
        return resp.json(), None
    return [], err


def fetch_all_kimai_activities(settings):
    resp, err = kimai_request("GET", "/activities", settings)
    if resp:
        return resp.json(), None
    return [], err


# ---------------- BACKGROUND SCHEDULER ----------------
class BackgroundScheduler:
    def __init__(self):
        self.is_running = False
        self.thread = None
        self.last_run_date = None
        self.logs = []

    def log(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.logs.append(f"[{timestamp}] {message}")
        if len(self.logs) > 120:
            self.logs.pop(0)

    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self._run_loop, daemon=True)
            self.thread.start()
            self.log("Background scheduler service activated.")

    def stop(self):
        if self.is_running:
            self.is_running = False
            self.log("Background scheduler service stopped.")

    def _run_loop(self):
        while self.is_running:
            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d")
            today_date = date.today()

            is_work, reason = is_working_day(today_date)
            if is_work and self.last_run_date != today_str:
                if (now.hour == 18 and now.minute >= 30) or (now.hour > 18):
                    self.log(f"18:30 reached. Executing live submission for today ({today_str})...")
                    try:
                        submit_timesheets(today_date, self.log)
                        self.last_run_date = today_str
                        self.log("Today's submission completed successfully.")
                    except Exception as e:
                        self.log(f"Execution error: {str(e)}")
            elif not is_work and self.last_run_date != today_str:
                self.last_run_date = today_str
                self.log(f"Skipping {today_str}: {reason}")

            for _ in range(30):
                if not self.is_running:
                    break
                time.sleep(1)


@st.cache_resource
def get_scheduler():
    return BackgroundScheduler()


# ---------------- DATABASE STATS ----------------
def get_database_stats(target_date: date, logger_func) -> str:
    date_str = target_date.isoformat()
    query1 = f"""
        SELECT
            ns.name AS name,
            abm.newspaperid AS newspaper_id,
            COUNT(*) AS article_count
        FROM articles.article_body_master abm, conf.newspapers ns
        WHERE abm.adddate::date = '{date_str}'::date
          AND abm.newspaperid IN (188, 10)
          AND ns.id = abm.newspaperid
        GROUP BY ns.name, abm.newspaperid;
    """
    lines = []
    try:
        with psycopg2.connect(**DB_CONFIG, connect_timeout=4) as conn:
            with conn.cursor() as cur:
                cur.execute(query1)
                for row in cur.fetchall():
                    lines.append(f"{row[0]}-{row[1]}-{row[2]}")
    except Exception as e:
        logger_func(f"Database note on {date_str}: {e}")
        return "Routine database verification and scraping pipeline monitoring."

    return "\n".join(lines) if lines else f"Routine scraping monitoring for {date_str}."


# ---------------- ROTATION & SUBMISSION LOGIC ----------------
def sync_ids_if_needed(cfg):
    updated = False
    all_k_p, _ = fetch_all_kimai_projects(cfg)
    all_k_a, _ = fetch_all_kimai_activities(cfg)

    for p in cfg.get("projects", []):
        if not p.get("id") and all_k_p:
            match = next((item for item in all_k_p if item["name"].strip().lower() == p["name"].strip().lower()), None)
            if match:
                p["id"] = match["id"]
                updated = True

    for a in cfg.get("activities", []):
        if not a.get("id") and all_k_a:
            match = next((item for item in all_k_a if item["name"].strip().lower() == a["name"].strip().lower()), None)
            if match:
                a["id"] = match["id"]
                updated = True

    if updated:
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=4)
    return all_k_p, all_k_a


def get_daily_project(target_date: date, projects: list, all_kimai_projects: list) -> tuple[int, str]:
    if not projects:
        raise ValueError("No projects configured in rotation.")

    idx = target_date.toordinal() % len(projects)
    item = projects[idx]
    target_name = item["name"]

    if item.get("id"):
        return item["id"], target_name

    for p in all_kimai_projects:
        if p["name"].strip().lower() == target_name.strip().lower():
            return p["id"], p["name"]

    return None, target_name


def get_daily_activity(target_date: date, activities: list, all_kimai_acts: list) -> tuple[int, str, str]:
    if not activities:
        raise ValueError("No activities configured in rotation.")

    idx = target_date.toordinal() % len(activities)
    item = activities[idx]
    target_name = item["name"]
    prefix = item.get("prefix", "")

    if item.get("id"):
        return item["id"], target_name, prefix

    for a in all_kimai_acts:
        if a["name"].strip().lower() == target_name.strip().lower():
            return a["id"], a["name"], prefix

    return None, target_name, prefix


def get_slots_for_date(target_date: date, schedules: list) -> list:
    date_str = target_date.isoformat()
    if not schedules:
        return [
            {"begin": f"{date_str}T09:00:00", "end": f"{date_str}T13:00:00"},
            {"begin": f"{date_str}T14:00:00", "end": f"{date_str}T18:00:00"},
        ]
    schedule_idx = target_date.toordinal() % len(schedules)
    selected_sched = schedules[schedule_idx]
    return [
        {
            "begin": f"{date_str}T{selected_sched['slot1_start']}:00",
            "end": f"{date_str}T{selected_sched['slot1_end']}:00",
        },
        {
            "begin": f"{date_str}T{selected_sched['slot2_start']}:00",
            "end": f"{date_str}T{selected_sched['slot2_end']}:00",
        },
    ]


def submit_timesheets(target_date: date, logger_func):
    is_work, reason = is_working_day(target_date)
    if not is_work:
        logger_func(f"Skipping {target_date.isoformat()} - {reason}")
        return False

    cfg = load_settings()
    all_k_p, all_k_a = sync_ids_if_needed(cfg)

    project_id, project_name = get_daily_project(target_date, cfg.get("projects", []), all_k_p)
    if not project_id:
        raise ValueError(f"Project '{project_name}' ID could not be resolved from Kimai.")

    activity_id, activity_name, prefix_text = get_daily_activity(target_date, cfg.get("activities", []), all_k_a)
    if not activity_id:
        raise ValueError(f"Activity '{activity_name}' ID could not be resolved from Kimai.")

    db_stats = get_database_stats(target_date, logger_func)
    description = f"{prefix_text}\n{db_stats}".strip() if prefix_text else db_stats
    slots = get_slots_for_date(target_date, cfg.get("shift_schedules", []))

    logger_func(
        f"Processing {target_date.isoformat()} | Project: {project_name} (ID: {project_id}) | Activity: {activity_name} (ID: {activity_id})"
    )

    for i, slot in enumerate(slots, 1):
        payload = {
            "begin": slot["begin"],
            "end": slot["end"],
            "project": int(project_id),
            "activity": int(activity_id),
            "description": description,
        }

        resp, err = kimai_request("POST", "/timesheets", cfg, json_payload=payload)
        if not resp:
            resp, err = kimai_request("POST", "/timesheet", cfg, json_payload=payload)

        if resp:
            try:
                res_data = resp.json()
                if isinstance(res_data, dict) and "id" in res_data:
                    logger_func(f"✅ Slot {i} recorded (ID #{res_data['id']}): {slot['begin']} -> {slot['end']}")
                else:
                    logger_func(f"Slot {i} recorded: {slot['begin']} -> {slot['end']}")
            except Exception:
                logger_func(f"Slot {i} recorded: {slot['begin']} -> {slot['end']}")
        else:
            logger_func(f"❌ Slot {i} failed: {err}")
    return True


# ---------------- STREAMLIT FRONTEND ----------------
st.set_page_config(page_title="Kimai Timesheet Platform", layout="wide", page_icon="⏱️")
scheduler = get_scheduler()

st.title("Kimai Timesheet Automation Platform")

tab_ops, tab_projects, tab_activities, tab_shifts, tab_api = st.tabs(
    [
        "Operations & Live Service",
        "Projects (Rotation)",
        "Activities (Rotation)",
        "Shift Schedules (Rotation)",
        "API Settings",
    ]
)

# ---------------- TAB 1: OPERATIONS ----------------
with tab_ops:
    col_status, col_date = st.columns([1, 1])
    today = date.today()

    with col_date:
        st.subheader("Automation Settings")
        start_selected_date = st.date_input("Start Date (First Unfilled Day)", value=date(2026, 9, 9))

        if start_selected_date < today:
            st.info(
                f"ℹ️ **Historical sync detected:** Unfilled dates from **{start_selected_date}** to **{today - timedelta(days=1)}** will be backfilled immediately before activating live scheduling."
            )
        else:
            st.info("ℹ️ **Live mode only:** Tracking will run automatically at **18:30 (6:30 PM)** for working days.")

    with col_status:
        st.subheader("Service Control")
        if scheduler.is_running:
            st.success("🟢 **LIVE SERVICE ACTIVE**")
        else:
            st.warning("⚪ **SERVICE STOPPED**")

        btn_c1, btn_c2 = st.columns(2)
        with btn_c1:
            if st.button("▶️ Start Service", type="primary", disabled=scheduler.is_running, use_container_width=True):
                current_projects = st.session_state.settings.get("projects", [])
                current_activities = st.session_state.settings.get("activities", [])
                current_shifts = st.session_state.settings.get("shift_schedules", [])

                if not current_projects:
                    st.error("⚠️ Project is not chosen! Please configure Projects tab.")
                elif not current_activities:
                    st.error("⚠️ Activity is not chosen! Please configure Activities tab.")
                elif not current_shifts:
                    st.error("⚠️ Shift schedule is not chosen! Please configure Shifts tab.")
                elif start_selected_date > today:
                    st.error("Selected start date cannot be in the future.")
                else:
                    # Sync historical records first if start date is prior to today
                    if start_selected_date < today:
                        with st.spinner(f"Syncing history ({start_selected_date} → {today - timedelta(days=1)})..."):
                            try:
                                curr = start_selected_date
                                while curr < today:
                                    submit_timesheets(curr, scheduler.log)
                                    curr += timedelta(days=1)
                                scheduler.log("Historical backfill completed successfully.")
                            except Exception as e:
                                scheduler.log(f"Backfill error: {e}")
                                st.error(f"Error during backfill: {e}")

                    # Activate background daemon loop for today and upcoming runs
                    scheduler.start()
                    st.rerun()

        with btn_c2:
            if st.button("⏹️ Stop Service", disabled=not scheduler.is_running, use_container_width=True):
                scheduler.stop()
                st.rerun()

    st.divider()
    st.subheader("Activity Logs")
    if scheduler.logs:
        st.text_area(
            label="Real-time Logs",
            value="\n".join(reversed(scheduler.logs)),
            height=260,
        )
    else:
        st.caption("No log entries recorded yet.")

    if st.button("Refresh Logs"):
        st.rerun()

# ---------------- TAB 2: PROJECTS ----------------
with tab_projects:
    st.subheader("Project Rotation Management")
    projects_list = st.session_state.settings.get("projects", [])

    for p_idx, p_item in enumerate(projects_list):
        with st.container(border=True):
            col_p_title, col_p_del = st.columns([5, 1])
            with col_p_title:
                st.markdown(f"**Rotation Position #{p_idx + 1}: {p_item['name']}** (ID: `{p_item.get('id', 'Pending Sync')}`)")
            with col_p_del:
                if st.button("🗑️ Remove", key=f"del_proj_{p_idx}", use_container_width=True):
                    projects_list.pop(p_idx)
                    st.session_state.settings["projects"] = projects_list
                    save_settings()
                    st.rerun()

    st.divider()
    kimai_project_list, p_err = fetch_all_kimai_projects(st.session_state.settings)
    existing_p_names = [p["name"].strip().lower() for p in projects_list]
    available_p_map = {}

    if kimai_project_list:
        for p in kimai_project_list:
            if p["name"].strip().lower() not in existing_p_names:
                available_p_map[p["name"]] = p.get("id")
    else:
        for name in KIMAI_MASTER_PROJECTS:
            if name.lower() not in existing_p_names:
                available_p_map[name] = None

    if available_p_map:
        col_sel_p, col_btn_p = st.columns([3, 1])
        with col_sel_p:
            selected_proj_name = st.selectbox("Select Project to add:", options=list(available_p_map.keys()), key="select_proj_dropdown")
        with col_btn_p:
            st.write("")
            if st.button("➕ Add Project", type="primary", use_container_width=True):
                chosen_id = available_p_map[selected_proj_name]
                st.session_state.settings["projects"].append({"id": chosen_id, "name": selected_proj_name})
                save_settings()
                st.rerun()

# ---------------- TAB 3: ACTIVITIES ----------------
with tab_activities:
    st.subheader("Activities Rotation Management")
    activities_list = st.session_state.settings.get("activities", [])

    for a_idx, act in enumerate(activities_list):
        with st.container(border=True):
            col_act_title, col_act_del = st.columns([5, 1])
            with col_act_title:
                st.markdown(f"**Order #{a_idx + 1}: {act.get('name', 'Activity')}** (ID: `{act.get('id', 'Pending Sync')}`)")
            with col_act_del:
                if st.button("🗑️ Remove", key=f"del_act_{a_idx}", use_container_width=True):
                    activities_list.pop(a_idx)
                    st.session_state.settings["activities"] = activities_list
                    save_settings()
                    st.rerun()

            c_act_name, c_act_prefix = st.columns([1, 2])
            with c_act_name:
                st.text_input("Activity Name", value=act.get("name", ""), disabled=True, key=f"act_name_view_{a_idx}")
            with c_act_prefix:
                act["prefix"] = st.text_input("Description Prefix", value=act.get("prefix", ""), key=f"act_prefix_{a_idx}")

    if st.button("Save Prefix Changes"):
        save_settings()
        st.success("Prefix updates saved!")

# ---------------- TAB 4: SHIFT SCHEDULES ----------------
with tab_shifts:
    st.subheader("Shift Sets Rotation Cycle")
    schedules = st.session_state.settings.get("shift_schedules", [])

    for idx, sched in enumerate(schedules):
        with st.container(border=True):
            st.markdown(f"**Shift {idx + 1}: {sched.get('name', f'Shift {idx + 1}')}**")
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                sched["slot1_start"] = st.text_input("Slot 1 Start", value=sched.get("slot1_start", "09:00"), key=f"s1_start_{idx}")
            with c2:
                sched["slot1_end"] = st.text_input("Slot 1 End", value=sched.get("slot1_end", "13:00"), key=f"s1_end_{idx}")
            with c3:
                sched["slot2_start"] = st.text_input("Slot 2 Start", value=sched.get("slot2_start", "14:00"), key=f"s2_start_{idx}")
            with c4:
                sched["slot2_end"] = st.text_input("Slot 2 End", value=sched.get("slot2_end", "18:00"), key=f"s2_end_{idx}")

    if st.button("Save Shifts"):
        save_settings()
        st.success("Shift changes saved!")

# ---------------- TAB 5: API SETTINGS ----------------
with tab_api:
    st.subheader("Kimai API Configuration")
    col_api1, col_api2 = st.columns(2)
    with col_api1:
        api_user_input = st.text_input(
            "Kimai API User",
            value=st.session_state.settings.get("api_user", "Thiyagu.Sekar@euroland.com"),
        )
        api_url_input = st.text_input(
            "Kimai Base URL",
            value=st.session_state.settings.get("api_base_url", "https://in-timetracking.euroland.com/api"),
        )
    with col_api2:
        api_token_input = st.text_input(
            "Kimai API Key / Token",
            value=st.session_state.settings.get("api_token", ""),
            type="password",
        )

    if st.button("Test Connection & Fetch IDs", use_container_width=True):
        clean_url = api_url_input.strip()
        if clean_url.startswith("http://"):
            clean_url = "https://" + clean_url[7:]

        temp_settings = {
            "api_base_url": clean_url,
            "api_user": api_user_input.strip(),
            "api_token": api_token_input.strip(),
        }
        p_list, p_err = fetch_all_kimai_projects(temp_settings)
        a_list, a_err = fetch_all_kimai_activities(temp_settings)
        if p_list or a_list:
            st.session_state.settings["api_user"] = api_user_input.strip()
            st.session_state.settings["api_token"] = api_token_input.strip()
            st.session_state.settings["api_base_url"] = clean_url
            sync_ids_if_needed(st.session_state.settings)
            save_settings()
            st.success(f"Connected securely via HTTPS! Synced {len(p_list)} projects and {len(a_list)} activities.")
        else:
            st.error(f"Connection failed: {p_err or a_err}")