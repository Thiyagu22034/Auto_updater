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
    "api_base_url": "http://in-timetracking.euroland.com/api",
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
                # Ensure all default keys exist
                for k, v in DEFAULT_SETTINGS.items():
                    if k not in data:
                        data[k] = v
                return data
        except Exception:
            pass
    return DEFAULT_SETTINGS.copy()


def save_settings():
    with open(CONFIG_FILE, "w") as f:
        json.dump(st.session_state.settings, f, indent=4)


# Initialize session state for settings
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


# ---------------- MULTI-STRATEGY HTTP CLIENT ----------------
def kimai_request(method: str, endpoint: str, settings: dict, json_payload=None):
    base_url = settings.get("api_base_url", "").rstrip("/")
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
            "X-AUTH-USER": user.lower(),
            "X-AUTH-TOKEN": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        {
            "X-AUTH-USER": user,
            "X-AUTH-TOKEN": token,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    ]

    last_resp = None
    for headers in header_candidates:
        try:
            resp = requests.request(
                method, url, headers=headers, json=json_payload, timeout=6
            )
            if resp.status_code in (200, 201):
                return resp, None
            last_resp = resp
        except Exception as err:
            return None, str(err)

    if last_resp is not None:
        return None, f"HTTP {last_resp.status_code}: {last_resp.text}"
    return None, "Connection failed to Kimai"


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
            self.log("Background scheduler started.")

    def stop(self):
        if self.is_running:
            self.is_running = False
            self.log("Background scheduler stopped.")

    def _run_loop(self):
        while self.is_running:
            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d")
            today_date = date.today()

            is_work, reason = is_working_day(today_date)
            if is_work and self.last_run_date != today_str:
                if now.hour == 18 and now.minute >= 30:
                    self.log(
                        f"18:30 reached. Executing submission for {today_str}..."
                    )
                    try:
                        submit_timesheets(today_date, self.log)
                        self.last_run_date = today_str
                        self.log("Submission successful.")
                    except Exception as e:
                        self.log(f"Execution failed: {str(e)}")
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
    query2 = """
        SELECT
            m.newspaper_id,
            'in_queue' AS status,
            COUNT(*) AS total
        FROM articles.article_main_metadata m
        LEFT JOIN articles.article_body_master b
            ON b.fid = m.fid
           AND b.newspaperid = m.newspaper_id
        LEFT JOIN monitoring.failed_articles f
            ON f.article_id = m.fid
           AND f.newspaper_id = m.newspaper_id
        WHERE m.newspaper_id IN (188, 10)
          AND b.fid IS NULL        
          AND f.article_id IS NULL
        GROUP BY m.newspaper_id
        ORDER BY m.newspaper_id;
    """
    lines = []
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query1)
                for row in cur.fetchall():
                    lines.append(f"{row[0]}-{row[1]}-{row[2]}")
                cur.execute(query2)
                for row in cur.fetchall():
                    lines.append(f"Queue: Newspaper {row[0]} - {row[2]} pending")
    except Exception as e:
        logger_func(f"Database error on {date_str}: {e}")
        return "Routine database verification and scraping pipeline monitoring."

    return (
        "\n".join(lines)
        if lines
        else f"No new articles processed for {date_str}."
    )


# ---------------- ROTATION & SUBMISSION LOGIC ----------------
def get_daily_project(
    target_date: date, projects: list, all_kimai_projects: list
) -> tuple[int, str]:
    if not projects:
        raise ValueError("No projects configured in rotation.")

    idx = target_date.toordinal() % len(projects)
    target_name = projects[idx]["name"]

    for p in all_kimai_projects:
        if p["name"].strip().lower() == target_name.strip().lower():
            return p["id"], p["name"]

    return projects[idx].get("id"), target_name


def get_daily_activity(
    target_date: date, activities: list, all_kimai_acts: list
) -> tuple[int, str, str]:
    if not activities:
        raise ValueError("No activities configured in rotation.")

    idx = target_date.toordinal() % len(activities)
    target_name = activities[idx]["name"]
    prefix = activities[idx].get("prefix", "")

    for a in all_kimai_acts:
        if a["name"].strip().lower() == target_name.strip().lower():
            return a["id"], a["name"], prefix

    return activities[idx].get("id"), target_name, prefix


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
    all_k_projects, _ = fetch_all_kimai_projects(cfg)
    all_k_acts, _ = fetch_all_kimai_activities(cfg)

    project_id, project_name = get_daily_project(
        target_date, cfg.get("projects", []), all_k_projects
    )
    if not project_id:
        raise ValueError(
            f"Project '{project_name}' ID could not be identified in Kimai."
        )

    activity_id, activity_name, prefix_text = get_daily_activity(
        target_date, cfg.get("activities", []), all_k_acts
    )
    if not activity_id:
        raise ValueError(
            f"Activity '{activity_name}' ID could not be identified in Kimai."
        )

    db_stats = get_database_stats(target_date, logger_func)
    description = (
        f"{prefix_text}\n{db_stats}".strip() if prefix_text else db_stats
    )
    slots = get_slots_for_date(target_date, cfg.get("shift_schedules", []))

    logger_func(
        f"Processing {target_date.isoformat()} | Project: {project_name} (ID: {project_id}) | Activity: {activity_name} (ID: {activity_id})"
    )

    for i, slot in enumerate(slots, 1):
        payload = {
            "begin": slot["begin"],
            "end": slot["end"],
            "project": project_id,
            "activity": activity_id,
            "description": description,
        }
        resp, err = kimai_request(
            "POST", "/timesheet", cfg, json_payload=payload
        )
        if resp:
            logger_func(f"Slot {i} logged: {slot['begin']} to {slot['end']}")
        else:
            logger_func(f"Failed slot {i}: {err}")
    return True


# ---------------- STREAMLIT FRONTEND ----------------
st.set_page_config(
    page_title="Kimai Timesheet Platform", layout="wide", page_icon="⏱️"
)
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

    with col_status:
        st.subheader("Live Background Service")
        if scheduler.is_running:
            st.success("🟢 **LIVE SERVICE ACTIVE (Running in Background)**")
        else:
            st.warning("⚪ **SERVICE STOPPED**")

        btn_c1, btn_c2 = st.columns(2)
        with btn_c1:
            if st.button(
                "Start Live Service",
                type="primary",
                disabled=scheduler.is_running,
                use_container_width=True,
            ):
                scheduler.start()
                st.rerun()
        with btn_c2:
            if st.button(
                "End Service",
                type="secondary",
                disabled=not scheduler.is_running,
                use_container_width=True,
            ):
                scheduler.stop()
                st.rerun()

    with col_date:
        st.subheader("Date & Execution Target")
        start_selected_date = st.date_input("Selected Date", value=date.today())
        is_work, day_status = is_working_day(start_selected_date)

        all_k_p, _ = fetch_all_kimai_projects(st.session_state.settings)
        all_k_a, _ = fetch_all_kimai_activities(st.session_state.settings)

        proj_display = "None"
        act_display = "None"
        try:
            _, proj_display = get_daily_project(
                start_selected_date,
                st.session_state.settings.get("projects", []),
                all_k_p,
            )
        except Exception:
            pass

        try:
            _, act_display, _ = get_daily_activity(
                start_selected_date,
                st.session_state.settings.get("activities", []),
                all_k_a,
            )
        except Exception:
            pass

        schedules = st.session_state.settings.get("shift_schedules", [])
        sched_name = (
            schedules[start_selected_date.toordinal() % len(schedules)].get(
                "name", "Default"
            )
            if schedules
            else "None"
        )

        if is_work:
            st.info(
                f"Status: **{day_status}**\n\n"
                f"**Project:** {proj_display} | **Activity:** {act_display} | **Shift:** {sched_name}"
            )
        else:
            st.warning(f"Target date is a non-working day: **{day_status}**.")

        act_c1, act_c2 = st.columns(2)
        with act_c1:
            if st.button("Submit Selected Date", use_container_width=True):
                with st.spinner("Submitting entry..."):
                    try:
                        ok = submit_timesheets(
                            start_selected_date, scheduler.log
                        )
                        if ok:
                            st.success(f"Processed {start_selected_date}!")
                        else:
                            st.warning(f"Skipped {start_selected_date}.")
                    except Exception as e:
                        st.error(f"Error: {e}")
                st.rerun()

        with act_c2:
            if st.button(
                f"Fill Range: {start_selected_date} -> Today",
                use_container_width=True,
            ):
                if start_selected_date > date.today():
                    st.error("Cannot backfill future dates.")
                else:
                    with st.spinner("Processing date range..."):
                        curr = start_selected_date
                        while curr <= date.today():
                            submit_timesheets(curr, scheduler.log)
                            curr += timedelta(days=1)
                        st.success("Range backfill completed!")
                    st.rerun()

    st.divider()

    st.subheader("Activity Logs")
    if scheduler.logs:
        st.text_area(
            label="Real-time Logs",
            value="\n".join(reversed(scheduler.logs)),
            height=240,
        )
    else:
        st.caption("No log entries recorded yet.")

    if st.button("Refresh Logs"):
        st.rerun()


# ---------------- TAB 2: PROJECTS (ROTATION) ----------------
with tab_projects:
    st.subheader("Project Rotation Management")
    st.caption("Active projects will cycle sequentially each working day.")

    projects_list = st.session_state.settings.get("projects", [])

    # Display active projects with Remove action
    for p_idx, p_item in enumerate(projects_list):
        with st.container(border=True):
            col_p_title, col_p_del = st.columns([5, 1])
            with col_p_title:
                st.markdown(
                    f"**Rotation Position #{p_idx + 1}: {p_item['name']}**"
                )
            with col_p_del:
                if st.button(
                    "🗑️ Remove",
                    key=f"del_proj_{p_idx}",
                    use_container_width=True,
                ):
                    projects_list.pop(p_idx)
                    st.session_state.settings["projects"] = projects_list
                    save_settings()
                    st.rerun()

    st.divider()

    # Add Project Section
    st.markdown("### Add Project to Rotation")
    kimai_project_list, p_err = fetch_all_kimai_projects(
        st.session_state.settings
    )
    existing_p_names = [p["name"].strip().lower() for p in projects_list]

    available_p_map = {}
    if kimai_project_list:
        for p in kimai_project_list:
            if p["name"].strip().lower() not in existing_p_names:
                available_p_map[p["name"]] = p.get("id")
        st.success(
            f"Loaded {len(kimai_project_list)} projects live from Kimai API."
        )
    else:
        for name in KIMAI_MASTER_PROJECTS:
            if name.lower() not in existing_p_names:
                available_p_map[name] = None
        if p_err:
            st.info(f"Using catalog (Kimai sync note: {p_err})")

    if available_p_map:
        col_sel_p, col_btn_p = st.columns([3, 1])
        with col_sel_p:
            selected_proj_name = st.selectbox(
                "Select Project to add:",
                options=list(available_p_map.keys()),
                key="select_proj_dropdown",
            )
        with col_btn_p:
            st.write("")
            if st.button(
                "➕ Add Project", type="primary", use_container_width=True
            ):
                chosen_id = available_p_map[selected_proj_name]
                st.session_state.settings["projects"].append(
                    {"id": chosen_id, "name": selected_proj_name}
                )
                save_settings()
                st.success(f"Added '{selected_proj_name}' to rotation!")
                st.rerun()
    else:
        st.info("All projects are currently added to rotation.")


# ---------------- TAB 3: ACTIVITIES (ROTATION) ----------------
with tab_activities:
    st.subheader("Activities Rotation Management")
    st.caption("Active activities cycle sequentially each working day.")

    activities_list = st.session_state.settings.get("activities", [])

    # Display active activities with live prefix editing and Remove action
    for a_idx, act in enumerate(activities_list):
        with st.container(border=True):
            col_act_title, col_act_del = st.columns([5, 1])
            with col_act_title:
                st.markdown(
                    f"**Order #{a_idx + 1}: {act.get('name', 'Activity')}**"
                )
            with col_act_del:
                if st.button(
                    "🗑️ Remove",
                    key=f"del_act_{a_idx}",
                    use_container_width=True,
                ):
                    activities_list.pop(a_idx)
                    st.session_state.settings["activities"] = activities_list
                    save_settings()
                    st.rerun()

            c_act_name, c_act_prefix = st.columns([1, 2])
            with c_act_name:
                st.text_input(
                    "Activity Name",
                    value=act.get("name", ""),
                    disabled=True,
                    key=f"act_name_view_{a_idx}",
                )
            with c_act_prefix:
                act["prefix"] = st.text_input(
                    "Description Prefix Line (Optional)",
                    value=act.get("prefix", ""),
                    placeholder="e.g. I was tested the code",
                    key=f"act_prefix_{a_idx}",
                )

    if st.button("Save Description Prefix Changes"):
        save_settings()
        st.success("Prefix updates saved successfully!")

    st.divider()

    # Add Activity Section
    st.markdown("### Add Activity to Rotation")
    kimai_act_list, a_err = fetch_all_kimai_activities(
        st.session_state.settings
    )
    existing_a_names = [a["name"].strip().lower() for a in activities_list]

    available_a_map = {}
    if kimai_act_list:
        for a in kimai_act_list:
            if a["name"].strip().lower() not in existing_a_names:
                available_a_map[a["name"]] = a.get("id")
        st.success(
            f"Loaded {len(kimai_act_list)} activities live from Kimai API."
        )
    else:
        for name in KIMAI_MASTER_ACTIVITIES:
            if name.lower() not in existing_a_names:
                available_a_map[name] = None
        if a_err:
            st.info(f"Using catalog (Kimai sync note: {a_err})")

    if available_a_map:
        col_act_sel, col_act_pref = st.columns([1, 2])
        with col_act_sel:
            chosen_act_name = st.selectbox(
                "Select Activity to Add:",
                options=list(available_a_map.keys()),
                key="select_act_dropdown",
            )
        with col_act_pref:
            new_prefix_text = st.text_input(
                "Prefix Line for Description (Optional)",
                placeholder="e.g. I was reviewing pull requests",
                key="new_act_prefix_input",
            )

        if st.button(
            "➕ Add Activity to Rotation",
            type="primary",
            use_container_width=True,
        ):
            chosen_act_id = available_a_map[chosen_act_name]
            st.session_state.settings["activities"].append(
                {
                    "id": chosen_act_id,
                    "name": chosen_act_name,
                    "prefix": new_prefix_text.strip(),
                }
            )
            save_settings()
            st.success(f"Added '{chosen_act_name}' to rotation!")
            st.rerun()
    else:
        st.info("All activities are currently added to the rotation.")


# ---------------- TAB 4: SHIFT SCHEDULES (ROTATION) ----------------
with tab_shifts:
    st.subheader("Shift Sets Rotation Cycle")
    st.caption(
        "Working days will cycle sequentially through these shift sets (Set 1 → Set 2 → Set 3 → Set 1)."
    )

    schedules = st.session_state.settings.get("shift_schedules", [])

    for idx, sched in enumerate(schedules):
        with st.container(border=True):
            col_h, col_del = st.columns([5, 1])
            with col_h:
                st.markdown(
                    f"**Shift {idx + 1}: {sched.get('name', f'Shift {idx + 1}')}**"
                )
            with col_del:
                if st.button(
                    "🗑️ Delete",
                    key=f"del_sched_{idx}",
                    use_container_width=True,
                ):
                    schedules.pop(idx)
                    st.session_state.settings["shift_schedules"] = schedules
                    save_settings()
                    st.rerun()

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                sched["slot1_start"] = st.text_input(
                    "Slot 1 Start",
                    value=sched.get("slot1_start", "09:00"),
                    key=f"s1_start_{idx}",
                )
            with c2:
                sched["slot1_end"] = st.text_input(
                    "Slot 1 End",
                    value=sched.get("slot1_end", "13:00"),
                    key=f"s1_end_{idx}",
                )
            with c3:
                sched["slot2_start"] = st.text_input(
                    "Slot 2 Start",
                    value=sched.get("slot2_start", "14:00"),
                    key=f"s2_start_{idx}",
                )
            with c4:
                sched["slot2_end"] = st.text_input(
                    "Slot 2 End",
                    value=sched.get("slot2_end", "18:00"),
                    key=f"s2_end_{idx}",
                )

    if st.button("Save Shift Changes"):
        save_settings()
        st.success("Shift changes saved!")

    st.divider()

    st.markdown("**Add a New Shift Set**")
    with st.form("new_schedule_form", clear_on_submit=True):
        new_name = st.text_input(
            "Schedule Name", placeholder="Shift 3 (11 AM - 8 PM)"
        )
        c_add1, c_add2, c_add3, c_add4 = st.columns(4)
        with c_add1:
            n_s1_start = st.text_input("Slot 1 Start", value="11:00")
        with c_add2:
            n_s1_end = st.text_input("Slot 1 End", value="15:00")
        with c_add3:
            n_s2_start = st.text_input("Slot 2 Start", value="16:00")
        with c_add4:
            n_s2_end = st.text_input("Slot 2 End", value="20:00")

        if st.form_submit_button("➕ Add Shift Set"):
            schedules.append(
                {
                    "name": new_name.strip()
                    if new_name.strip()
                    else f"Shift {len(schedules) + 1}",
                    "slot1_start": n_s1_start.strip(),
                    "slot1_end": n_s1_end.strip(),
                    "slot2_start": n_s2_start.strip(),
                    "slot2_end": n_s2_end.strip(),
                }
            )
            st.session_state.settings["shift_schedules"] = schedules
            save_settings()
            st.rerun()


# ---------------- TAB 5: API SETTINGS ----------------
with tab_api:
    st.subheader("Kimai API Configuration")
    col_api1, col_api2 = st.columns(2)
    with col_api1:
        api_user_input = st.text_input(
            "Kimai API User",
            value=st.session_state.settings.get(
                "api_user", "Thiyagu.Sekar@euroland.com"
            ),
        )
        api_url_input = st.text_input(
            "Kimai Base URL",
            value=st.session_state.settings.get("api_base_url", ""),
        )
    with col_api2:
        api_token_input = st.text_input(
            "Kimai API Key / Token",
            value=st.session_state.settings.get("api_token", ""),
            type="password",
        )

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("Test Connection & Probe Auth", use_container_width=True):
            temp_settings = {
                "api_base_url": api_url_input.strip(),
                "api_user": api_user_input.strip(),
                "api_token": api_token_input.strip(),
            }
            p_list, p_err = fetch_all_kimai_projects(temp_settings)
            a_list, a_err = fetch_all_kimai_activities(temp_settings)

            if p_list or a_list:
                st.success(
                    f"Authentication Verified! Found {len(p_list)} projects and {len(a_list)} activities live from Kimai."
                )
            else:
                st.error(f"Auth Failed: {p_err or a_err}")

    with col_btn2:
        if st.button("Save API Credentials", use_container_width=True):
            st.session_state.settings["api_user"] = api_user_input.strip()
            st.session_state.settings["api_token"] = api_token_input.strip()
            st.session_state.settings["api_base_url"] = api_url_input.strip()
            save_settings()
            st.success("API credentials saved!")