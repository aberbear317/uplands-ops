"""Streamlit dashboard for the Uplands Lovedean site management portal."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import html
import json
from pathlib import Path
import re
import subprocess
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import streamlit as st

from uplands_site_command_centre import (
    COSHH_DESTINATION,
    COSHHDocument,
    DATABASE_PATH,
    FILE_1_OUTPUT_DIR,
    FILE_2_CHECKLIST_OUTPUT_DIR,
    FILE_3_COMPLETED_INDUCTIONS_DIR,
    FILE_3_OUTPUT_DIR,
    FILE_3_SIGNATURES_DIR,
    InductionDocument,
    LadderPermit,
    PlantAssetDocument,
    PERMITS_DESTINATION,
    PLANT_HIRE_REGISTER_DIR,
    RAMSDocument,
    RAMS_DESTINATION,
    SafetyAsset,
    SITE_CHECK_WEEKDAY_KEYS,
    SITE_CHECK_WEEKDAY_LABELS,
    CarrierComplianceDocument,
    CarrierComplianceDocumentType,
    ComplianceAlertStatus,
    DocumentStatus,
    DocumentRepository,
    FileGroup,
    SiteAttendanceRecord,
    SiteAttendanceRegister,
    SiteWorker,
    TemplateValidationError,
    TOOLBOX_TALK_REGISTER_DIR,
    ValidationError,
    WeeklySiteCheck,
    WasteRegister,
    WasteTransferNoteDocument,
    build_site_worker_roster,
    check_carrier_compliance,
    create_weekly_site_check_checklist_draft,
    create_site_induction_document,
    create_ladder_permit_draft,
    file_and_index_all,
    generate_site_induction_poster,
    generate_coshh_register_document,
    generate_rams_register_document,
    generate_waste_register_document,
    generate_plant_register_document,
    generate_permit_register_document,
    get_waste_kpi_sheet_metadata,
    get_valid_template_tags,
    get_weekly_site_check_row_definitions,
    run_workspace_diagnostic,
    smart_scan_waste_transfer_note,
    sync_file_4_permit_records,
    update_logged_waste_transfer_note,
)


APP_ROOT = Path(__file__).resolve().parent
UPLANDS_LOGO = APP_ROOT / "Home Uplands.png"
NATIONAL_GRID_LOGO = APP_ROOT / "Ng logo.png"
PROJECT_SETUP_PATH = DATABASE_PATH.parent / "project_setup.json"

UPLANDS_PINK = "#D1228E"
UPLANDS_BLUE = "#5B8DEF"
SUCCESS_GREEN = "#16A34A"
ALERT_RED = "#D92D20"
PAGE_BACKGROUND = "#F6F7F9"
CARD_BORDER = "#EEF0F2"
TEXT_DARK = "#121826"
TEXT_MUTED = "#6B7280"
SIDEBAR_BACKGROUND = "#F7F7F8"

SITE_DATA_KEYWORD = "lovedean"
PROJECT_NAME = "NG Lovedean Substation"
SITE_TITLE = "Lovedean Substation - Site Management Portal"
ABUCS_NAME = "Abucs"
SITE_MANAGER_NAME = "Ceri Edwards"
LADDER_CHECKLIST_QUESTIONS: Dict[int, str] = {
    1: "Safer alternative eliminated",
    2: "Task-specific RAMS prepared and approved",
    3: "Personnel briefed and understand task",
    4: "Competent supervisor appointed",
    5: "Operatives suitably trained",
    6: "Ladder length suitable",
    7: "Ladder conforms to BS Class A",
    8: "Three points of contact maintained",
    9: "Harness worn and secured above head height",
    10: "Ladder stabilised / secured in place",
    11: "Equipment inspected for defects",
}


@dataclass(frozen=True)
class AbucsStatusRow:
    """UI-ready compliance row for the Abucs card."""

    label: str
    status: ComplianceAlertStatus
    reason: str

    @property
    def indicator_colour(self) -> str:
        return SUCCESS_GREEN if self.status == ComplianceAlertStatus.OK else ALERT_RED


@dataclass(frozen=True)
class FileStation:
    """UI metadata for one of the four physical site files."""

    label: str
    number: str
    title: str
    subtitle: str


@dataclass(frozen=True)
class ContractorFolderRow:
    """Roll-up view of one contractor's File 3 document coverage."""

    contractor_name: str
    workers_today: int
    rams_count: int
    coshh_count: int
    induction_count: int


@dataclass(frozen=True)
class ProjectSetup:
    """Portable project metadata reused across the command centre."""

    current_site_name: str
    job_number: str
    site_address: str
    client_name: str


FILE_STATIONS: List[FileStation] = [
    FileStation(
        label="📁 FILE 1: Environment & Waste",
        number="FILE 1",
        title="Environment & Waste",
        subtitle="Tonnage, carrier gatekeeping, and environmental records.",
    ),
    FileStation(
        label="📋 FILE 2: Registers & Diary",
        number="FILE 2",
        title="Registers & Diary",
        subtitle="Attendance, man-hours, and the live site register feed.",
    ),
    FileStation(
        label="🛡️ FILE 3: Contractor Master",
        number="FILE 3",
        title="Contractor Master",
        subtitle="The bouncer for RAMS, COSHH, and induction coverage.",
    ),
    FileStation(
        label="📝 SITE INDUCTION",
        number="INDUCTION",
        title="Site Induction",
        subtitle="Mobile sign-in kiosk with signature capture and live induction logging.",
    ),
    FileStation(
        label="⚡ FILE 4: Permits & Temp Works",
        number="FILE 4",
        title="Permits & Temp Works",
        subtitle="The motor for UHSF21.09 and live permit issue control.",
    ),
]
DEFAULT_FILE_STATION_LABEL = FILE_STATIONS[1].label
STANDARD_SITE_CHECKS: List[tuple[str, str]] = [
    ("Daily", "Site access and egress routes are clear."),
    ("Daily", "Housekeeping standards are acceptable across the workface."),
    ("Daily", "Fire points, extinguishers, and emergency routes are in place."),
    ("Daily", "First aid provision and welfare facilities are ready for shift start."),
    ("Daily", "Barriers, exclusion zones, and pedestrian segregation are in place."),
    ("Weekly", "Plant condition and lifting/inspection records have been reviewed."),
    ("Weekly", "Notice boards, permits, and statutory site signage are current."),
]
WEEKLY_SITE_CHECK_STATUS_OPTIONS: Dict[str, Optional[bool]] = {
    "Blank": None,
    "Tick ✔": True,
    "Cross ✘": False,
}
WEEKLY_SITE_CHECK_STATUS_LABELS: Dict[Optional[bool], str] = {
    None: "",
    True: "✔",
    False: "✘",
}


def _current_week_commencing(reference_date: Optional[date] = None) -> date:
    """Return the Monday date for the active site-check week."""

    resolved_date = reference_date or date.today()
    return resolved_date - timedelta(days=resolved_date.weekday())


def _current_active_day_key(reference_date: Optional[date] = None) -> str:
    """Return the current weekday key used by the File 2 checklist grid."""

    resolved_date = reference_date or date.today()
    return SITE_CHECK_WEEKDAY_KEYS[resolved_date.weekday()]


def _weekly_site_check_status_label(value: Optional[bool]) -> str:
    """Return the UI symbol for one weekly checklist cell."""

    return WEEKLY_SITE_CHECK_STATUS_LABELS[value]


def _cycle_weekly_site_check_value(value: Optional[bool]) -> Optional[bool]:
    """Cycle one checklist cell through blank, tick, and cross."""

    if value is None:
        return True
    if value is True:
        return False
    return None


def _weekly_site_check_template_tag(day_key: str, row_number: int) -> str:
    """Return the placeholder tag name for one File 2 matrix cell."""

    return f"{day_key}_{row_number}"


def _set_weekly_site_check_column_value(
    *,
    namespace: str,
    row_definitions: List[Any],
    day_key: str,
    value: Optional[bool],
    valid_template_tags: set[str],
) -> None:
    """Set an entire File 2 checklist column to one value in session state."""

    for row_definition in row_definitions:
        if (
            _weekly_site_check_template_tag(day_key, row_definition.row_number)
            not in valid_template_tags
        ):
            continue
        state_key = _weekly_site_check_state_key(
            namespace,
            kind="cell",
            row_number=row_definition.row_number,
            day_key=day_key,
        )
        st.session_state[state_key] = value


def _weekly_site_check_namespace(site_name: str, week_commencing: date) -> str:
    """Return a stable session-state namespace for one site/week editor."""

    slug = re.sub(r"[^a-z0-9]+", "-", site_name.casefold()).strip("-")
    return f"{slug}-{week_commencing:%Y%m%d}"


def _weekly_site_check_state_key(
    namespace: str,
    *,
    kind: str,
    row_number: Optional[int] = None,
    day_key: Optional[str] = None,
) -> str:
    """Build a deterministic session-state key for the File 2 grid editor."""

    parts = ["weekly-site-check", namespace, kind]
    if row_number is not None:
        parts.append(str(row_number))
    if day_key is not None:
        parts.append(day_key)
    return "-".join(parts)


def _ensure_weekly_site_check_editor_state(
    *,
    namespace: str,
    weekly_site_check: Optional[WeeklySiteCheck],
    row_definitions: List[Any],
) -> None:
    """Load one week of File 2 matrix state into Streamlit session state once."""

    loaded_key = "weekly-site-check-editor-loaded"
    if st.session_state.get(loaded_key) == namespace:
        return

    row_lookup = {
        row_state.row_number: row_state
        for row_state in (weekly_site_check.row_states if weekly_site_check else [])
    }
    for row_definition in row_definitions:
        row_state = row_lookup.get(row_definition.row_number)
        for day_key in list(SITE_CHECK_WEEKDAY_KEYS) + ["weekly"]:
            state_key = _weekly_site_check_state_key(
                namespace,
                kind="cell",
                row_number=row_definition.row_number,
                day_key=day_key,
            )
            st.session_state[state_key] = (
                row_state.get_value(day_key)
                if row_state is not None
                else None
            )

    for day_key in SITE_CHECK_WEEKDAY_KEYS:
        st.session_state[
            _weekly_site_check_state_key(namespace, kind="initials", day_key=day_key)
        ] = (
            weekly_site_check.daily_initials.get(day_key, "")
            if weekly_site_check is not None
            else ""
        )
        st.session_state[
            _weekly_site_check_state_key(namespace, kind="time", day_key=day_key)
        ] = (
            weekly_site_check.daily_time_markers.get(day_key, "")
            if weekly_site_check is not None
            else ""
        )

    st.session_state[
        _weekly_site_check_state_key(namespace, kind="checked-by")
    ] = (
        weekly_site_check.checked_by
        if weekly_site_check is not None
        else SITE_MANAGER_NAME
    )
    st.session_state[
        _weekly_site_check_state_key(namespace, kind="active-day")
    ] = (
        weekly_site_check.active_day_key
        if weekly_site_check is not None
        else _current_active_day_key()
    )
    st.session_state[loaded_key] = namespace


def _default_project_setup() -> ProjectSetup:
    """Return the fallback project metadata used before setup is saved."""

    return ProjectSetup(
        current_site_name=PROJECT_NAME,
        job_number="",
        site_address="",
        client_name="National Grid",
    )


def _load_project_setup() -> ProjectSetup:
    """Load persisted project metadata from disk."""

    default_setup = _default_project_setup()
    try:
        payload = json.loads(PROJECT_SETUP_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default_setup
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return default_setup

    return ProjectSetup(
        current_site_name=str(payload.get("current_site_name") or default_setup.current_site_name).strip()
        or default_setup.current_site_name,
        job_number=str(payload.get("job_number") or "").strip(),
        site_address=str(payload.get("site_address") or "").strip(),
        client_name=str(payload.get("client_name") or default_setup.client_name).strip()
        or default_setup.client_name,
    )


def _save_project_setup(project_setup: ProjectSetup) -> None:
    """Persist project metadata to a small JSON file."""

    PROJECT_SETUP_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROJECT_SETUP_PATH.write_text(
        json.dumps(
            {
                "current_site_name": project_setup.current_site_name,
                "job_number": project_setup.job_number,
                "site_address": project_setup.site_address,
                "client_name": project_setup.client_name,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _get_project_setup() -> ProjectSetup:
    """Return the cached project metadata for the active session."""

    cached_setup = st.session_state.get("project_setup")
    if isinstance(cached_setup, ProjectSetup):
        return cached_setup

    loaded_setup = _load_project_setup()
    st.session_state["project_setup"] = loaded_setup
    return loaded_setup


def _reset_ladder_permit_form_state() -> None:
    """Reset the File 4 permit helper fields back to their default values."""

    st.session_state["ladder_permit_description_of_work"] = ""
    st.session_state["ladder_permit_location_of_work"] = ""
    st.session_state["ladder_permit_supervisor_name"] = SITE_MANAGER_NAME
    st.session_state["ladder_permit_inspection_checked_by"] = SITE_MANAGER_NAME
    st.session_state["ladder_permit_inspection_rungs_ok"] = True
    st.session_state["ladder_permit_inspection_stiles_ok"] = True
    st.session_state["ladder_permit_inspection_feet_ok"] = True
    st.session_state["ladder_permit_inspection_ok_to_use"] = True
    st.session_state["ladder_permit_inspection_comments"] = "No defects found"
    for question_number in LADDER_CHECKLIST_QUESTIONS:
        st.session_state[f"ladder_permit_q{question_number}"] = True


def _reset_site_induction_form_state() -> None:
    """Reset the induction kiosk fields for the next operative."""

    state_defaults = {
        "site_induction_full_name": "",
        "site_induction_home_address": "",
        "site_induction_contact_number": "",
        "site_induction_company": "",
        "site_induction_occupation": "",
        "site_induction_emergency_contact": "",
        "site_induction_emergency_tel": "",
        "site_induction_medical": "",
        "site_induction_cscs_number": "",
        "site_induction_first_aider": False,
        "site_induction_fire_warden": False,
        "site_induction_supervisor": False,
        "site_induction_smsts": False,
    }
    for state_key, state_value in state_defaults.items():
        st.session_state[state_key] = state_value


def _get_station_label_from_query_params() -> Optional[str]:
    """Return the requested station label from the current URL, if any."""

    raw_station_value = st.query_params.get("station")
    if not raw_station_value:
        return None

    requested_station = str(raw_station_value).strip().casefold()
    for station in FILE_STATIONS:
        if requested_station in {
            station.number.casefold(),
            station.label.casefold(),
            station.title.casefold(),
        }:
            return station.label
    if requested_station == "induction":
        return next(
            (station.label for station in FILE_STATIONS if station.number == "INDUCTION"),
            None,
        )
    return None


def main() -> None:
    """Render the Streamlit portal."""

    st.set_page_config(
        page_title=SITE_TITLE,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_styles()

    repository = _build_repository()
    project_setup = _get_project_setup()
    requested_station_label = _get_station_label_from_query_params()
    if (
        "active_file_station" not in st.session_state
        or st.session_state["active_file_station"]
        not in {station.label for station in FILE_STATIONS}
    ):
        st.session_state["active_file_station"] = DEFAULT_FILE_STATION_LABEL
    if requested_station_label is not None:
        st.session_state["active_file_station"] = requested_station_label
    active_station_label = str(st.session_state["active_file_station"])

    with st.sidebar:
        _render_sidebar(repository, active_station_label, project_setup)

    active_station_label = _render_file_station_navigation()
    _render_active_station(repository, active_station_label, project_setup)


def _build_repository() -> DocumentRepository:
    """Return a repository bound to the configured workspace database."""

    repository = DocumentRepository(DATABASE_PATH)
    repository.create_schema()
    return repository


def _inject_styles() -> None:
    """Apply the Lovedean portal styling."""

    st.markdown(
        f"""
        <style>
            :root {{
                color-scheme: light !important;
            }}
            html,
            body,
            .stApp,
            .main,
            [data-testid="stAppViewContainer"],
            [data-testid="stAppViewContainer"] > .main,
            [data-testid="stAppViewContainer"] > .main *,
            [data-testid="stAppViewContainer"] *,
            section[data-testid="stSidebar"],
            section[data-testid="stSidebar"] * {{
                color-scheme: light !important;
            }}
            section[data-testid="stSidebar"],
            .main,
            .stApp {{
                background-color: #ffffff !important;
                color: #000000 !important;
                -webkit-text-fill-color: #000000 !important;
            }}
            [style*="background: rgb(0, 0, 0)"],
            [style*="background-color: rgb(0, 0, 0)"],
            [style*="background:#000"],
            [style*="background-color:#000"],
            [style*="background: #000000"],
            [style*="background-color: #000000"] {{
                background: #f0f2f6 !important;
                background-color: #f0f2f6 !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                filter: none !important;
            }}
            #MainMenu {{visibility: hidden;}}
            header {{visibility: hidden;}}
            footer {{visibility: hidden;}}
            .stApp {{
                background:
                    radial-gradient(circle at top left, rgba(209, 34, 142, 0.10), transparent 24%),
                    linear-gradient(90deg, #f8f6f9 0%, #f2f4f8 100%);
                color: {TEXT_DARK};
                font-family: "Avenir Next", "Segoe UI", sans-serif;
            }}
            [data-testid="stAppViewContainer"] > .main {{
                padding-left: 0 !important;
            }}
            [data-testid="stAppViewContainer"] > .main .block-container {{
                padding-top: 1.75rem;
                padding-bottom: 2rem;
                padding-left: 2.5rem;
                padding-right: 2.5rem;
                max-width: 1380px;
            }}
            section[data-testid="stSidebar"] {{
                background: {SIDEBAR_BACKGROUND};
                border-right: 1px solid #e7e9ee;
            }}
            section[data-testid="stSidebar"] * {{
                color: {TEXT_DARK};
            }}
            [data-testid="stSidebarCollapseButton"],
            [data-testid="collapsedControl"] {{
                background: #f0f2f6 !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 999px !important;
                box-shadow: 0 6px 14px rgba(18, 24, 38, 0.1) !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                opacity: 1 !important;
                z-index: 10000 !important;
            }}
            [data-testid="stSidebarCollapseButton"] *,
            [data-testid="collapsedControl"] *,
            [data-testid="stSidebarCollapseButton"] svg,
            [data-testid="collapsedControl"] svg,
            [data-testid="stSidebarCollapseButton"] path,
            [data-testid="collapsedControl"] path {{
                color: #31333F !important;
                fill: #31333F !important;
                stroke: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            @media (min-width: 768px) {{
                [data-testid="collapsedControl"] {{
                    display: none !important;
                }}
                section[data-testid="stSidebar"] {{
                    min-width: 22rem !important;
                    max-width: 22rem !important;
                }}
                section[data-testid="stSidebar"][aria-expanded="false"] {{
                    min-width: 22rem !important;
                    max-width: 22rem !important;
                    transform: translateX(0) !important;
                    margin-left: 0 !important;
                }}
            }}
            @media (max-width: 767.98px) {{
                section[data-testid="stSidebar"] {{
                    min-width: auto !important;
                    max-width: none !important;
                }}
                section[data-testid="stSidebar"][aria-expanded="false"] {{
                    min-width: 0 !important;
                    max-width: 0 !important;
                    transform: none !important;
                    margin-left: 0 !important;
                }}
                [data-testid="stAppViewContainer"] > .main .block-container {{
                    padding-left: 1rem !important;
                    padding-right: 1rem !important;
                }}
            }}
            section[data-testid="stSidebar"] .stImage img {{
                background: #ffffff;
                border: 1px solid #eceef2;
                border-radius: 12px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.05);
                padding: 0.7rem;
            }}
            section[data-testid="stSidebar"] div.stButton > button,
            section[data-testid="stSidebar"] div.stFormSubmitButton > button {{
                background: linear-gradient(135deg, {UPLANDS_PINK} 0%, {UPLANDS_BLUE} 100%) !important;
                color: #ffffff !important;
                border: none !important;
                border-radius: 12px !important;
                box-shadow: 0 10px 22px rgba(91, 141, 239, 0.18);
                font-weight: 800 !important;
                justify-content: center !important;
                min-height: 3rem;
                padding: 0.65rem 1rem !important;
            }}
            section[data-testid="stSidebar"] div.stButton > button:hover,
            section[data-testid="stSidebar"] div.stFormSubmitButton > button:hover {{
                background: linear-gradient(135deg, #b91c7b 0%, #4f7ddd 100%) !important;
                color: #ffffff !important;
                transform: translateY(-1px);
            }}
            section[data-testid="stSidebar"] div.stButton > button:focus,
            section[data-testid="stSidebar"] div.stFormSubmitButton > button:focus {{
                color: #ffffff !important;
                box-shadow: 0 0 0 0.18rem rgba(209, 34, 142, 0.2) !important;
            }}
            div[data-testid="stExpander"],
            details[data-testid="stExpander"] {{
                background: #ffffff !important;
                color-scheme: light !important;
                filter: none !important;
            }}
            div[data-testid="stExpander"] summary,
            details[data-testid="stExpander"] summary {{
                background: #f0f2f6 !important;
                background-color: #f0f2f6 !important;
                background-image: none !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 10px !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                box-shadow: none !important;
                filter: none !important;
            }}
            div[data-testid="stExpander"] summary:hover,
            div[data-testid="stExpander"] summary:focus,
            details[data-testid="stExpander"] summary:hover,
            details[data-testid="stExpander"] summary:focus {{
                background-color: #e8edf4 !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            div[data-testid="stExpander"] summary *,
            details[data-testid="stExpander"] summary *,
            div[data-testid="stExpander"] summary p,
            details[data-testid="stExpander"] summary p {{
                background: transparent !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                opacity: 1 !important;
            }}
            div[data-testid="stExpander"] summary svg,
            div[data-testid="stExpander"] summary path,
            details[data-testid="stExpander"] summary svg,
            details[data-testid="stExpander"] summary path {{
                color: #31333F !important;
                fill: #31333F !important;
                stroke: #31333F !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stExpander"],
            [data-testid="stAppViewContainer"] > .main details[data-testid="stExpander"] {{
                background: #ffffff !important;
                background-color: #ffffff !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 12px !important;
                box-shadow: none !important;
                color-scheme: light !important;
                filter: none !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stExpander"] summary,
            [data-testid="stAppViewContainer"] > .main details[data-testid="stExpander"] summary {{
                background: #f0f2f6 !important;
                background-color: #f0f2f6 !important;
                background-image: none !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 10px !important;
                box-shadow: none !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                color-scheme: light !important;
                filter: none !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stExpander"] summary *,
            [data-testid="stAppViewContainer"] > .main details[data-testid="stExpander"] summary *,
            [data-testid="stAppViewContainer"] > .main div[data-testid="stExpander"] summary p,
            [data-testid="stAppViewContainer"] > .main details[data-testid="stExpander"] summary p {{
                background: transparent !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                opacity: 1 !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stExpander"] summary svg,
            [data-testid="stAppViewContainer"] > .main div[data-testid="stExpander"] summary path,
            [data-testid="stAppViewContainer"] > .main details[data-testid="stExpander"] summary svg,
            [data-testid="stAppViewContainer"] > .main details[data-testid="stExpander"] summary path {{
                color: #31333F !important;
                fill: #31333F !important;
                stroke: #31333F !important;
            }}
            [data-testid="stAppViewContainer"] > .main button,
            [data-testid="stAppViewContainer"] > .main button[kind],
            [data-testid="stAppViewContainer"] > .main input,
            [data-testid="stAppViewContainer"] > .main textarea,
            [data-testid="stAppViewContainer"] > .main select {{
                appearance: none !important;
                -webkit-appearance: none !important;
                color-scheme: light !important;
                filter: none !important;
            }}
            [data-testid="stAppViewContainer"] > .main div.stButton > button,
            [data-testid="stAppViewContainer"] > .main div.stFormSubmitButton > button,
            [data-testid="stAppViewContainer"] > .main button[kind="secondary"],
            [data-testid="stAppViewContainer"] > .main button[kind="primary"],
            [data-testid="stAppViewContainer"] > .main button[kind="secondaryFormSubmit"],
            [data-testid="stAppViewContainer"] > .main button[kind="primaryFormSubmit"] {{
                background: #f0f2f6 !important;
                background-image: none !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 12px !important;
                box-shadow: 0 4px 10px rgba(18, 24, 38, 0.06) !important;
                appearance: none !important;
                -webkit-appearance: none !important;
                filter: none !important;
                font-weight: 800 !important;
                min-height: 2.9rem;
                padding: 0.65rem 1rem !important;
            }}
            [data-testid="stAppViewContainer"] > .main button *,
            [data-testid="stAppViewContainer"] > .main div.stButton > button *,
            [data-testid="stAppViewContainer"] > .main div.stFormSubmitButton > button * {{
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                background: transparent !important;
            }}
            [data-testid="stAppViewContainer"] > .main div.stButton > button:hover,
            [data-testid="stAppViewContainer"] > .main div.stFormSubmitButton > button:hover,
            [data-testid="stAppViewContainer"] > .main div.stButton > button:focus,
            [data-testid="stAppViewContainer"] > .main div.stFormSubmitButton > button:focus,
            [data-testid="stAppViewContainer"] > .main button[kind="secondary"]:hover,
            [data-testid="stAppViewContainer"] > .main button[kind="primary"]:hover,
            [data-testid="stAppViewContainer"] > .main button[kind="secondaryFormSubmit"]:hover,
            [data-testid="stAppViewContainer"] > .main button[kind="primaryFormSubmit"]:hover,
            [data-testid="stAppViewContainer"] > .main button[kind="secondary"]:focus,
            [data-testid="stAppViewContainer"] > .main button[kind="primary"]:focus,
            [data-testid="stAppViewContainer"] > .main button[kind="secondaryFormSubmit"]:focus,
            [data-testid="stAppViewContainer"] > .main button[kind="primaryFormSubmit"]:focus {{
                background: #e8edf4 !important;
                background-image: none !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                box-shadow: 0 0 0 0.18rem rgba(209, 34, 142, 0.12) !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stTextInput"] input,
            [data-testid="stAppViewContainer"] > .main div[data-testid="stTextInput"] > div > div input,
            [data-testid="stAppViewContainer"] > .main div[data-baseweb="input"] > div,
            [data-testid="stAppViewContainer"] > .main div[data-baseweb="base-input"] > div,
            [data-testid="stAppViewContainer"] > .main input[type="text"],
            [data-testid="stAppViewContainer"] > .main input[type="search"],
            [data-testid="stAppViewContainer"] > .main div[data-testid="stSelectbox"] > div[data-baseweb="select"] > div,
            [data-testid="stAppViewContainer"] > .main div[data-role="dropdown"],
            [data-testid="stAppViewContainer"] > .main textarea {{
                background-color: #f0f2f6 !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 10px !important;
                box-shadow: none !important;
                color-scheme: light !important;
                background-clip: padding-box !important;
                -webkit-box-shadow: 0 0 0 1000px #f0f2f6 inset !important;
                box-shadow: inset 0 0 0 1000px #f0f2f6 !important;
                filter: none !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stTextInput"] *,
            [data-testid="stAppViewContainer"] > .main div[data-testid="stSelectbox"] *,
            [data-testid="stAppViewContainer"] > .main div[data-baseweb="input"] *,
            [data-testid="stAppViewContainer"] > .main div[data-baseweb="base-input"] *,
            [data-testid="stAppViewContainer"] > .main div[data-role="dropdown"] *,
            [data-testid="stAppViewContainer"] > .main div[data-testid="stToggle"] *,
            [data-testid="stAppViewContainer"] > .main textarea * {{
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stToggle"] label,
            [data-testid="stAppViewContainer"] > .main div[data-testid="stToggle"] p,
            [data-testid="stAppViewContainer"] > .main div[data-testid="stToggle"] span {{
                background: transparent !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] {{
                background: rgba(255, 255, 255, 0.92) !important;
                border: 1px solid #eceef2 !important;
                border-radius: 12px !important;
                box-shadow: none !important;
                color-scheme: light !important;
                margin-top: 0.8rem;
                overflow: hidden;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] > details,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] {{
                background: #ffffff !important;
                color-scheme: light !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary {{
                background-color: #f0f2f6 !important;
                border-bottom: 1px solid #d9dde5 !important;
                color: #31333F !important;
                color-scheme: light !important;
                padding: 0.35rem 0.65rem;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary:hover,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary:focus,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary:hover,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary:focus {{
                background-color: #e8edf4 !important;
                color: #31333F !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary *,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary * {{
                background-color: transparent !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                opacity: 1 !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary p,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary p {{
                color: #31333F !important;
                font-size: 0.95rem;
                font-weight: 800;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary svg,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary path,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary svg,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary path {{
                color: #31333F !important;
                fill: #31333F !important;
                stroke: #31333F !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] .streamlit-expanderContent,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] .streamlit-expanderContent {{
                background: #ffffff !important;
                border: none;
                color-scheme: light !important;
                padding-top: 0.15rem;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] input,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] textarea,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] select,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[role="combobox"],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[role="listbox"],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-baseweb="select"] > div,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stSelectbox"] > div,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-role="dropdown"] {{
                background-color: #ffffff !important;
                color: #000000 !important;
                -webkit-text-fill-color: #000000 !important;
                border: 1px solid #eceef2 !important;
                border-radius: 10px !important;
                box-shadow: none !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stSelectbox"],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stTextInput"],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-baseweb="select"],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[role="listbox"],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-role="dropdown"],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] input *,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] textarea *,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] select *,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-baseweb="select"] *,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[role="combobox"] *,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[role="listbox"] *,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-role="dropdown"] * {{
                background-color: #ffffff !important;
                color: #000000 !important;
                -webkit-text-fill-color: #000000 !important;
                opacity: 1 !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] input::placeholder,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] textarea::placeholder {{
                color: #6B7280 !important;
                -webkit-text-fill-color: #6B7280 !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] svg,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] path {{
                fill: #31333F !important;
                color: #31333F !important;
                stroke: #31333F !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] ul,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] li {{
                background-color: #ffffff !important;
                color: #000000 !important;
                -webkit-text-fill-color: #000000 !important;
            }}
            div[data-baseweb="popover"] *,
            ul[role="listbox"] * {{
                background-color: #ffffff !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            div[data-baseweb="popover"],
            div[data-baseweb="popover"] > div,
            div[data-baseweb="popover"] ul[role="listbox"],
            ul[role="listbox"] {{
                background-color: #ffffff !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 10px !important;
                box-shadow: 0 10px 24px rgba(18, 24, 38, 0.12) !important;
                filter: none !important;
                min-width: 7.5rem !important;
                width: max-content !important;
            }}
            div[data-baseweb="popover"] [role="option"],
            div[data-baseweb="popover"] li,
            ul[role="listbox"] [role="option"],
            ul[role="listbox"] li {{
                align-items: center !important;
                background-color: #ffffff !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                display: flex !important;
                min-height: 2rem !important;
                opacity: 1 !important;
                padding: 0.4rem 0.75rem !important;
                white-space: nowrap !important;
            }}
            div[data-baseweb="popover"] [role="option"]:hover,
            div[data-baseweb="popover"] li:hover,
            ul[role="listbox"] [role="option"]:hover,
            ul[role="listbox"] li:hover {{
                background-color: #f0f2f6 !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            .file-2-section-heading {{
                align-items: center;
                background-color: #f0f2f6 !important;
                background-image: none !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 10px !important;
                color: #31333F !important;
                display: flex;
                font-size: 1.02rem;
                font-weight: 800;
                margin-bottom: 0.65rem;
                padding: 0.75rem 0.9rem;
                -webkit-text-fill-color: #31333F !important;
            }}
            .file-2-section-heading *,
            .file-2-section-heading::before,
            .file-2-section-heading::after {{
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                background: transparent !important;
            }}
            .weekly-grid-section {{
                color: #31333F !important;
                font-size: 0.78rem !important;
                font-weight: 800 !important;
                line-height: 1.35 !important;
                padding: 0.25rem 0 !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            .weekly-grid-prompt {{
                color: #31333F !important;
                font-size: 0.84rem !important;
                line-height: 1.45 !important;
                padding: 0.25rem 0 !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            .weekly-grid-cell {{
                align-items: center !important;
                background: #f8fafc !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 8px !important;
                color: #31333F !important;
                display: flex !important;
                font-size: 1rem !important;
                font-weight: 800 !important;
                height: 2.5rem !important;
                justify-content: center !important;
                -webkit-text-fill-color: #31333F !important;
            }}
            section[data-testid="stSidebar"] .quick-action-panel {{
                background: #ffffff !important;
                border: 1px solid #eceef2 !important;
                border-radius: 12px !important;
                box-shadow: 0 4px 6px rgba(0,0,0,0.05) !important;
                color: #31333F !important;
                margin-top: 0.8rem;
                padding: 0.9rem 0.95rem;
                -webkit-text-fill-color: #31333F !important;
            }}
            section[data-testid="stSidebar"] .quick-action-title {{
                color: #31333F !important;
                font-size: 0.98rem;
                font-weight: 800;
                margin-bottom: 0.4rem;
                -webkit-text-fill-color: #31333F !important;
            }}
            section[data-testid="stSidebar"] .quick-action-copy {{
                color: #31333F !important;
                font-size: 0.92rem;
                line-height: 1.45;
                -webkit-text-fill-color: #31333F !important;
            }}
            .stProgress > div > div > div > div {{
                background-color: {UPLANDS_PINK};
            }}
            div[data-testid="stRadio"] > label {{
                display: none;
            }}
            div[data-testid="stRadio"] div[role="radiogroup"] {{
                gap: 0.75rem;
                margin-bottom: 1.25rem;
            }}
            div[data-testid="stRadio"] div[role="radiogroup"] label {{
                background: #ffffff;
                border: 1px solid #dfe3ea;
                border-radius: 14px;
                box-shadow: 0 8px 16px rgba(18, 24, 38, 0.04);
                padding: 0.9rem 1rem;
            }}
            div[data-testid="stRadio"] div[role="radiogroup"] label[data-checked="true"] {{
                background:
                    linear-gradient(#ffffff, #ffffff) padding-box,
                    linear-gradient(135deg, rgba(209, 34, 142, 0.95), rgba(91, 141, 239, 0.95)) border-box;
                border: 1px solid transparent;
                box-shadow: 0 12px 22px rgba(91, 141, 239, 0.12);
            }}
            div[data-testid="stRadio"] div[role="radiogroup"] label p {{
                color: {TEXT_DARK};
                font-size: 0.95rem;
                font-weight: 800;
                line-height: 1.35;
            }}
            .station-header {{
                background: #ffffff;
                border-radius: 16px;
                box-shadow: 0 10px 24px rgba(18, 24, 38, 0.05);
                background:
                    linear-gradient(#ffffff, #ffffff) padding-box,
                    linear-gradient(135deg, rgba(209, 34, 142, 0.88), rgba(91, 141, 239, 0.88)) border-box;
                border: 1px solid transparent;
                margin-bottom: 1rem;
                padding: 1.2rem 1.35rem;
            }}
            .station-number {{
                color: {TEXT_MUTED};
                font-size: 0.82rem;
                font-weight: 800;
                letter-spacing: 0.14em;
                text-transform: uppercase;
                margin-bottom: 0.45rem;
            }}
            .station-title {{
                color: {TEXT_DARK};
                font-size: 2rem;
                font-weight: 900;
                letter-spacing: -0.04em;
                line-height: 1.05;
                margin-bottom: 0.45rem;
            }}
            .station-subtitle {{
                color: {TEXT_MUTED};
                font-size: 1rem;
                line-height: 1.55;
            }}
            .station-project-name {{
                color: {TEXT_DARK};
                font-size: 1.15rem;
                font-weight: 800;
                margin-bottom: 0.35rem;
            }}
            .station-project-meta {{
                color: {TEXT_MUTED};
                font-size: 0.93rem;
                line-height: 1.5;
                margin-top: 0.55rem;
            }}
            .file-list-panel {{
                background: #ffffff;
                border: 1px solid {CARD_BORDER};
                border-radius: 12px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                padding: 1.15rem 1.2rem;
                margin-top: 1rem;
            }}
            .file-list-heading {{
                color: {TEXT_MUTED};
                font-size: 0.8rem;
                font-weight: 800;
                letter-spacing: 0.14em;
                text-transform: uppercase;
                margin-bottom: 0.55rem;
            }}
            .file-list-title {{
                color: {TEXT_DARK};
                font-size: 1.2rem;
                font-weight: 800;
                margin-bottom: 0.45rem;
            }}
            .file-list-caption {{
                color: {TEXT_MUTED};
                font-size: 0.95rem;
                line-height: 1.5;
                margin-bottom: 0.95rem;
            }}
            .file-list-item {{
                border-top: 1px solid {CARD_BORDER};
                color: {TEXT_DARK};
                font-size: 0.98rem;
                line-height: 1.45;
                padding: 0.8rem 0;
            }}
            .file-list-item:first-of-type {{
                border-top: none;
                padding-top: 0.1rem;
            }}
            .file-list-empty {{
                color: {TEXT_MUTED};
                font-size: 0.95rem;
                line-height: 1.5;
            }}
            .roster-table-wrapper {{
                background: #ffffff;
                border: 1px solid {CARD_BORDER};
                border-radius: 12px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                padding: 1.1rem 1.15rem;
                margin-top: 1rem;
                overflow-x: auto;
            }}
            .roster-table {{
                border-collapse: collapse;
                width: 100%;
            }}
            .roster-table thead th {{
                border-bottom: 1px solid {CARD_BORDER};
                color: {TEXT_MUTED};
                font-size: 0.78rem;
                font-weight: 800;
                letter-spacing: 0.1em;
                padding: 0 0 0.75rem;
                text-align: left;
                text-transform: uppercase;
            }}
            .roster-table tbody td {{
                border-top: 1px solid {CARD_BORDER};
                color: {TEXT_DARK};
                font-size: 0.96rem;
                line-height: 1.45;
                padding: 0.82rem 0;
                vertical-align: top;
            }}
            .roster-table tbody tr:first-child td {{
                border-top: none;
            }}
            .roster-status {{
                color: {SUCCESS_GREEN};
                font-weight: 700;
            }}
            .sidebar-heading {{
                color: #5c6884;
                font-size: 0.78rem;
                font-weight: 800;
                letter-spacing: 0.1em;
                text-transform: uppercase;
                margin-top: 1.4rem;
                margin-bottom: 0.7rem;
            }}
            .sidebar-status-row {{
                align-items: center;
                background: rgba(255, 255, 255, 0.72);
                border: 1px solid #eceef2;
                border-radius: 12px;
                color: {TEXT_DARK};
                display: flex;
                font-size: 0.92rem;
                font-weight: 700;
                justify-content: space-between;
                margin-bottom: 0.55rem;
                padding: 0.8rem 0.9rem;
            }}
            .sidebar-status-label {{
                color: {TEXT_DARK};
            }}
            .sidebar-status-value {{
                color: {SUCCESS_GREEN};
            }}
            .sync-summary {{
                background: linear-gradient(180deg, #ffffff 0%, #fcf7fb 100%);
                border: 1px solid #f3d4e8;
                border-radius: 14px;
                padding: 0.85rem 0.9rem;
                margin-top: 0.8rem;
            }}
            .data-card {{
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                background:
                    linear-gradient(#ffffff, #ffffff) padding-box,
                    linear-gradient(90deg, rgba(209, 34, 142, 0.85), rgba(91, 141, 239, 0.85)) border-box;
                border: 1px solid transparent;
                padding: 20px;
                min-height: 220px;
            }}
            .data-card-icon {{
                align-items: center;
                background: #f6f3f7;
                border: 1px solid #f0c8e0;
                border-radius: 16px;
                color: {UPLANDS_PINK};
                display: inline-flex;
                font-size: 1.15rem;
                height: 2.9rem;
                justify-content: center;
                margin-bottom: 1rem;
                width: 2.9rem;
            }}
            .data-card-title {{
                color: {TEXT_MUTED};
                font-size: 0.76rem;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                margin-bottom: 0.75rem;
            }}
            .data-card-value {{
                color: {TEXT_DARK};
                font-size: 2.8rem;
                font-weight: 800;
                line-height: 1.05;
                margin-bottom: 0.55rem;
            }}
            .data-card-caption {{
                color: {TEXT_MUTED};
                font-size: 0.95rem;
                line-height: 1.45;
                margin-bottom: 0.9rem;
            }}
            .data-card-subtext {{
                color: #344054;
                font-size: 0.94rem;
                line-height: 1.55;
            }}
            .indicator-row {{
                display: flex;
                align-items: flex-start;
                gap: 0.55rem;
                margin-bottom: 0.75rem;
            }}
            .indicator-dot {{
                width: 0.72rem;
                height: 0.72rem;
                min-width: 0.72rem;
                border-radius: 50%;
                margin-top: 0.28rem;
            }}
            .indicator-label {{
                font-weight: 700;
                color: {TEXT_DARK};
                font-size: 1.02rem;
            }}
            .indicator-reason {{
                color: {TEXT_MUTED};
                font-size: 0.9rem;
                line-height: 1.4;
            }}
            .audit-panel {{
                background: #ffffff;
                border-radius: 12px;
                background:
                    linear-gradient(#ffffff, #ffffff) padding-box,
                    linear-gradient(90deg, rgba(209, 34, 142, 0.75), rgba(91, 141, 239, 0.75)) border-box;
                border: 1px solid transparent;
                box-shadow: 0 14px 24px rgba(189, 77, 154, 0.08);
                padding: 1.25rem 1.3rem;
                margin-top: 1rem;
            }}
            .audit-panel-heading {{
                color: {TEXT_MUTED};
                font-size: 0.82rem;
                font-weight: 800;
                letter-spacing: 0.14em;
                text-transform: uppercase;
                margin-bottom: 0.55rem;
            }}
            .audit-panel-status {{
                color: {TEXT_DARK};
                font-size: 3.4rem;
                font-weight: 900;
                letter-spacing: -0.04em;
                line-height: 1;
                margin-bottom: 0.6rem;
            }}
            .audit-status-ok {{
                color: {SUCCESS_GREEN};
            }}
            .audit-status-critical {{
                color: {ALERT_RED};
            }}
            .audit-status-neutral {{
                color: {TEXT_MUTED};
            }}
            .audit-panel-caption {{
                color: {TEXT_MUTED};
                font-size: 0.98rem;
                line-height: 1.45;
                margin-bottom: 1rem;
            }}
            .audit-detail-row {{
                border-top: 1px solid {CARD_BORDER};
                padding-top: 0.85rem;
                margin-top: 0.85rem;
            }}
            .audit-detail-top {{
                align-items: center;
                display: flex;
                justify-content: space-between;
                gap: 1rem;
                margin-bottom: 0.25rem;
            }}
            .audit-detail-title {{
                color: {TEXT_DARK};
                font-size: 1rem;
                font-weight: 800;
            }}
            .audit-detail-status {{
                font-size: 1.35rem;
                font-weight: 900;
                letter-spacing: -0.02em;
            }}
            .audit-detail-reason {{
                color: {TEXT_MUTED};
                font-size: 0.92rem;
                line-height: 1.45;
            }}
            .missing-worker-name {{
                color: {ALERT_RED};
                display: block;
                font-size: 1.08rem;
                font-weight: 800;
                line-height: 1.45;
            }}
            .audit-summary-line {{
                color: {TEXT_DARK};
                font-size: 1rem;
                font-weight: 700;
                line-height: 1.45;
            }}
            [data-testid="stMetric"] {{
                background: #ffffff;
                border-radius: 12px;
                border: 1px solid {CARD_BORDER};
                box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                padding: 1rem;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_sidebar(
    repository: DocumentRepository,
    station_label: str,
    project_setup: ProjectSetup,
) -> None:
    """Render the branded sidebar and sync controls."""

    if UPLANDS_LOGO.exists():
        st.image(str(UPLANDS_LOGO), width=220)
    if NATIONAL_GRID_LOGO.exists():
        st.image(str(NATIONAL_GRID_LOGO), width=120)

    if st.button("SYNC WORKSPACE", use_container_width=True):
        with st.spinner("Syncing Uplands workspace..."):
            filed_assets = file_and_index_all(repository)
        auto_capture_messages = [
            (
                f"Captured {asset.auto_captured_document_type.label} expiry for "
                f"{asset.auto_captured_carrier_name}: "
                f"{asset.auto_captured_expiry_date.isoformat()}"
            )
            for asset in filed_assets
            if asset.auto_captured_expiry_date is not None
            and asset.auto_captured_carrier_name is not None
            and asset.auto_captured_document_type is not None
        ]
        st.session_state["sync_summary"] = {
            "moved_count": len(filed_assets),
            "file_names": [asset.destination_path.name for asset in filed_assets],
            "auto_capture_messages": auto_capture_messages,
        }
        st.session_state["sync_auto_capture_messages"] = auto_capture_messages
        st.rerun()

    for message in st.session_state.pop("sync_auto_capture_messages", []):
        st.toast(message, icon="✅")

    sync_summary = st.session_state.get("sync_summary")
    if sync_summary is not None:
        st.markdown("<div class='sync-summary'>", unsafe_allow_html=True)
        st.progress(100)
        st.caption(f"{sync_summary['moved_count']} file(s) filed into the workspace.")
        if sync_summary["file_names"]:
            st.write("\n".join(f"- {file_name}" for file_name in sync_summary["file_names"]))
        else:
            st.write("- No new files detected in the ingest folder.")
        if sync_summary.get("auto_capture_messages"):
            st.success("\n".join(sync_summary["auto_capture_messages"]))
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='sidebar-heading'>Workspace Status</div>", unsafe_allow_html=True)
    for label in (
        "📥 Ingest Inbox",
        "📁 File 1 Ready",
        "📋 File 2 Ready",
        "🛡️ File 3 Ready",
        "⚡ File 4 Ready",
    ):
        st.markdown(
            (
                "<div class='sidebar-status-row'>"
                f"<span class='sidebar-status-label'>{label}</span>"
                "<span class='sidebar-status-value'>Active</span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    _render_sidebar_project_setup(project_setup)
    _render_workspace_doctor()

    station = _get_file_station(station_label)
    st.markdown(
        f"<div class='sidebar-heading'>Quick Actions · {station.number}</div>",
        unsafe_allow_html=True,
    )
    if station.number == "FILE 1":
        _render_sidebar_compliance_admin(repository, project_setup)
    elif station.number == "FILE 4":
        _render_sidebar_tools(repository, project_setup)
    elif station.number == "FILE 3":
        _render_sidebar_file_3_quick_actions(repository, project_setup)
    elif station.number == "INDUCTION":
        _render_sidebar_site_induction_quick_actions(project_setup)
    else:
        _render_sidebar_file_2_quick_actions(repository, project_setup)


def _render_sidebar_project_setup(project_setup: ProjectSetup) -> None:
    """Render the persisted project metadata form."""

    with st.expander("Project Setup", expanded=False):
        flash_message = st.session_state.pop("project_setup_flash", None)
        if flash_message is not None:
            st.success(flash_message)

        with st.form("project_setup_form", clear_on_submit=False):
            current_site_name = st.text_input(
                "Current Site Name",
                value=project_setup.current_site_name,
            )
            job_number = st.text_input(
                "Job Number",
                value=project_setup.job_number,
            )
            site_address = st.text_input(
                "Site Address",
                value=project_setup.site_address,
            )
            client_name = st.text_input(
                "Client Name",
                value=project_setup.client_name,
            )
            submitted = st.form_submit_button(
                "Save Project Setup",
                use_container_width=True,
            )

        if not submitted:
            return

        saved_setup = ProjectSetup(
            current_site_name=current_site_name.strip() or PROJECT_NAME,
            job_number=job_number.strip(),
            site_address=site_address.strip(),
            client_name=client_name.strip(),
        )
        _save_project_setup(saved_setup)
        st.session_state["project_setup"] = saved_setup
        st.session_state["project_setup_flash"] = "Project setup saved."
        st.rerun()


def _render_workspace_doctor() -> None:
    """Render the workspace doctor status block in the sidebar."""

    with st.expander("🏥 Workspace Doctor", expanded=False):
        if st.button(
            "Re-scan Workspace",
            use_container_width=True,
            key="workspace_doctor_rescan",
        ):
            st.rerun()

        diagnostic_checks = run_workspace_diagnostic()
        failing_checks = [check for check in diagnostic_checks if not check.exists]

        if not failing_checks:
            st.success("✅ System Healthy: All folders and templates verified.")
            return

        for diagnostic_check in failing_checks:
            st.warning(f"⚠️ Missing: {diagnostic_check.display_path}")


def _render_sidebar_compliance_admin(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render the carrier compliance editor for expiry-date maintenance."""

    with st.expander("Compliance", expanded=False):
        flash_message = st.session_state.pop("carrier_compliance_flash", None)
        if flash_message is not None:
            if flash_message["level"] == "warning":
                st.warning(flash_message["message"])
            else:
                st.success(flash_message["message"])

        carrier_options = _get_known_carrier_names(repository)
        default_carrier = ABUCS_NAME if ABUCS_NAME in carrier_options else carrier_options[0]
        selected_carrier = st.session_state.get(
            "carrier_compliance_selected_carrier",
            default_carrier,
        )
        if selected_carrier not in carrier_options:
            selected_carrier = default_carrier

        selected_document_type_value = st.session_state.get(
            "carrier_compliance_selected_document_type",
            CarrierComplianceDocumentType.LICENCE.value,
        )
        try:
            selected_document_type = CarrierComplianceDocumentType(
                selected_document_type_value
            )
        except ValueError:
            selected_document_type = CarrierComplianceDocumentType.LICENCE

        existing_document = _get_carrier_compliance_document(
            repository,
            selected_carrier,
            selected_document_type,
        )
        default_expiry_date = (
            existing_document.expiry_date
            if existing_document is not None
            else date.today() + timedelta(days=365)
        )

        st.caption("Update carrier licence and insurance expiry dates.")
        if existing_document is not None:
            st.caption(
                "Current record: "
                f"{existing_document.reference_number} | expires "
                f"{existing_document.expiry_date.isoformat()}"
            )
        else:
            inferred_reference_number = _infer_carrier_reference_number(
                repository,
                selected_carrier,
                selected_document_type,
            )
            st.caption(
                "No compliance record saved yet. "
                f"Reference will use {inferred_reference_number}."
            )

        with st.form("carrier_compliance_form", clear_on_submit=False):
            carrier_name = st.selectbox(
                "Carrier",
                carrier_options,
                index=carrier_options.index(selected_carrier),
                key="carrier_compliance_selected_carrier",
            )
            document_type_options = list(CarrierComplianceDocumentType)
            document_type = st.selectbox(
                "Document Type",
                document_type_options,
                index=document_type_options.index(selected_document_type),
                format_func=lambda item: item.label,
                key="carrier_compliance_selected_document_type",
            )
            expiry_date = st.date_input(
                "Expiry Date",
                value=default_expiry_date,
            )
            submitted = st.form_submit_button("Update Status", use_container_width=True)

        if not submitted:
            return

        saved_document = _upsert_carrier_compliance_document(
            repository,
            carrier_name=carrier_name,
            carrier_document_type=document_type,
            expiry_date=expiry_date,
            site_name=project_setup.current_site_name,
        )

        if saved_document.expires_within(30):
            st.session_state["carrier_compliance_flash"] = {
                "level": "warning",
                "message": (
                    f"{carrier_name} {document_type.label} saved, but it remains "
                    "critical because it expires within 30 days."
                ),
            }
        else:
            st.session_state["carrier_compliance_flash"] = {
                "level": "success",
                "message": (
                    f"{carrier_name} {document_type.label} saved. "
                    "Compliance indicators refreshed."
                ),
            }
        st.rerun()


def _render_sidebar_tools(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render document-generation tools for live site paperwork."""

    with st.expander("Tools", expanded=False):
        st.caption("Generate a draft ladder permit from the live contractor roster.")
        st.caption(f"Output folder: {PERMITS_DESTINATION.name}")
        st.caption(
            "Job number source: "
            f"{project_setup.job_number or 'Project Setup field is currently blank.'}"
        )
        st.caption(
            "Site-manager sign-off fields are auto-filled as "
            f"{SITE_MANAGER_NAME} | Project Manager."
        )
        if not project_setup.job_number.strip():
            st.warning("Enter the Job Number in Project Setup before generating permits.")
        if st.button(
            "Refresh & Sync",
            key="file_4_refresh_sync",
            use_container_width=True,
        ):
            sync_result = sync_file_4_permit_records(
                repository,
                site_name=project_setup.current_site_name,
            )
            if sync_result.removed_count:
                st.session_state["ladder_permit_flash"] = {
                    "level": "success",
                    "message": (
                        f"Removed {sync_result.removed_count} ghost permit record"
                        f"{'' if sync_result.removed_count == 1 else 's'} from File 4."
                    ),
                }
            else:
                st.session_state["ladder_permit_flash"] = {
                    "level": "success",
                    "message": "File 4 sync complete. No ghost permit records found.",
                }
            st.rerun()
        flash_message = st.session_state.pop("ladder_permit_flash", None)
        if flash_message is not None:
            if flash_message["level"] == "error":
                st.error(flash_message["message"])
            else:
                st.success(flash_message["message"])

        roster = build_site_worker_roster(site_name=project_setup.current_site_name)
        worker_options = _build_file_4_worker_options(
            repository,
            site_name=project_setup.current_site_name,
        )

        if not roster or not worker_options:
            st.info("No workers found in the live contractor roster.")
            return

        worker_labels = list(worker_options)

        with st.form("ladder_permit_form", clear_on_submit=False):
            selected_worker_label = st.selectbox(
                "Operative",
                options=worker_labels,
            )
            selected_worker, _ = worker_options[selected_worker_label]
            st.text_input(
                "Company",
                value=selected_worker.company,
                disabled=True,
            )
            description_of_work = st.text_input(
                "Description of Work",
                value=st.session_state.get(
                    "ladder_permit_description_of_work",
                    "",
                ),
                key="ladder_permit_description_of_work",
            )
            location_of_work = st.text_input(
                "Location of Work",
                value=st.session_state.get(
                    "ladder_permit_location_of_work",
                    "",
                ),
                key="ladder_permit_location_of_work",
            )
            supervisor_name = st.text_input(
                "Supervisor Name",
                value=st.session_state.get(
                    "ladder_permit_supervisor_name",
                    SITE_MANAGER_NAME,
                ),
                key="ladder_permit_supervisor_name",
            )
            with st.expander("Safety Checklist (UHSF21.09)", expanded=False):
                safety_checklist = {
                    question_number: st.checkbox(
                        question_label,
                        value=st.session_state.get(
                            f"ladder_permit_q{question_number}",
                            True,
                        ),
                        key=f"ladder_permit_q{question_number}",
                    )
                    for question_number, question_label in LADDER_CHECKLIST_QUESTIONS.items()
                }
            with st.expander("Ladder Inspection Details", expanded=False):
                inspection_checked_by = st.text_input(
                    "Inspected By",
                    value=st.session_state.get(
                        "ladder_permit_inspection_checked_by",
                        SITE_MANAGER_NAME,
                    ),
                    key="ladder_permit_inspection_checked_by",
                )
                inspection_rungs_ok = st.checkbox(
                    "Rungs OK?",
                    value=st.session_state.get(
                        "ladder_permit_inspection_rungs_ok",
                        True,
                    ),
                    key="ladder_permit_inspection_rungs_ok",
                )
                inspection_stiles_ok = st.checkbox(
                    "Stiles OK?",
                    value=st.session_state.get(
                        "ladder_permit_inspection_stiles_ok",
                        True,
                    ),
                    key="ladder_permit_inspection_stiles_ok",
                )
                inspection_feet_ok = st.checkbox(
                    "Feet OK?",
                    value=st.session_state.get(
                        "ladder_permit_inspection_feet_ok",
                        True,
                    ),
                    key="ladder_permit_inspection_feet_ok",
                )
                inspection_ok_to_use = st.checkbox(
                    "Ok to Use?",
                    value=st.session_state.get(
                        "ladder_permit_inspection_ok_to_use",
                        True,
                    ),
                    key="ladder_permit_inspection_ok_to_use",
                )
                inspection_comments = st.text_input(
                    "Comments/Action",
                    value=st.session_state.get(
                        "ladder_permit_inspection_comments",
                        "No defects found",
                    ),
                    key="ladder_permit_inspection_comments",
                )
            submitted = st.form_submit_button(
                "Generate Ladder Permit",
                use_container_width=True,
                disabled=not project_setup.job_number.strip(),
            )

        if st.button(
            "Clear Form",
            key="ladder_permit_clear_form",
            use_container_width=True,
        ):
            _reset_ladder_permit_form_state()
            st.session_state["ladder_permit_flash"] = {
                "level": "success",
                "message": "File 4 permit form reset.",
            }
            st.rerun()

        if not submitted:
            return

        _, selected_record = worker_options[selected_worker_label]
        try:
            generated_permit = create_ladder_permit_draft(
                repository,
                attendance_record=selected_record,
                site_worker=selected_worker,
                description_of_work=description_of_work,
                location_of_work=location_of_work,
                supervisor_name=supervisor_name,
                safety_checklist=safety_checklist,
                inspection_checked_by=inspection_checked_by,
                inspection_rungs_ok=inspection_rungs_ok,
                inspection_stiles_ok=inspection_stiles_ok,
                inspection_feet_ok=inspection_feet_ok,
                inspection_ok_to_use=inspection_ok_to_use,
                inspection_comments=inspection_comments,
                site_name=project_setup.current_site_name,
                job_number=project_setup.job_number,
            )
        except ValidationError as exc:
            st.session_state["ladder_permit_flash"] = {
                "level": "error",
                "message": str(exc),
            }
            st.rerun()
        except TemplateValidationError as exc:
            st.session_state["ladder_permit_flash"] = {
                "level": "error",
                "message": f"Official ladder permit template failed validation: {exc}",
            }
            st.rerun()
        except Exception as exc:
            st.session_state["ladder_permit_flash"] = {
                "level": "error",
                "message": f"Unable to generate ladder permit: {exc}",
            }
            st.rerun()

        _open_file_for_printing(generated_permit.output_path)
        st.session_state["ladder_permit_flash"] = {
            "level": "success",
            "message": (
                f"{generated_permit.permit.permit_number} saved to FILE_4_Permits "
                f"for {generated_permit.permit.worker_name}."
            ),
        }
        st.toast(
            f"Ladder permit ready: {generated_permit.output_path.name}",
            icon="🪜",
        )
        st.rerun()


def _render_sidebar_file_2_quick_actions(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render the File 2 sidebar quick actions."""

    attendance_register = _get_lovedean_attendance_register(
        repository,
        site_name=project_setup.current_site_name,
    )
    latest_site_check = _get_latest_weekly_site_check(
        repository,
        site_name=project_setup.current_site_name,
    )
    plant_assets = _get_file_2_plant_assets(
        repository,
        site_name=project_setup.current_site_name,
    )
    st.markdown(
        (
            "<div class='quick-action-panel'>"
            "<div class='quick-action-title'>Registers</div>"
            "<div class='quick-action-copy'>"
            "Morning Station quick view for File 2."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    if attendance_register is None:
        st.info("Use SYNC WORKSPACE to file the next attendance export into File 2.")
        return
    st.caption(
        "Latest register loaded: "
        f"{attendance_register.created_at.strftime('%Y-%m-%d %H:%M')}"
    )
    st.caption(
        f"Rows: {len(attendance_register.attendance_records)} | "
        f"Workers today: {len(_get_todays_attendance_records(attendance_register))}"
    )
    st.caption(
        f"Plant assets: {len(plant_assets)} | "
        f"Pending serials: {sum(1 for asset in plant_assets if asset.is_pending)}"
    )
    st.caption(
        f"Latest site check: {_format_site_check_timestamp(latest_site_check)}"
    )


def _render_sidebar_file_3_quick_actions(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render the File 3 sidebar quick actions."""

    roster = build_site_worker_roster(site_name=project_setup.current_site_name)
    with st.expander("Roster Feed", expanded=False):
        if not roster:
            st.info("No KPI roster data was found for this project yet.")
            return
        companies = sorted({worker.company for worker in roster}, key=str.casefold)
        latest_date = max(worker.last_on_site_date for worker in roster)
        st.success(
            f"{len(roster)} worker(s) across {len(companies)} compan"
            f"{'y' if len(companies) == 1 else 'ies'} are verified by paper record."
        )
        st.caption(f"Latest on site: {latest_date.strftime('%d/%m/%Y')}")
        st.caption("Source: site-kpi-backup JSON feed")


def _render_sidebar_site_induction_quick_actions(project_setup: ProjectSetup) -> None:
    """Render the induction kiosk sidebar hints."""

    with st.expander("Kiosk", expanded=False):
        st.success("Live site induction form with signature capture is ready.")
        st.caption(f"Project: {project_setup.current_site_name}")
        st.caption(f"Signatures folder: {FILE_3_SIGNATURES_DIR.name}")
        st.caption(f"Completed docs: {FILE_3_COMPLETED_INDUCTIONS_DIR.name}")


def _render_file_station_navigation() -> str:
    """Render the top-level four-file navigator and return the active station."""

    current_label = str(st.session_state.get("active_file_station", DEFAULT_FILE_STATION_LABEL))
    current_index = next(
        (
            index
            for index, station in enumerate(FILE_STATIONS)
            if station.label == current_label
        ),
        1,
    )
    return st.radio(
        "Site Files",
        options=[station.label for station in FILE_STATIONS],
        index=current_index,
        key="active_file_station",
        horizontal=True,
        label_visibility="collapsed",
    )


def _render_active_station(
    repository: DocumentRepository,
    station_label: str,
    project_setup: ProjectSetup,
) -> None:
    """Render the active file-station page."""

    station = _get_file_station(station_label)
    _render_station_header(station, project_setup)

    if station.number == "FILE 1":
        _render_file_1_station(repository, project_setup)
        return
    if station.number == "FILE 2":
        _render_file_2_station(repository, project_setup)
        return
    if station.number == "FILE 3":
        _render_file_3_station(repository, project_setup)
        return
    if station.number == "INDUCTION":
        _render_site_induction_station(repository, project_setup)
        return
    _render_file_4_station(repository, project_setup)


def _render_station_header(station: FileStation, project_setup: ProjectSetup) -> None:
    """Render the file header card."""

    st.markdown(
        (
            "<div class='station-header'>"
            f"<div class='station-number'>{station.number}</div>"
            f"<div class='station-title'>{station.title}</div>"
            f"<div class='station-project-name'>{project_setup.current_site_name}</div>"
            f"<div class='station-subtitle'>{station.subtitle}</div>"
            f"<div class='station-project-meta'>"
            f"Client: {project_setup.client_name or 'Not set'}"
            f"{' | Job ' + project_setup.job_number if project_setup.job_number else ''}"
            f"{' | ' + project_setup.site_address if project_setup.site_address else ''}"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_file_1_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render File 1: Environment & Waste."""

    waste_notes = _get_lovedean_waste_notes(
        repository,
        site_name=project_setup.current_site_name,
    )
    active_waste_notes = [
        note for note in waste_notes if note.status == DocumentStatus.ACTIVE
    ]
    current_month_active_waste_notes = [
        note
        for note in active_waste_notes
        if note.date.month == date.today().month and note.date.year == date.today().year
    ]
    waste_tonnage = sum(note.quantity_tonnes for note in current_month_active_waste_notes)
    monthly_waste_count = len(current_month_active_waste_notes)
    unverified_count = len(
        [
            note
            for note in active_waste_notes
            if note.verification_status.value == "UNVERIFIED"
        ]
    )
    file_1_indexed_files = repository.list_indexed_files(file_group=WasteTransferNoteDocument.file_group)
    abucs_rows = _get_abucs_status_rows(repository)
    waste_kpi_metadata = get_waste_kpi_sheet_metadata(
        site_name=project_setup.current_site_name,
        site_address=project_setup.site_address,
        fallback_project_number=project_setup.job_number,
    )
    carrier_status = (
        "OK"
        if abucs_rows and all(row.status == ComplianceAlertStatus.OK for row in abucs_rows)
        else "CRITICAL"
    )

    cards = st.columns(4)
    with cards[0]:
        _render_metric_card(
            title="Waste Tonnage",
            icon="♻",
            value=f"{waste_tonnage:.2f} t",
            caption=f"Active waste moved in {date.today():%B %Y}.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Monthly WTNs: <strong>{monthly_waste_count}</strong>"
                "</div>"
            ),
        )
    with cards[1]:
        _render_metric_card(
            title="Active WTNs",
            icon="🧾",
            value=str(len(active_waste_notes)),
            caption="Live waste transfer notes held in File 1.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Unverified tickets: <strong>{unverified_count}</strong>"
                "</div>"
            ),
        )
    with cards[2]:
        _render_metric_card(
            title="Carrier Gate",
            icon="🚛",
            value=carrier_status,
            caption="Current Abucs carrier gatekeeper result.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Licence and insurance checks: <strong>{len(abucs_rows)}</strong>"
                "</div>"
            ),
        )
    with cards[3]:
        _render_metric_card(
            title="Indexed Records",
            icon="📚",
            value=str(len(file_1_indexed_files)),
            caption="Waste notes, carrier docs, and waste reports in File 1.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Current month activity: <strong>{monthly_waste_count}</strong>"
                "</div>"
            ),
        )

    columns = st.columns(2)
    with columns[0]:
        _render_carrier_compliance_panel(abucs_rows)
    with columns[1]:
        _render_file_1_waste_register_panel(
            repository,
            project_setup=project_setup,
            waste_notes=active_waste_notes,
            waste_kpi_metadata=waste_kpi_metadata,
        )

    _render_file_1_waste_log_panel(
        repository,
        project_setup=project_setup,
        waste_kpi_metadata=waste_kpi_metadata,
        waste_notes=active_waste_notes,
    )


def _render_file_1_waste_register_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
    waste_notes: List[WasteTransferNoteDocument],
    waste_kpi_metadata: Any,
) -> None:
    """Render the live File 1 waste register and print action."""

    st.markdown(
        (
            "<div class='panel-card'>"
            "<div class='panel-heading'>Waste Register</div>"
            "<div class='panel-title'>Live Waste Removal History</div>"
            "<div class='panel-caption'>"
            "Full File 1 Tab 17 history used by the printable UHSF50.0 register."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    if waste_kpi_metadata.workbook_path is not None:
        st.caption(f"KPI source: {waste_kpi_metadata.workbook_path.name}")
    else:
        st.caption("KPI source: not found in FILE_1_Environment/Waste_Reports.")

    register_rows = _build_live_waste_register_rows(waste_notes)
    if register_rows:
        st.dataframe(
            pd.DataFrame(register_rows),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No waste register rows have been logged yet.")

    if st.button(
        "🖨️ Print Waste Register",
        key="print_waste_register",
        use_container_width=True,
    ):
        try:
            generated_register = generate_waste_register_document(
                repository,
                site_name=project_setup.current_site_name,
                client_name=(
                    waste_kpi_metadata.client_name or project_setup.client_name
                ),
                site_address=(
                    waste_kpi_metadata.site_address or project_setup.site_address
                ),
                manager_name=(
                    waste_kpi_metadata.manager_name or SITE_MANAGER_NAME
                ),
            )
        except TemplateValidationError as exc:
            st.error(f"Official waste register template failed validation: {exc}")
        except Exception as exc:
            st.error(f"Unable to generate waste register: {exc}")
        else:
            _open_file_for_printing(generated_register.output_path)
            st.success(
                "Waste register ready: "
                f"{generated_register.output_path}"
            )
            st.caption(
                f"Rows printed: {generated_register.row_count} | Output folder: {FILE_1_OUTPUT_DIR.name}"
            )


def _render_file_1_waste_log_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
    waste_kpi_metadata: Any,
    waste_notes: List[WasteTransferNoteDocument],
) -> None:
    """Render the File 1 WTN smart-scan form for already-filed notes."""

    st.markdown(
        (
            "<div class='panel-card'>"
            "<div class='panel-heading'>Smart Scan</div>"
            "<div class='panel-title'>Filed Waste Transfer Notes</div>"
            "<div class='panel-caption'>"
            "Use the WTNs already synced from the ingest folder, bridge the File 1 KPI workbook header, and update the live waste register."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    flash_message = st.session_state.pop("waste_log_flash", None)
    if flash_message is not None:
        if flash_message["level"] == "error":
            st.error(flash_message["message"])
        else:
            st.success(flash_message["message"])

    if not waste_notes:
        st.info("No filed WTNs found in File 1 yet. Run SYNC WORKSPACE to ingest the waste PDFs first.")
        return

    note_options = {
        (
            f"{waste_note.wtn_number} | {waste_note.date.strftime('%d/%m/%Y')} | "
            f"{waste_note.quantity_tonnes:.2f} t | {waste_note.carrier_name}"
        ): waste_note
        for waste_note in sorted(
            waste_notes,
            key=lambda note: (note.date, note.created_at, note.wtn_number),
            reverse=True,
        )
    }
    selected_note_label = st.selectbox(
        "Select Waste Transfer Note",
        options=list(note_options),
        key="file_1_selected_waste_note",
    )
    selected_waste_note = note_options[selected_note_label]
    selected_source_path = _get_file_1_waste_note_source_path(
        repository,
        selected_waste_note,
    )

    scanned_waste_note = None
    if selected_source_path is not None and selected_source_path.exists():
        try:
            scanned_waste_note = smart_scan_waste_transfer_note(
                repository,
                source_path=selected_source_path,
            )
        except Exception:
            scanned_waste_note = None
        st.caption(f"WTN source file: {selected_source_path.name}")
    else:
        st.caption("WTN source file is not currently indexed on disk.")

    workbook_client_name = waste_kpi_metadata.client_name or project_setup.client_name
    workbook_site_address = (
        waste_kpi_metadata.site_address or project_setup.site_address
    )
    workbook_project_number = (
        waste_kpi_metadata.project_number or project_setup.job_number
    )
    workbook_manager_name = (
        waste_kpi_metadata.manager_name or SITE_MANAGER_NAME
    )

    if waste_kpi_metadata.workbook_path is None:
        st.warning(
            "No File 1 KPI workbook was found. The form is using the current Project Setup values."
        )

    scan_columns = st.columns(3)
    with scan_columns[0]:
        st.text_input(
            "Client Name",
            value=workbook_client_name,
            disabled=True,
        )
    with scan_columns[1]:
        st.text_input(
            "Site Address",
            value=workbook_site_address,
            disabled=True,
        )
    with scan_columns[2]:
        st.text_input(
            "Project Number",
            value=workbook_project_number,
            disabled=True,
        )

    default_carrier_name = (
        scanned_waste_note.carrier_name
        if scanned_waste_note is not None and scanned_waste_note.carrier_name
        else selected_waste_note.carrier_name
    )
    default_vehicle_registration = (
        scanned_waste_note.vehicle_registration
        if scanned_waste_note is not None and scanned_waste_note.vehicle_registration
        else selected_waste_note.vehicle_registration
    )
    default_waste_description = (
        scanned_waste_note.waste_description
        if scanned_waste_note is not None and scanned_waste_note.waste_description
        else selected_waste_note.waste_description
    )
    default_ticket_date = (
        scanned_waste_note.ticket_date
        if scanned_waste_note is not None
        else selected_waste_note.date
    )
    default_quantity_tonnes = (
        scanned_waste_note.quantity_tonnes
        if scanned_waste_note is not None and scanned_waste_note.quantity_tonnes is not None
        else selected_waste_note.quantity_tonnes
    )
    default_ewc_code = (
        scanned_waste_note.ewc_code
        if scanned_waste_note is not None and scanned_waste_note.ewc_code
        else selected_waste_note.ewc_code
    )
    default_destination_facility = (
        scanned_waste_note.destination_facility
        if scanned_waste_note is not None and scanned_waste_note.destination_facility
        else selected_waste_note.destination_facility
    )

    with st.form("file_1_waste_log_form", clear_on_submit=False):
        detail_columns = st.columns(3)
        with detail_columns[0]:
            carrier_name = st.text_input(
                "Carrier Name",
                value=default_carrier_name,
            )
        with detail_columns[1]:
            vehicle_registration = st.text_input(
                "Vehicle Reg",
                value=default_vehicle_registration,
            )
        with detail_columns[2]:
            wtn_number = st.text_input(
                "WTN Reference",
                value=selected_waste_note.wtn_number,
                disabled=True,
            )

        waste_description = st.text_input(
            "Description of Waste",
            value=default_waste_description,
        )

        detail_columns = st.columns(4)
        with detail_columns[0]:
            ticket_date = st.date_input(
                "Date",
                value=default_ticket_date,
            )
        with detail_columns[1]:
            quantity_tonnes = st.number_input(
                "Quantity (tonnes)",
                min_value=0.0,
                step=0.01,
                value=float(default_quantity_tonnes or 0.0),
            )
        with detail_columns[2]:
            ewc_code = st.text_input(
                "EWC Code",
                value=default_ewc_code,
            )
        with detail_columns[3]:
            destination_facility = st.text_input(
                "Destination Facility",
                value=default_destination_facility,
            )

        submitted = st.form_submit_button("Log Waste", use_container_width=True)

    if not submitted:
        if scanned_waste_note is not None and scanned_waste_note.extracted_text.strip():
            with st.expander("Scanned text preview", expanded=False):
                st.text(scanned_waste_note.extracted_text[:3000])
        return

    if quantity_tonnes <= 0:
        st.session_state["waste_log_flash"] = {
            "level": "error",
            "message": "Quantity (tonnes) must be greater than zero before logging waste.",
        }
        st.rerun()

    try:
        logged_waste_note = update_logged_waste_transfer_note(
            repository,
            source_document=selected_waste_note,
            site_name=project_setup.current_site_name,
            carrier_name=carrier_name,
            vehicle_registration=vehicle_registration,
            waste_description=waste_description,
            ticket_date=ticket_date,
            quantity_tonnes=float(quantity_tonnes),
            ewc_code=ewc_code,
            destination_facility=destination_facility,
        )
    except ValidationError as exc:
        st.session_state["waste_log_flash"] = {
            "level": "error",
            "message": str(exc),
        }
        st.rerun()
    except Exception as exc:
        st.session_state["waste_log_flash"] = {
            "level": "error",
            "message": f"Unable to log waste transfer note: {exc}",
        }
        st.rerun()

    st.session_state["waste_log_flash"] = {
        "level": "success",
        "message": (
            f"{logged_waste_note.waste_transfer_note.wtn_number} logged to File 1. "
            f"Source file: {logged_waste_note.stored_file_path.name if logged_waste_note.stored_file_path else 'already filed'}"
        ),
    }
    st.rerun()


def _render_file_2_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render File 2: Registers & Diary."""

    attendance_register = _get_lovedean_attendance_register(
        repository,
        site_name=project_setup.current_site_name,
    )
    todays_records = _get_todays_attendance_records(attendance_register)
    latest_site_check = _get_latest_weekly_site_check(
        repository,
        site_name=project_setup.current_site_name,
    )
    total_attendance_rows = (
        len(attendance_register.attendance_records) if attendance_register else 0
    )
    total_attendance_hours = (
        sum(record.totalHours for record in attendance_register.attendance_records)
        if attendance_register
        else 0.0
    )
    workers_today = len({record.workerName for record in todays_records})
    register_date = (
        attendance_register.created_at.date().isoformat()
        if attendance_register is not None
        else "No data"
    )
    latest_site_check_status = _weekly_site_check_dashboard_status(latest_site_check)
    indexed_register_files = repository.list_indexed_files(
        file_group=SiteAttendanceRegister.file_group
    )

    cards = st.columns(4)
    with cards[0]:
        _render_metric_card(
            title="Man Hours",
            icon="⏱",
            value=f"{total_attendance_hours:.1f}",
            caption="Total logged hours in the latest attendance register.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Rows imported: <strong>{total_attendance_rows}</strong>"
                "</div>"
            ),
        )
    with cards[1]:
        _render_metric_card(
            title="Lads on Site",
            icon="👷",
            value=str(workers_today),
            caption="Workers currently present on today's attendance sheet.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Attendance date: <strong>{date.today().isoformat()}</strong>"
                "</div>"
            ),
        )
    with cards[2]:
        _render_metric_card(
            title="Daily Checks",
            icon="✅",
            value=latest_site_check_status,
            caption="Latest daily/weekly site check sheet status.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Latest check: <strong>{_format_site_check_timestamp(latest_site_check)}</strong>"
                "</div>"
            ),
        )
    with cards[3]:
        _render_metric_card(
            title="Register Files",
            icon="📋",
            value=str(len(indexed_register_files)),
            caption="Attendance exports indexed in File 2.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Latest register: <strong>{register_date}</strong>"
                "</div>"
            ),
        )

    columns = st.columns(2)
    with columns[0]:
        _render_file_2_site_checks_panel(
            repository,
            site_name=project_setup.current_site_name,
            latest_site_check=latest_site_check,
        )
    with columns[1]:
        _render_file_list_panel(
            heading="Attendance Register",
            title="Today's Workforce",
            caption="Man hours and lads on site in one glance for the morning brief.",
            items=[
                (
                    f"{record.workerName} | {record.company} | "
                    f"{record.timeIn.strftime('%H:%M')} - {record.timeOut.strftime('%H:%M')} | "
                    f"{record.totalHours:.1f} hrs"
                )
                for record in todays_records
            ],
            empty_message="No workers are listed on today's attendance register.",
        )

    register_columns = st.columns(2)
    with register_columns[0]:
        _render_file_2_plant_register_panel(
            repository,
            project_setup=project_setup,
        )
    with register_columns[1]:
        _render_file_2_register_link(
            title="Toolbox Talk Register",
            caption="Open the live File 2 toolbox talk register folder.",
            destination=TOOLBOX_TALK_REGISTER_DIR,
            button_label="Open Toolbox Talk Register",
        )


def _render_file_3_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render File 3: Contractor Master."""

    roster = build_site_worker_roster(site_name=project_setup.current_site_name)
    companies = sorted({worker.company for worker in roster}, key=str.casefold)
    latest_roster_date = (
        max(worker.last_on_site_date for worker in roster) if roster else None
    )
    verified_count = sum(
        1 for worker in roster if worker.induction_status == "Verified (Paper Record)"
    )

    cards = st.columns(4)
    with cards[0]:
        _render_metric_card(
            title="Roster Workers",
            icon="👷",
            value=str(len(roster)),
            caption="Unique worker and company pairs surfaced from KPI backup data.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Companies: <strong>{len(companies)}</strong>"
                "</div>"
            ),
        )
    with cards[1]:
        _render_metric_card(
            title="Companies",
            icon="🗂",
            value=str(len(companies)),
            caption="Contractors currently represented in the live KPI roster feed.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Roster source files: <strong>JSON feed</strong>"
                "</div>"
            ),
        )
    with cards[2]:
        _render_metric_card(
            title="Last On Site",
            icon="📅",
            value=latest_roster_date.strftime("%d/%m/%Y") if latest_roster_date else "NO DATA",
            caption="Most recent attendance date found across the live KPI roster.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Project filter: <strong>{html.escape(project_setup.current_site_name)}</strong>"
                "</div>"
            ),
        )
    with cards[3]:
        _render_metric_card(
            title="Verification",
            icon="✅",
            value=str(verified_count),
            caption="Workers defaulted to verified induction status from paper records.",
            body_html=(
                "<div class='data-card-subtext'>"
                "Status policy: <strong>Verified (Paper Record)</strong>"
                "</div>"
            ),
        )

    _render_file_3_safety_panel(
        repository,
        project_setup=project_setup,
    )

    columns = st.columns(2)
    with columns[0]:
        _render_site_worker_roster_table(roster)
    with columns[1]:
        _render_file_list_panel(
            heading="Company Summary",
            title="Contractor Master Feed",
            caption="Unique companies derived automatically from the KPI backup roster.",
            items=[
                (
                    f"{company} | workers {sum(1 for worker in roster if worker.company == company)} | "
                    f"last on site {max(worker.last_on_site_date for worker in roster if worker.company == company).strftime('%d/%m/%Y')}"
                )
                for company in companies
            ],
            empty_message="No contractor roster entries are available yet.",
        )


def _render_file_3_safety_panel(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render the live File 3 RAMS and COSHH inventories."""

    site_name = project_setup.current_site_name
    rams_documents = sorted(
        [
            document
            for document in repository.list_documents(
                document_type=RAMSDocument.document_type,
                site_name=site_name,
            )
            if isinstance(document, RAMSDocument)
            and document.status != DocumentStatus.ARCHIVED
        ],
        key=lambda document: (
            document.review_date or date.min,
            document.reference.casefold(),
            document.activity_description.casefold(),
        ),
        reverse=True,
    )
    coshh_documents = sorted(
        [
            document
            for document in repository.list_documents(
                document_type=COSHHDocument.document_type,
                site_name=site_name,
            )
            if isinstance(document, COSHHDocument)
            and document.status != DocumentStatus.ARCHIVED
        ],
        key=lambda document: (
            document.review_date or date.min,
            document.reference.casefold(),
            document.substance_name.casefold(),
        ),
        reverse=True,
    )

    st.markdown(
        (
            "<div class='panel-card'>"
            "<div class='panel-heading'>Safety Inventory</div>"
            "<div class='panel-title'>Live RAMS & COSHH Records</div>"
            "<div class='panel-caption'>"
            "File 3 safety assets synced from the ingest folder, including PDF and Word submissions."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    wiped_count = st.session_state.pop("file3_safety_wipe_count", None)
    if wiped_count is not None:
        st.success(f"Deleted {wiped_count} File 3 safety record(s) from the database.")

    rams_tab, coshh_tab = st.tabs(["RAMS", "COSHH"])
    with rams_tab:
        rams_assets = [document.as_safety_asset() for document in rams_documents]
        if rams_assets:
            st.dataframe(
                pd.DataFrame(_build_file_3_rams_rows(rams_assets)),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No RAMS documents are filed for this site yet.")

        if st.button(
            "🖨️ Print RAMS Register",
            key="print_rams_register",
            use_container_width=True,
        ):
            try:
                generated_register = generate_rams_register_document(
                    repository,
                    site_name=site_name,
                )
            except TemplateValidationError as exc:
                st.error(f"Official RAMS register template failed validation: {exc}")
            except Exception as exc:
                st.error(f"Unable to generate RAMS register: {exc}")
            else:
                _open_file_for_printing(generated_register.output_path)
                st.success(f"RAMS register ready: {generated_register.output_path}")
                st.caption(
                    f"Rows printed: {generated_register.row_count} | Output folder: {FILE_3_OUTPUT_DIR.name}"
                )

    with coshh_tab:
        coshh_assets = [document.as_safety_asset() for document in coshh_documents]
        if coshh_assets:
            st.dataframe(
                pd.DataFrame(_build_file_3_coshh_rows(coshh_assets)),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No COSHH documents are filed for this site yet.")

        if st.button(
            "🖨️ Print COSHH Register",
            key="print_coshh_register",
            use_container_width=True,
        ):
            try:
                generated_register = generate_coshh_register_document(
                    repository,
                    site_name=site_name,
                )
            except TemplateValidationError as exc:
                st.error(f"Official COSHH register template failed validation: {exc}")
            except Exception as exc:
                st.error(f"Unable to generate COSHH register: {exc}")
            else:
                _open_file_for_printing(generated_register.output_path)
                st.success(f"COSHH register ready: {generated_register.output_path}")
                st.caption(
                    f"Rows printed: {generated_register.row_count} | Output folder: {FILE_3_OUTPUT_DIR.name}"
                )

    st.markdown(
        """
        <style>
        .st-key-file3_wipe_safety_database button {
            background: #D92D20 !important;
            color: #FFFFFF !important;
            border: 1px solid #D92D20 !important;
        }
        .st-key-file3_wipe_safety_database button:hover {
            background: #B42318 !important;
            color: #FFFFFF !important;
            border: 1px solid #B42318 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.warning(
        "DEV tool: this deletes all RAMS and COSHH records from SQLite. Re-syncing File 3 will rebuild them from the filed source documents."
    )
    if st.button(
        "⚠️ DEV: Wipe Safety Database",
        key="file3_wipe_safety_database",
        use_container_width=True,
    ):
        deleted_count = 0
        for document_type in (RAMSDocument.document_type, COSHHDocument.document_type):
            for document in repository.list_documents(document_type=document_type):
                repository.delete_document(document.doc_id)
                deleted_count += 1
        st.session_state["file3_safety_wipe_count"] = deleted_count
        st.rerun()


def _render_site_induction_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render the mobile-responsive induction sign-in kiosk."""

    if st.session_state.pop("site_induction_reset_pending", False):
        _reset_site_induction_form_state()

    flash_message = st.session_state.pop("site_induction_flash", None)
    if flash_message is not None:
        st.success(flash_message)
    delete_flash_message = st.session_state.pop("site_induction_delete_flash", None)
    if delete_flash_message is not None:
        st.success(delete_flash_message)

    try:
        from streamlit_drawable_canvas import st_canvas
    except ImportError:
        st.error(
            "streamlit-drawable-canvas is not installed. Install dependencies and restart the app."
        )
        return

    inductions = [
        document
        for document in repository.list_documents(
            document_type=InductionDocument.document_type,
            site_name=project_setup.current_site_name,
        )
        if isinstance(document, InductionDocument)
        and document.status != DocumentStatus.ARCHIVED
    ]
    inductions.sort(
        key=lambda document: (document.created_at, document.individual_name.casefold()),
        reverse=True,
    )

    summary_columns = st.columns([1.2, 0.8], gap="large")
    with summary_columns[0]:
        st.markdown(
            (
                "<div class='panel-card'>"
                "<div class='panel-heading'>Induction Kiosk</div>"
                "<div class='panel-title'>UHSF16.01 Mobile Sign-In</div>"
                "<div class='panel-caption'>"
                "Complete the operative induction, capture the signature, and generate the signed Word record in one flow."
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
    with summary_columns[1]:
        _render_metric_card(
            title="Inductions Logged",
            icon="📝",
            value=str(len(inductions)),
            caption="Completed induction records saved for the active project.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Output: <strong>{html.escape(FILE_3_COMPLETED_INDUCTIONS_DIR.name)}</strong>"
                "</div>"
            ),
        )

    if st.button("Generate Site Poster", key="generate_site_induction_poster", use_container_width=False):
        try:
            poster = generate_site_induction_poster(
                site_name=project_setup.current_site_name,
                logo_path=UPLANDS_LOGO if UPLANDS_LOGO.exists() else None,
            )
        except RuntimeError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.error(f"Unable to generate the site poster: {exc}")
        else:
            st.session_state["site_induction_poster_png"] = poster.poster_png
            st.session_state["site_induction_qr_png"] = poster.qr_code_png
            st.session_state["site_induction_qr_url"] = poster.induction_url

    poster_png = st.session_state.get("site_induction_poster_png")
    qr_png = st.session_state.get("site_induction_qr_png")
    qr_url = st.session_state.get("site_induction_qr_url")
    if poster_png and qr_png and qr_url:
        poster_columns = st.columns([0.52, 0.48], gap="large")
        with poster_columns[0]:
            st.markdown(
                (
                    "<div class='panel-card'>"
                    "<div class='panel-heading'>Poster Preview</div>"
                    "<div class='panel-title'>SCAN TO SIGN IN</div>"
                    "<div class='panel-caption'>"
                    "1. Scan QR Code  2. Complete Induction  3. Sign on Screen"
                    "</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            if UPLANDS_LOGO.exists():
                st.image(str(UPLANDS_LOGO), width=220)
            st.image(qr_png, width=320)
            st.caption(qr_url)
            st.download_button(
                "Download Poster",
                data=poster_png,
                file_name=(
                    "Site_Induction_Poster_"
                    f"{re.sub(r'[^A-Za-z0-9]+', '_', project_setup.current_site_name).strip('_') or 'Site'}"
                    ".png"
                ),
                mime="image/png",
                use_container_width=True,
                key="download_site_induction_poster",
            )
        with poster_columns[1]:
            st.image(poster_png, caption="Printable site sign-in poster", use_container_width=True)

    detail_columns = st.columns(2, gap="large")
    with detail_columns[0]:
        full_name = st.text_input("Full Name", key="site_induction_full_name")
        home_address = st.text_input("Home Address", key="site_induction_home_address")
        contact_number = st.text_input(
            "Contact Number",
            key="site_induction_contact_number",
        )
        company = st.text_input("Company", key="site_induction_company")
        occupation = st.text_input("Occupation", key="site_induction_occupation")
    with detail_columns[1]:
        emergency_contact = st.text_input(
            "Emergency Contact",
            key="site_induction_emergency_contact",
        )
        emergency_tel = st.text_input(
            "Emergency Tel",
            key="site_induction_emergency_tel",
        )
        medical = st.text_input("Medical", key="site_induction_medical")
        cscs_number = st.text_input("CSCS No.", key="site_induction_cscs_number")
        role_columns = st.columns(2)
        with role_columns[0]:
            first_aider = st.checkbox("First Aider", key="site_induction_first_aider")
            fire_warden = st.checkbox("Fire Warden", key="site_induction_fire_warden")
        with role_columns[1]:
            supervisor = st.checkbox("Supervisor", key="site_induction_supervisor")
            smsts = st.checkbox("SMSTS", key="site_induction_smsts")

    st.markdown(
        "<div class='file-2-section-heading'>Operative Signature</div>",
        unsafe_allow_html=True,
    )
    canvas_revision = int(st.session_state.get("site_induction_canvas_revision", 0))
    canvas_result = st_canvas(
        update_streamlit=True,
        key=f"site_induction_canvas_{canvas_revision}",
        height=200,
        width=420,
        stroke_width=3,
        stroke_color="#000000",
        background_color="#ffffff",
        drawing_mode="freedraw",
        display_toolbar=False,
    )

    submit_columns = st.columns([0.65, 0.35], gap="large")
    with submit_columns[0]:
        if st.button("Submit Induction", use_container_width=True):
            try:
                generated_document = create_site_induction_document(
                    repository,
                    site_name=project_setup.current_site_name,
                    full_name=full_name,
                    home_address=home_address,
                    contact_number=contact_number,
                    company=company,
                    occupation=occupation,
                    emergency_contact=emergency_contact,
                    emergency_tel=emergency_tel,
                    medical=medical,
                    cscs_number=cscs_number,
                    first_aider=first_aider,
                    fire_warden=fire_warden,
                    supervisor=supervisor,
                    smsts=smsts,
                    signature_image_data=canvas_result.image_data,
                )
            except ValidationError as exc:
                st.error(str(exc))
            except TemplateValidationError as exc:
                st.error(f"Official induction template failed validation: {exc}")
            except FileNotFoundError as exc:
                st.error(f"Induction template not found: {exc}")
            except Exception as exc:
                st.error(f"Unable to complete induction: {exc}")
            else:
                st.session_state["site_induction_reset_pending"] = True
                st.session_state["site_induction_canvas_revision"] = canvas_revision + 1
                st.session_state["site_induction_flash"] = (
                    "Induction Complete. Welcome to site, "
                    f"{generated_document.induction_document.individual_name}!"
                )
                st.rerun()
    with submit_columns[1]:
        st.caption(f"Site: {project_setup.current_site_name}")
        st.caption("Template: templates/UHSF16.01_Template.docx")
        st.caption(f"Signatures: {FILE_3_SIGNATURES_DIR.name}")
        st.caption(f"Completed docs: {FILE_3_COMPLETED_INDUCTIONS_DIR.name}")

    if inductions:
        st.markdown(
            (
                "<div class='panel-card'>"
                "<div class='panel-heading'>Recent Submissions</div>"
                "<div class='panel-title'>Completed Inductions</div>"
                "<div class='panel-caption'>"
                "Latest induction records saved to SQLite for the active site."
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        _render_site_induction_recent_submissions(repository, inductions)
    else:
        st.info("No inductions have been logged for this site yet.")


def _render_file_4_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render File 4: Permits & Temp Works."""

    roster = build_site_worker_roster(site_name=project_setup.current_site_name)
    worker_options = _build_file_4_worker_options(
        repository,
        site_name=project_setup.current_site_name,
    )
    ladder_permits = [
        permit
        for permit in _filter_for_lovedean(
            repository.list_documents(document_type=LadderPermit.document_type),
            site_name=project_setup.current_site_name,
        )
        if isinstance(permit, LadderPermit)
    ]
    todays_permits = [
        permit for permit in ladder_permits if permit.created_at.date() == date.today()
    ]
    draft_permits = [
        permit for permit in ladder_permits if permit.status == DocumentStatus.DRAFT
    ]
    indexed_permits = repository.list_indexed_files(
        file_group=LadderPermit.file_group,
        file_category="ladder_permit_docx",
    )
    permit_register_rows = _build_live_permit_register_rows(ladder_permits)

    cards = st.columns(4)
    with cards[0]:
        _render_metric_card(
            title="Ladder Permits",
            icon="🪜",
            value=str(len(ladder_permits)),
            caption="Ladder permits stored inside File 4.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Drafts: <strong>{len(draft_permits)}</strong>"
                "</div>"
            ),
        )
    with cards[1]:
        _render_metric_card(
            title="Roster Workers",
            icon="⚡",
            value=str(len(worker_options)),
            caption="Workers available to the permit helper from the live JSON roster.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Issued today: <strong>{len(todays_permits)}</strong>"
                "</div>"
            ),
        )
    with cards[2]:
        _render_metric_card(
            title="Permit Files",
            icon="📄",
            value=str(len(indexed_permits)),
            caption="Generated permit documents indexed in File 4.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Output folder: <strong>{PERMITS_DESTINATION.name}</strong>"
                "</div>"
            ),
        )
    with cards[3]:
        _render_metric_card(
            title="Permit Gate",
            icon="🔐",
            value="READY" if worker_options else "NO DATA",
            caption="Permit helper is driven by the live contractor roster feed.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Roster workers: <strong>{len(roster)}</strong>"
                "</div>"
            ),
        )

    columns = st.columns(2)
    with columns[0]:
        _render_file_list_panel(
            heading="Permit Control",
            title="UHSF21.09 Helper",
            caption="Use the File 4 quick action in the sidebar to issue a ladder permit against the live contractor roster.",
            items=[
                "Worker is selected from the live JSON-driven contractor roster.",
                "Company is auto-filled from the selected roster entry.",
                "The official registered template is used as the only output source.",
            ],
            empty_message="No permit controls are configured.",
        )
    with columns[1]:
        _render_file_list_panel(
            heading="Latest Permits",
            title="Recent File 4 Output",
            caption="Most recent ladder permits stored for printing and signature.",
            items=[
                (
                    f"{permit.permit_number} | {permit.worker_name or '-'} | "
                    f"{permit.location_of_work} | {permit.created_at.strftime('%Y-%m-%d %H:%M')}"
                )
                for permit in sorted(
                    ladder_permits,
                    key=lambda item: item.created_at,
                    reverse=True,
                )[:5]
            ],
            empty_message="No ladder permits have been generated yet.",
        )

    st.markdown(
        "<div class='file-2-section-heading'>Live Permit Register</div>",
        unsafe_allow_html=True,
    )
    action_columns = st.columns([1.1, 2.4])
    with action_columns[0]:
        if st.button(
            "🖨️ Print Physical Register",
            key="print_physical_register",
            use_container_width=True,
        ):
            try:
                generated_register = generate_permit_register_document(
                    repository,
                    site_name=project_setup.current_site_name,
                    job_number=project_setup.job_number,
                )
            except TemplateValidationError as exc:
                st.error(f"Permit register template failed validation: {exc}")
            except Exception as exc:
                st.error(f"Unable to generate the physical permit register: {exc}")
            else:
                _open_file_for_printing(generated_register.output_path)
                st.success(
                    "Physical permit register generated: "
                    f"{generated_register.output_path}"
                )
                st.toast(
                    f"Permit register ready: {generated_register.output_path.name}",
                    icon="🖨️",
                )
    with action_columns[1]:
        st.caption(
            "Live File 4 permit register for the active project. "
            "This table drives the printable UHSF21.00 register."
        )

    st.dataframe(
        pd.DataFrame(
            permit_register_rows,
            columns=[
                "Permit Number",
                "Date Issued",
                "Worker Name",
                "Company",
                "Job Number",
                "Location",
            ],
        ),
        use_container_width=True,
        hide_index=True,
    )


def _render_metric_card(
    *,
    title: str,
    icon: str,
    value: str,
    caption: str,
    body_html: str,
) -> None:
    """Render a branded HTML metric card."""

    st.markdown(
        (
            "<div class='data-card'>"
            f"<div class='data-card-icon'>{icon}</div>"
            f"<div class='data-card-title'>{title}</div>"
            f"<div class='data-card-value'>{value}</div>"
            f"<div class='data-card-caption'>{caption}</div>"
            f"{body_html}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_file_list_panel(
    *,
    heading: str,
    title: str,
    caption: str,
    items: List[str],
    empty_message: str,
) -> None:
    """Render a white list panel for a file station."""

    if items:
        items_html = "".join(
            f"<div class='file-list-item'>{item}</div>" for item in items
        )
    else:
        items_html = f"<div class='file-list-empty'>{empty_message}</div>"

    st.markdown(
        (
            "<div class='file-list-panel'>"
            f"<div class='file-list-heading'>{heading}</div>"
            f"<div class='file-list-title'>{title}</div>"
            f"<div class='file-list-caption'>{caption}</div>"
            f"{items_html}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_site_worker_roster_table(site_workers: List[SiteWorker]) -> None:
    """Render the live File 3 contractor roster with interactive filters."""

    if not site_workers:
        st.markdown(
            (
                "<div class='roster-table-wrapper'>"
                "<div class='file-list-heading'>Contractor Roster</div>"
                "<div class='file-list-title'>Live KPI Feed</div>"
                "<div class='file-list-caption'>"
                "No site-kpi-backup JSON data is available for this project yet."
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        return

    st.markdown(
        (
            "<div class='roster-table-wrapper'>"
            "<div class='file-list-heading'>Contractor Roster</div>"
            "<div class='file-list-title'>Live Site Workers</div>"
            "<div class='file-list-caption'>"
            "Generated from site-kpi-backup JSON files instead of manual induction PDF scans."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    filter_columns = st.columns([1.4, 1.6])
    with filter_columns[0]:
        worker_search = st.text_input(
            "Search Worker Name",
            key="file3_roster_worker_search",
            placeholder="Type a worker name",
        ).strip()
    with filter_columns[1]:
        company_options = sorted({worker.company for worker in site_workers}, key=str.casefold)
        selected_companies = st.multiselect(
            "Filter by Company",
            options=company_options,
            default=[],
            key="file3_roster_company_filter",
        )

    filtered_workers = [
        worker
        for worker in site_workers
        if (
            not worker_search
            or worker_search.casefold() in worker.worker_name.casefold()
        )
        and (
            not selected_companies
            or worker.company in selected_companies
        )
    ]

    st.caption(
        f"Showing {len(filtered_workers)} of {len(site_workers)} roster entries."
    )
    dataframe_rows = [
        {
            "Company": worker.company,
            "Name": worker.worker_name,
            "Last On Site Date": worker.last_on_site_date.strftime("%d/%m/%Y"),
            "Induction Status": worker.induction_status,
        }
        for worker in filtered_workers
    ]
    st.dataframe(
        pd.DataFrame(dataframe_rows),
        use_container_width=True,
        hide_index=True,
    )


def _render_file_2_site_checks_panel(
    repository: DocumentRepository,
    *,
    site_name: str,
    latest_site_check: Optional[WeeklySiteCheck],
) -> None:
    """Render the File 2 weekly template grid and latest result."""

    st.markdown(
        "<div class='file-2-section-heading'>Daily/Weekly Site Checks</div>",
        unsafe_allow_html=True,
    )
    flash_message = st.session_state.pop("site_check_flash", None)
    if flash_message is not None:
        if flash_message["level"] == "warning":
            st.warning(flash_message["message"])
        else:
            st.success(flash_message["message"])

    if latest_site_check is not None:
        latest_summary = (
            f"Latest submission: {latest_site_check.checked_at.strftime('%Y-%m-%d %H:%M')} "
            f"by {latest_site_check.checked_by} "
            f"({SITE_CHECK_WEEKDAY_LABELS[latest_site_check.active_day_key]})"
        )
        st.caption(latest_summary)
    try:
        row_definitions = get_weekly_site_check_row_definitions()
        valid_template_tags = set(get_valid_template_tags())
    except Exception as exc:
        st.error(f"Unable to load the UHSF19.1 checklist template structure: {exc}")
        return

    selected_week_commencing = _current_week_commencing(
        st.session_state.get(
            "weekly_site_check_week_commencing",
            _current_week_commencing(),
        )
    )
    weekly_site_check = _get_weekly_site_check_for_week(
        repository,
        site_name=site_name,
        week_commencing=selected_week_commencing,
    )
    namespace = _weekly_site_check_namespace(site_name, selected_week_commencing)
    _ensure_weekly_site_check_editor_state(
        namespace=namespace,
        weekly_site_check=weekly_site_check,
        row_definitions=row_definitions,
    )
    checked_by_key = _weekly_site_check_state_key(namespace, kind="checked-by")
    active_day_key_key = _weekly_site_check_state_key(namespace, kind="active-day")

    st.caption(f"Template output folder: {FILE_2_CHECKLIST_OUTPUT_DIR}")

    metadata_columns = st.columns(3)
    with metadata_columns[0]:
        checked_by = st.text_input(
            "Checked By",
            key=checked_by_key,
        )
    with metadata_columns[1]:
        week_commencing = st.date_input(
            "Week Commencing",
            value=selected_week_commencing,
            format="DD/MM/YYYY",
            key="weekly_site_check_week_commencing",
        )
    with metadata_columns[2]:
        active_day_key = st.selectbox(
            "Active Day",
            options=list(SITE_CHECK_WEEKDAY_KEYS),
            key=active_day_key_key,
            format_func=lambda day_key: SITE_CHECK_WEEKDAY_LABELS[day_key],
        )

    active_day_label = SITE_CHECK_WEEKDAY_LABELS[active_day_key]
    st.caption(
        "Bulk update: stamp the whole active day or weekly column before fine-tuning individual rows."
    )
    bulk_columns = st.columns(6)
    bulk_actions = [
        (
            bulk_columns[0],
            f"{active_day_label} all ✔",
            active_day_key,
            True,
            f"weekly-site-check-bulk-{namespace}-{active_day_key}-tick",
        ),
        (
            bulk_columns[1],
            f"{active_day_label} all ✘",
            active_day_key,
            False,
            f"weekly-site-check-bulk-{namespace}-{active_day_key}-cross",
        ),
        (
            bulk_columns[2],
            f"Clear {active_day_label}",
            active_day_key,
            None,
            f"weekly-site-check-bulk-{namespace}-{active_day_key}-clear",
        ),
        (
            bulk_columns[3],
            "Weekly all ✔",
            "weekly",
            True,
            f"weekly-site-check-bulk-{namespace}-weekly-tick",
        ),
        (
            bulk_columns[4],
            "Weekly all ✘",
            "weekly",
            False,
            f"weekly-site-check-bulk-{namespace}-weekly-cross",
        ),
        (
            bulk_columns[5],
            "Clear Weekly",
            "weekly",
            None,
            f"weekly-site-check-bulk-{namespace}-weekly-clear",
        ),
    ]
    for column, label, day_key, value, key in bulk_actions:
        with column:
            if st.button(label, key=key, use_container_width=True):
                _set_weekly_site_check_column_value(
                    namespace=namespace,
                    row_definitions=row_definitions,
                    day_key=day_key,
                    value=value,
                    valid_template_tags=valid_template_tags,
                )
                st.rerun()

    header_columns = st.columns([2.2, 5.8] + [0.9] * 8)
    header_columns[0].markdown("**Section**")
    header_columns[1].markdown("**Checklist Item**")
    for column, day_key in zip(
        header_columns[2:],
        list(SITE_CHECK_WEEKDAY_KEYS) + ["weekly"],
    ):
        column.markdown(
            f"**{SITE_CHECK_WEEKDAY_LABELS.get(day_key, day_key.title())}**"
        )

    edited_values: Dict[int, Dict[str, Optional[bool]]] = {}
    for row_definition in row_definitions:
        row_columns = st.columns([2.2, 5.8] + [0.9] * 8)
        row_columns[0].markdown(
            f"<div class='weekly-grid-section'>{row_definition.section}</div>",
            unsafe_allow_html=True,
        )
        row_columns[1].markdown(
            f"<div class='weekly-grid-prompt'>{row_definition.prompt}</div>",
            unsafe_allow_html=True,
        )
        edited_values[row_definition.row_number] = {}
        for column_offset, day_key in enumerate(
            list(SITE_CHECK_WEEKDAY_KEYS) + ["weekly"],
            start=2,
        ):
            template_tag = _weekly_site_check_template_tag(
                day_key,
                row_definition.row_number,
            )
            cell_state_key = _weekly_site_check_state_key(
                namespace,
                kind="cell",
                row_number=row_definition.row_number,
                day_key=day_key,
            )
            current_value = st.session_state.get(cell_state_key)
            if template_tag not in valid_template_tags:
                edited_values[row_definition.row_number][day_key] = None
                row_columns[column_offset].markdown(
                    (
                        "<div class='weekly-grid-cell' "
                        "style='background: #e2e8f0; border: none;'>&nbsp;</div>"
                    ),
                    unsafe_allow_html=True,
                )
            elif day_key in {active_day_key, "weekly"}:
                edited_values[row_definition.row_number][day_key] = current_value
                button_label = _weekly_site_check_status_label(current_value) or "·"
                clicked = row_columns[column_offset].button(
                    button_label,
                    key=f"weekly-site-check-button-{namespace}-{row_definition.row_number}-{day_key}",
                    use_container_width=True,
                    help="Click to cycle blank, tick, and cross.",
                )
                if clicked:
                    st.session_state[cell_state_key] = _cycle_weekly_site_check_value(
                        current_value
                    )
                    st.rerun()
            else:
                edited_values[row_definition.row_number][day_key] = current_value
                row_columns[column_offset].markdown(
                    (
                        "<div class='weekly-grid-cell'>"
                        f"{_weekly_site_check_status_label(current_value) or '&nbsp;'}"
                        "</div>"
                    ),
                    unsafe_allow_html=True,
                )

    st.markdown(
        "<div class='file-2-section-heading'>Daily Sign-Off</div>",
        unsafe_allow_html=True,
    )
    initials_key = _weekly_site_check_state_key(
        namespace,
        kind="initials",
        day_key=active_day_key,
    )
    time_key = _weekly_site_check_state_key(
        namespace,
        kind="time",
        day_key=active_day_key,
    )
    signoff_columns = st.columns(2)
    with signoff_columns[0]:
        initials_value = st.text_input(
            f"Initials ({SITE_CHECK_WEEKDAY_LABELS[active_day_key]})",
            key=initials_key,
        )
    with signoff_columns[1]:
        time_marker_value = st.selectbox(
            f"AM/PM ({SITE_CHECK_WEEKDAY_LABELS[active_day_key]})",
            options=["", "AM", "PM"],
            key=time_key,
        )

    action_columns = st.columns(2)
    with action_columns[0]:
        save_submitted = st.button(
            "Submit Check",
            key=f"weekly-site-check-save-{namespace}",
            use_container_width=True,
        )
    with action_columns[1]:
        generate_submitted = st.button(
            "Generate Printable Checklist",
            key=f"weekly-site-check-generate-{namespace}",
            use_container_width=True,
        )

    if save_submitted or generate_submitted:
        daily_initials_map = {
            day_key: str(
                st.session_state.get(
                    _weekly_site_check_state_key(
                        namespace,
                        kind="initials",
                        day_key=day_key,
                    ),
                    "",
                )
            ).strip()
            for day_key in SITE_CHECK_WEEKDAY_KEYS
        }
        daily_time_markers_map = {
            day_key: str(
                st.session_state.get(
                    _weekly_site_check_state_key(
                        namespace,
                        kind="time",
                        day_key=day_key,
                    ),
                    "",
                )
            ).strip()
            for day_key in SITE_CHECK_WEEKDAY_KEYS
        }
        saved_check = _save_weekly_site_check(
            repository,
            site_name=site_name,
            week_commencing=week_commencing,
            checked_by=checked_by,
            active_day_key=active_day_key,
            grid_values=edited_values,
            valid_template_tags=valid_template_tags,
            daily_initials_map=daily_initials_map,
            daily_time_markers_map=daily_time_markers_map,
        )

        if generate_submitted:
            try:
                generated_checklist = create_weekly_site_check_checklist_draft(
                    repository,
                    weekly_site_check=saved_check,
                )
            except TemplateValidationError as exc:
                st.session_state["site_check_flash"] = {
                    "level": "warning",
                    "message": f"Official File 2 template failed validation: {exc}",
                }
                st.rerun()
            except Exception as exc:
                st.session_state["site_check_flash"] = {
                    "level": "warning",
                    "message": f"Unable to generate the printable checklist: {exc}",
                }
                st.rerun()

            _open_file_for_printing(generated_checklist.output_path)
            st.session_state["site_check_flash"] = {
                "level": "success",
                "message": (
                    "Checklist saved and generated in "
                    f"{FILE_2_CHECKLIST_OUTPUT_DIR.name} for week commencing "
                    f"{saved_check.week_commencing.strftime('%d/%m/%Y')}."
                ),
            }
            st.toast(
                f"Printable checklist ready: {generated_checklist.output_path.name}",
                icon="📝",
            )
            st.rerun()

        st.session_state["site_check_flash"] = {
            "level": "success" if saved_check.overall_safe_to_start else "warning",
            "message": (
                "Weekly site check saved."
                if saved_check.overall_safe_to_start
                else "Weekly site check saved, but one or more checklist items are marked ✘ or left blank."
            ),
        }
        st.rerun()

    _render_file_list_panel(
        heading="Morning Station",
        title="Latest Site Check Sheet",
        caption="The most recent tick sheet saved to SQLite for File 2.",
        items=_weekly_site_check_items(latest_site_check),
        empty_message="No daily/weekly site checks have been submitted yet.",
    )


def _get_file_2_plant_assets(
    repository: DocumentRepository,
    *,
    site_name: str,
) -> List[PlantAssetDocument]:
    """Return the live File 2 plant assets for the active site."""

    plant_assets = [
        document
        for document in repository.list_documents(
            document_type=PlantAssetDocument.document_type,
            site_name=site_name,
        )
        if isinstance(document, PlantAssetDocument)
        and document.status != DocumentStatus.ARCHIVED
    ]
    return sorted(
        plant_assets,
        key=lambda asset: (
            int(re.search(r"(\d+)$", asset.hire_num).group(1))
            if re.search(r"(\d+)$", asset.hire_num)
            else 0,
            asset.on_hire,
            asset.description.casefold(),
        ),
    )


def _plant_asset_status_label(asset: PlantAssetDocument) -> str:
    """Return a UI label for one plant asset record."""

    return "Pending" if asset.is_pending else asset.status.label


def _plant_asset_inspection_alert_label(asset: PlantAssetDocument) -> str:
    """Return the inspection attention label for one plant asset."""

    due_date = asset.inspection_due_date()
    if due_date is None:
        return ""
    return "CRITICAL" if asset.inspection_requires_attention() else "OK"


def _render_file_2_plant_register_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
) -> None:
    """Render the live File 2 plant register panel and print action."""

    st.markdown(
        "<div class='file-2-section-heading'>Site Plant Register</div>",
        unsafe_allow_html=True,
    )
    flash_message = st.session_state.pop("plant_register_flash", None)
    if flash_message is not None:
        if flash_message["level"] == "error":
            st.error(flash_message["message"])
        else:
            st.success(flash_message["message"])

    plant_assets = _get_file_2_plant_assets(
        repository,
        site_name=project_setup.current_site_name,
    )
    pending_assets = sum(1 for asset in plant_assets if asset.is_pending)

    action_columns = st.columns(3)
    with action_columns[0]:
        if st.button(
            "🖨️ Print Plant Register",
            key="file_2_print_plant_register",
            use_container_width=True,
        ):
            try:
                generated_document = generate_plant_register_document(
                    repository,
                    site_name=project_setup.current_site_name,
                )
            except TemplateValidationError as exc:
                st.session_state["plant_register_flash"] = {
                    "level": "error",
                    "message": f"Official plant register template failed validation: {exc}",
                }
                st.rerun()
            except Exception as exc:
                st.session_state["plant_register_flash"] = {
                    "level": "error",
                    "message": f"Unable to generate the plant register: {exc}",
                }
                st.rerun()

            _open_file_for_printing(generated_document.output_path)
            st.session_state["plant_register_flash"] = {
                "level": "success",
                "message": (
                    f"Plant register saved to {generated_document.output_path.name} "
                    f"with {generated_document.asset_count} live asset row"
                    f"{'' if generated_document.asset_count == 1 else 's'}."
                ),
            }
            st.rerun()
    with action_columns[1]:
        if st.button(
            "Open Plant Folder",
            key="file_2_open_plant_folder",
            use_container_width=True,
        ):
            PLANT_HIRE_REGISTER_DIR.mkdir(parents=True, exist_ok=True)
            _open_workspace_path(PLANT_HIRE_REGISTER_DIR)
            st.toast("Opened Plant_Hire_Register", icon="📂")
    with action_columns[2]:
        st.metric("Pending Assets", str(pending_assets))

    if not plant_assets:
        st.info("SYNC WORKSPACE to file HSS/MEP plant hire paperwork into File 2.")
        return

    register_rows = []
    for plant_asset in plant_assets:
        register_rows.append(
            {
                "Hire Number": plant_asset.hire_num,
                "Description": plant_asset.description,
                "Company": plant_asset.company,
                "Phone": plant_asset.phone,
                "On Hire": plant_asset.on_hire.strftime("%d/%m/%Y"),
                "Hired By": plant_asset.hired_by,
                "Serial": plant_asset.serial or "Pending",
                "Inspection": plant_asset.inspection or "Pending",
                "Inspection Status": _plant_asset_inspection_alert_label(plant_asset),
                "Status": _plant_asset_status_label(plant_asset),
            }
        )

    dataframe = pd.DataFrame(register_rows)

    def _plant_register_row_styles(row: pd.Series) -> List[str]:
        styles = [""] * len(row)
        if row["Inspection Status"] == "CRITICAL":
            inspection_index = row.index.get_loc("Inspection")
            styles[inspection_index] = (
                "background-color: #FEE2E2; color: #991B1B; font-weight: 700;"
            )
        return styles

    styled_dataframe = (
        dataframe.style.apply(_plant_register_row_styles, axis=1)
        .hide(axis="index")
    )
    st.dataframe(styled_dataframe, use_container_width=True, hide_index=True)

    with st.expander("Update Plant Asset", expanded=False):
        asset_options = {
            f"{asset.hire_num} | {asset.description}": asset for asset in plant_assets
        }
        selected_asset_label = st.selectbox(
            "Plant Asset",
            options=list(asset_options),
            key="file_2_selected_plant_asset",
        )
        selected_asset = asset_options[selected_asset_label]
        with st.form("file_2_plant_asset_update_form", clear_on_submit=False):
            serial_value = st.text_input(
                "Serial Number",
                value=selected_asset.serial,
            )
            inspection_value = st.text_input(
                "LOLER / Inspection",
                value=selected_asset.inspection,
            )
            status_value = st.selectbox(
                "Record Status",
                options=[DocumentStatus.DRAFT, DocumentStatus.ACTIVE],
                index=[DocumentStatus.DRAFT, DocumentStatus.ACTIVE].index(
                    selected_asset.status
                    if selected_asset.status in {DocumentStatus.DRAFT, DocumentStatus.ACTIVE}
                    else DocumentStatus.DRAFT
                ),
                format_func=lambda item: item.label,
            )
            submitted = st.form_submit_button(
                "Save Plant Asset",
                use_container_width=True,
            )

        if submitted:
            resolved_status = (
                DocumentStatus.ACTIVE
                if serial_value.strip() and status_value == DocumentStatus.DRAFT
                else status_value
            )
            repository.save(
                PlantAssetDocument(
                    doc_id=selected_asset.doc_id,
                    site_name=selected_asset.site_name,
                    created_at=selected_asset.created_at,
                    status=resolved_status,
                    hire_num=selected_asset.hire_num,
                    description=selected_asset.description,
                    company=selected_asset.company,
                    phone=selected_asset.phone,
                    on_hire=selected_asset.on_hire,
                    hired_by=selected_asset.hired_by,
                    serial=serial_value,
                    inspection=inspection_value,
                    source_reference=selected_asset.source_reference,
                    purchase_order=selected_asset.purchase_order,
                )
            )
            st.session_state["plant_register_flash"] = {
                "level": "success",
                "message": f"Updated {selected_asset.hire_num}.",
            }
            st.rerun()


def _render_file_2_register_link(
    *,
    title: str,
    caption: str,
    destination: Path,
    button_label: str,
) -> None:
    """Render one quick-access card for a File 2 register folder."""

    with st.expander(title, expanded=True):
        st.caption(caption)
        destination.mkdir(parents=True, exist_ok=True)
        if st.button(button_label, key=f"open-{destination.name}", use_container_width=True):
            _open_workspace_path(destination)
            st.toast(f"Opened {destination.name}", icon="📂")
        st.caption(destination.name)


def _render_carrier_compliance_panel(rows: List[AbucsStatusRow]) -> None:
    """Render the high-visibility carrier compliance panel."""

    status_label = "OK" if rows and all(
        row.status == ComplianceAlertStatus.OK for row in rows
    ) else "CRITICAL"
    status_class = (
        "audit-status-ok" if status_label == "OK" else "audit-status-critical"
    )
    detail_html = "".join(
        (
            "<div class='audit-detail-row'>"
            "<div class='audit-detail-top'>"
            f"<span class='audit-detail-title'>{row.label}</span>"
            f"<span class='audit-detail-status {'audit-status-ok' if row.status == ComplianceAlertStatus.OK else 'audit-status-critical'}'>"
            f"{row.status.value}"
            "</span>"
            "</div>"
            f"<div class='audit-detail-reason'>{row.reason}</div>"
            "</div>"
        )
        for row in rows
    )
    _render_audit_panel(
        title="Carrier Compliance",
        status_label=status_label,
        status_class=status_class,
        caption="Abucs licence and insurance gatekeeper status.",
        body_html=detail_html or "<div class='audit-detail-reason'>No carrier data available.</div>",
    )


def _render_site_induction_panel(audit: SiteInductionAuditResult) -> None:
    """Render the high-visibility induction audit panel."""

    if not audit.workers_on_site:
        status_label = "NO DATA"
        status_class = "audit-status-neutral"
    elif audit.is_compliant:
        status_label = "OK"
        status_class = "audit-status-ok"
    else:
        status_label = "CRITICAL"
        status_class = "audit-status-critical"

    if not audit.workers_on_site:
        body_html = (
            "<div class='audit-detail-reason'>"
            "No workers found on today's attendance register."
            "</div>"
        )
    elif audit.is_compliant:
        body_html = (
            "<div class='audit-summary-line'>"
            f"All {len(audit.workers_on_site)} worker(s) on site have induction records."
            "</div>"
        )
    else:
        body_html = (
            "<div class='audit-summary-line'>Missing induction records:</div>"
            + "".join(
                f"<span class='missing-worker-name'>{worker_name}</span>"
                for worker_name in audit.missing_workers
            )
        )

    _render_audit_panel(
        title="Site Induction Audit",
        status_label=status_label,
        status_class=status_class,
        caption=_site_induction_audit_caption(audit),
        body_html=body_html,
    )


def _render_audit_panel(
    *,
    title: str,
    status_label: str,
    status_class: str,
    caption: str,
    body_html: str,
) -> None:
    """Render a large audit panel with a high-visibility status line."""

    st.markdown(
        (
            "<div class='audit-panel'>"
            f"<div class='audit-panel-heading'>{title}</div>"
            f"<div class='audit-panel-status {status_class}'>{status_label}</div>"
            f"<div class='audit-panel-caption'>{caption}</div>"
            f"{body_html}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _site_induction_audit_caption(audit: SiteInductionAuditResult) -> str:
    """Return the summary caption for the induction audit card."""

    if not audit.workers_on_site:
        return f"No workers found on the attendance register for {audit.audit_date.isoformat()}."
    if audit.is_compliant:
        return (
            f"All {len(audit.workers_on_site)} worker(s) on today's attendance "
            "sheet have matching induction PDFs."
        )
    return (
        f"{len(audit.missing_workers)} of {len(audit.workers_on_site)} worker(s) "
        "on site are missing induction records."
    )


def _get_latest_weekly_site_check(
    repository: DocumentRepository,
    *,
    site_name: Optional[str] = None,
) -> Optional[WeeklySiteCheck]:
    """Return the latest File 2 weekly site-check submission for the active site."""

    site_checks = [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=WeeklySiteCheck.document_type),
            site_name=site_name,
        )
        if isinstance(document, WeeklySiteCheck)
    ]
    if not site_checks:
        return None
    return max(site_checks, key=lambda document: document.checked_at)


def _get_weekly_site_check_for_week(
    repository: DocumentRepository,
    *,
    site_name: str,
    week_commencing: date,
) -> Optional[WeeklySiteCheck]:
    """Return the latest File 2 weekly site-check record for one calendar week."""

    matching_site_checks = [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=WeeklySiteCheck.document_type),
            site_name=site_name,
        )
        if isinstance(document, WeeklySiteCheck)
        and document.week_commencing == _current_week_commencing(week_commencing)
    ]
    if not matching_site_checks:
        return None
    return max(matching_site_checks, key=lambda document: document.checked_at)


def _save_weekly_site_check(
    repository: DocumentRepository,
    *,
    site_name: str,
    week_commencing: date,
    checked_by: str,
    active_day_key: str,
    grid_values: Dict[int, Dict[str, Optional[bool]]],
    valid_template_tags: set[str],
    daily_initials_map: Dict[str, str],
    daily_time_markers_map: Dict[str, str],
) -> WeeklySiteCheck:
    """Persist the cumulative weekly File 2 checklist for one calendar week."""

    checked_at = datetime.now()
    existing_check = _get_weekly_site_check_for_week(
        repository,
        site_name=site_name,
        week_commencing=week_commencing,
    )
    saved_check = (
        existing_check
        if existing_check is not None
        else WeeklySiteCheck(
            doc_id=f"WSC-{_current_week_commencing(week_commencing):%Y%m%d}",
            site_name=site_name,
            created_at=checked_at,
            status=DocumentStatus.ACTIVE,
            week_commencing=week_commencing,
            checked_at=checked_at,
            checked_by=checked_by,
            active_day_key=active_day_key,
            row_states=[],
            daily_initials={},
            daily_time_markers={},
            overall_safe_to_start=False,
        )
    )

    saved_check.created_at = (
        saved_check.created_at if existing_check is not None else checked_at
    )
    saved_check.week_commencing = _current_week_commencing(week_commencing)
    saved_check.checked_at = checked_at
    saved_check.checked_by = checked_by
    saved_check.active_day_key = active_day_key
    saved_check.daily_initials = {
        day_key: value.strip()
        for day_key, value in daily_initials_map.items()
    }
    saved_check.daily_time_markers = {
        day_key: value.strip()
        for day_key, value in daily_time_markers_map.items()
    }

    for row_definition in get_weekly_site_check_row_definitions():
        row_state = saved_check.get_row_state(row_definition.row_number)
        for day_key in list(SITE_CHECK_WEEKDAY_KEYS) + ["weekly"]:
            if (
                _weekly_site_check_template_tag(day_key, row_definition.row_number)
                not in valid_template_tags
            ):
                row_state.set_value(day_key, None)
                continue
            row_state.set_value(
                day_key,
                grid_values[row_definition.row_number][day_key],
            )

    active_day_values = [
        saved_check.get_row_state(row_definition.row_number).get_value(active_day_key)
        for row_definition in get_weekly_site_check_row_definitions()
        if _weekly_site_check_template_tag(active_day_key, row_definition.row_number)
        in valid_template_tags
    ]
    saved_check.overall_safe_to_start = bool(active_day_values) and all(
        value is True for value in active_day_values
    )
    repository.save(saved_check)
    return saved_check


def _weekly_site_check_items(
    weekly_site_check: Optional[WeeklySiteCheck],
) -> List[str]:
    """Return display lines for the most recent weekly site-check sheet."""

    if weekly_site_check is None:
        return []
    row_definitions = get_weekly_site_check_row_definitions()
    row_lookup = {
        row_state.row_number: row_state for row_state in weekly_site_check.row_states
    }
    items = [
        (
            f"{row_definition.row_number}. {row_definition.section} | "
            f"{row_definition.prompt} | "
            + " ".join(
                f"{label} "
                f"{_weekly_site_check_status_label(row_lookup[row_definition.row_number].get_value(day_key)) or '-'}"
                for day_key, label in (
                    ("mon", "Mon"),
                    ("tue", "Tue"),
                    ("wed", "Wed"),
                    ("thu", "Thu"),
                    ("fri", "Fri"),
                    ("sat", "Sat"),
                    ("sun", "Sun"),
                    ("weekly", "Weekly"),
                )
            )
        )
        for row_definition in row_definitions
    ]
    items.insert(
        0,
        (
            f"Week commencing {weekly_site_check.week_commencing.strftime('%d/%m/%Y')} | "
            f"Checked by {weekly_site_check.checked_by} | "
            f"{SITE_CHECK_WEEKDAY_LABELS[weekly_site_check.active_day_key]} "
            f"{weekly_site_check.daily_time_markers.get(weekly_site_check.active_day_key, '') or '-'} | "
            f"Initials {weekly_site_check.daily_initials.get(weekly_site_check.active_day_key, '') or '-'} | "
            f"{'Safe to start' if weekly_site_check.overall_safe_to_start else 'Review required'}"
        ),
    )
    return items


def _format_site_check_timestamp(
    weekly_site_check: Optional[WeeklySiteCheck],
) -> str:
    """Return the latest weekly site-check timestamp for KPI display."""

    if weekly_site_check is None:
        return "No data"
    return (
        f"{weekly_site_check.checked_at.strftime('%Y-%m-%d %H:%M')} | "
        f"{SITE_CHECK_WEEKDAY_LABELS[weekly_site_check.active_day_key]}"
    )


def _weekly_site_check_dashboard_status(
    weekly_site_check: Optional[WeeklySiteCheck],
) -> str:
    """Return the File 2 KPI status label for the latest weekly checklist."""

    if weekly_site_check is None:
        return "PENDING"
    valid_template_tags = set(get_valid_template_tags())
    active_day_values = [
        row_state.get_value(weekly_site_check.active_day_key)
        for row_state in weekly_site_check.row_states
        if _weekly_site_check_template_tag(
            weekly_site_check.active_day_key,
            row_state.row_number,
        )
        in valid_template_tags
    ]
    if not any(value is not None for value in active_day_values):
        return "PENDING"
    if all(value is True for value in active_day_values):
        return "OK"
    return "REVIEW"


def _get_file_station(station_label: str) -> FileStation:
    """Return the configured station metadata for the selected tab label."""

    for station in FILE_STATIONS:
        if station.label == station_label:
            return station
    return FILE_STATIONS[0]


def _build_contractor_folder_rows(
    repository: DocumentRepository,
    attendance_register: Optional[SiteAttendanceRegister],
    *,
    site_name: Optional[str] = None,
) -> List[ContractorFolderRow]:
    """Roll up File 3 contractor coverage into company-folder style rows."""

    workers_by_company: Dict[str, set[str]] = {}
    if attendance_register is not None:
        for record in _get_todays_attendance_records(attendance_register):
            workers_by_company.setdefault(record.company, set()).add(record.workerName)

    rams_documents = [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=RAMSDocument.document_type),
            site_name=site_name,
        )
        if isinstance(document, RAMSDocument)
    ]
    coshh_documents = [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=COSHHDocument.document_type),
            site_name=site_name,
        )
        if isinstance(document, COSHHDocument)
    ]
    induction_documents = [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=InductionDocument.document_type),
            site_name=site_name,
        )
        if isinstance(document, InductionDocument)
    ]

    contractor_names = set(workers_by_company)
    contractor_names.update(document.contractor_name for document in rams_documents)
    contractor_names.update(document.contractor_name for document in coshh_documents)
    contractor_names.update(document.contractor_name for document in induction_documents)

    rows = [
        ContractorFolderRow(
            contractor_name=contractor_name,
            workers_today=len(workers_by_company.get(contractor_name, set())),
            rams_count=sum(
                1
                for document in rams_documents
                if document.contractor_name.casefold() == contractor_name.casefold()
            ),
            coshh_count=sum(
                1
                for document in coshh_documents
                if document.contractor_name.casefold() == contractor_name.casefold()
            ),
            induction_count=sum(
                1
                for document in induction_documents
                if document.contractor_name.casefold() == contractor_name.casefold()
            ),
        )
        for contractor_name in contractor_names
        if contractor_name
    ]
    return sorted(
        rows,
        key=lambda row: (
            row.workers_today,
            row.induction_count + row.rams_count + row.coshh_count,
            row.contractor_name.casefold(),
        ),
        reverse=True,
    )


def _build_file_3_rams_rows(rams_assets: List[SafetyAsset]) -> List[Dict[str, str]]:
    """Return UI-ready RAMS table rows for the File 3 safety inventory."""

    return [
        {
            "Reference": asset.reference,
            "Version": asset.version or "",
            "Review Date": asset.review_date.strftime("%d/%m/%Y")
            if asset.review_date is not None
            else "",
            "Activity": asset.title,
            "Company": asset.company,
            "Manufacturer": asset.manufacturer or "",
            "Status": asset.status,
        }
        for asset in rams_assets
    ]


def _build_file_3_coshh_rows(coshh_assets: List[SafetyAsset]) -> List[Dict[str, str]]:
    """Return UI-ready COSHH table rows for the File 3 safety inventory."""

    return [
        {
            "Reference": asset.reference,
            "Version": asset.version or "",
            "Review Date": asset.review_date.strftime("%d/%m/%Y")
            if asset.review_date is not None
            else "",
            "Substance": asset.title,
            "Supplier / Manufacturer": asset.manufacturer or "",
            "Company": asset.company,
            "Status": asset.status,
        }
        for asset in coshh_assets
    ]


def _get_lovedean_waste_notes(
    repository: DocumentRepository,
    *,
    site_name: Optional[str] = None,
) -> List[WasteTransferNoteDocument]:
    """Return active-site WTNs, preferring direct WTN documents over nested registers."""

    direct_waste_notes = _filter_for_lovedean(
        repository.list_documents(document_type=WasteTransferNoteDocument.document_type),
        site_name=site_name,
    )
    if direct_waste_notes:
        return [
            note
            for note in direct_waste_notes
            if isinstance(note, WasteTransferNoteDocument)
        ]

    waste_notes: Dict[str, WasteTransferNoteDocument] = {}
    for waste_register in _filter_for_lovedean(
        repository.list_documents(document_type=WasteRegister.document_type),
        site_name=site_name,
    ):
        if not isinstance(waste_register, WasteRegister):
            continue
        for waste_note in waste_register.waste_transfer_notes:
            waste_notes[waste_note.wtn_number] = waste_note
    return list(waste_notes.values())


def _get_lovedean_attendance_register(
    repository: DocumentRepository,
    *,
    site_name: Optional[str] = None,
) -> Optional[SiteAttendanceRegister]:
    """Return the latest attendance register for the active site."""

    attendance_registers = [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=SiteAttendanceRegister.document_type),
            site_name=site_name,
        )
        if isinstance(document, SiteAttendanceRegister)
    ]
    if not attendance_registers:
        return None
    return max(attendance_registers, key=lambda register: register.created_at)


def _get_todays_attendance_records(
    attendance_register: Optional[SiteAttendanceRegister],
) -> List[SiteAttendanceRecord]:
    """Return today's attendance rows sorted for UI selection."""

    if attendance_register is None:
        return []
    todays_records = [
        record
        for record in attendance_register.attendance_records
        if record.date == date.today()
    ]
    return sorted(
        todays_records,
        key=lambda record: (record.workerName.casefold(), record.company.casefold()),
    )


def _get_latest_attendance_records_by_worker(
    repository: DocumentRepository,
    *,
    site_name: Optional[str] = None,
) -> Dict[tuple[str, str], SiteAttendanceRecord]:
    """Return the latest saved attendance row for each worker/company pair."""

    attendance_registers = [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=SiteAttendanceRegister.document_type),
            site_name=site_name,
        )
        if isinstance(document, SiteAttendanceRegister)
    ]
    latest_records: Dict[tuple[str, str], SiteAttendanceRecord] = {}
    for register in attendance_registers:
        for record in register.attendance_records:
            record_key = (
                record.company.casefold(),
                record.workerName.casefold(),
            )
            existing_record = latest_records.get(record_key)
            if existing_record is None or (
                record.date,
                record.timeOut,
                record.timeIn,
            ) > (
                existing_record.date,
                existing_record.timeOut,
                existing_record.timeIn,
            ):
                latest_records[record_key] = record
    return latest_records


def _build_file_4_worker_options(
    repository: DocumentRepository,
    *,
    site_name: Optional[str] = None,
) -> Dict[str, tuple[SiteWorker, SiteAttendanceRecord]]:
    """Build File 4 worker options from the live roster plus attendance details."""

    roster = build_site_worker_roster(site_name=site_name)
    latest_attendance_records = _get_latest_attendance_records_by_worker(
        repository,
        site_name=site_name,
    )
    worker_options: Dict[str, tuple[SiteWorker, SiteAttendanceRecord]] = {}
    for worker in roster:
        attendance_record = latest_attendance_records.get(
            (worker.company.casefold(), worker.worker_name.casefold())
        )
        if attendance_record is None:
            continue
        option_label = f"{worker.worker_name} ({worker.company})"
        if option_label in worker_options:
            option_label = (
                f"{option_label} - {worker.last_on_site_date.strftime('%d/%m/%Y')}"
            )
        worker_options[option_label] = (worker, attendance_record)
    return worker_options


def _build_live_permit_register_rows(
    permits: List[LadderPermit],
) -> List[Dict[str, str]]:
    """Return UI rows for the live File 4 permit register."""

    sorted_permits = sorted(
        permits,
        key=_permit_number_sort_key,
        reverse=True,
    )
    return [
        {
            "Permit Number": permit.permit_number,
            "Date Issued": (permit.issued_date or permit.valid_from_date).strftime(
                "%d/%m/%Y"
            ),
            "Worker Name": permit.worker_name or "",
            "Company": permit.worker_company or "",
            "Job Number": permit.project_number or "",
            "Location": permit.location_of_work or "",
        }
        for permit in sorted_permits
    ]


def _build_induction_rows(
    inductions: List[InductionDocument],
) -> List[Dict[str, str]]:
    """Return UI rows for recent completed inductions."""

    return [
        {
            "Date": induction.created_at.strftime("%d/%m/%Y %H:%M"),
            "Full Name": induction.individual_name,
            "Company": induction.contractor_name,
            "Occupation": induction.occupation or "",
            "CSCS": induction.cscs_number or "",
            "Roles": ", ".join(
                role_label
                for role_label, enabled in (
                    ("First Aider", induction.first_aider),
                    ("Fire Warden", induction.fire_warden),
                    ("Supervisor", induction.supervisor),
                    ("SMSTS", induction.smsts),
                )
                if enabled
            )
            or "-",
        }
        for induction in inductions
    ]


def _render_site_induction_recent_submissions(
    repository: DocumentRepository,
    inductions: List[InductionDocument],
) -> None:
    """Render recent induction submissions with delete actions and confirmation."""

    pending_delete_doc_id = st.session_state.get("site_induction_delete_pending_doc_id")
    header_columns = st.columns([1.4, 1.35, 1.0, 1.05, 1.4, 0.55], gap="small")
    for column, label in zip(
        header_columns,
        ("Date", "Full Name", "Company", "Occupation", "Roles", "Delete"),
    ):
        column.markdown(f"**{label}**")

    for induction in inductions:
        role_summary = ", ".join(
            role_label
            for role_label, enabled in (
                ("First Aider", induction.first_aider),
                ("Fire Warden", induction.fire_warden),
                ("Supervisor", induction.supervisor),
                ("SMSTS", induction.smsts),
            )
            if enabled
        ) or "-"

        row_columns = st.columns([1.4, 1.35, 1.0, 1.05, 1.4, 0.55], gap="small")
        row_columns[0].caption(induction.created_at.strftime("%d/%m/%Y %H:%M"))
        row_columns[1].write(induction.individual_name)
        row_columns[2].write(induction.contractor_name)
        row_columns[3].write(induction.occupation or "-")
        row_columns[4].caption(role_summary)
        if row_columns[5].button(
            "🗑️",
            key=f"delete-induction-{induction.doc_id}",
            help="Delete this induction record",
            use_container_width=True,
        ):
            st.session_state["site_induction_delete_pending_doc_id"] = induction.doc_id
            st.rerun()

        if pending_delete_doc_id != induction.doc_id:
            continue

        st.warning(
            "Delete this induction record? This will remove the SQLite entry and "
            "attempt to delete the saved signature PNG and completed Word document."
        )
        confirm_columns = st.columns([1.2, 1.0, 4.0], gap="small")
        if confirm_columns[0].button(
            "Confirm Delete",
            key=f"confirm-delete-induction-{induction.doc_id}",
            use_container_width=True,
        ):
            deleted_paths = repository.delete_document_and_files(induction.doc_id)
            st.session_state.pop("site_induction_delete_pending_doc_id", None)
            st.session_state["site_induction_delete_flash"] = (
                f"Deleted induction for {induction.individual_name}."
                + (
                    f" Removed {len(deleted_paths)} linked file(s)."
                    if deleted_paths
                    else " No linked files were present on disk."
                )
            )
            st.rerun()
        if confirm_columns[1].button(
            "Cancel",
            key=f"cancel-delete-induction-{induction.doc_id}",
            use_container_width=True,
        ):
            st.session_state.pop("site_induction_delete_pending_doc_id", None)
            st.rerun()


def _build_live_waste_register_rows(
    waste_notes: List[WasteTransferNoteDocument],
) -> List[Dict[str, str]]:
    """Return UI rows for the live File 1 waste register."""

    sorted_waste_notes = sorted(
        waste_notes,
        key=lambda waste_note: (waste_note.date, waste_note.created_at, waste_note.wtn_number),
        reverse=True,
    )
    return [
        {
            "Date": waste_note.date.strftime("%d/%m/%Y"),
            "Ticket No": waste_note.wtn_number,
            "Carrier": waste_note.carrier_name,
            "Waste Reg / Ticket": _format_waste_register_reference_for_ui(waste_note),
            "Description": waste_note.waste_description,
            "Tonnes": f"{waste_note.quantity_tonnes:.2f}",
            "Status": waste_note.verification_status.value,
        }
        for waste_note in sorted_waste_notes
    ]


def _format_waste_register_reference_for_ui(
    waste_note: WasteTransferNoteDocument,
) -> str:
    """Return the combined evidence reference shown in File 1."""

    parts = [
        part.strip()
        for part in (waste_note.vehicle_registration, waste_note.wtn_number)
        if part and part.strip()
    ]
    return " / ".join(parts)


def _get_file_1_waste_note_source_path(
    repository: DocumentRepository,
    waste_note: WasteTransferNoteDocument,
) -> Optional[Path]:
    """Return the physical filed PDF linked to one File 1 WTN."""

    indexed_files = repository.list_indexed_files(related_doc_id=waste_note.doc_id)
    for indexed_file in indexed_files:
        if indexed_file.file_group == FileGroup.FILE_1 and indexed_file.file_path.exists():
            return indexed_file.file_path
    return None


def _permit_number_sort_key(permit: LadderPermit) -> tuple[int, str]:
    """Return a stable sort key for printed permit references."""

    match = re.search(r"(\d+)$", permit.permit_number)
    if match is None:
        return (0, permit.permit_number)
    return (int(match.group(1)), permit.permit_number)


def _open_workspace_path(target_path: Path) -> None:
    """Open a workspace file or folder in the default macOS application."""

    try:
        subprocess.run(
            ["open", str(target_path)],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return


def _open_file_for_printing(file_path: Path) -> None:
    """Open the generated file in the default macOS application."""

    _open_workspace_path(file_path)


def _filter_for_lovedean(
    documents: Iterable[Any],
    *,
    site_name: Optional[str] = None,
) -> List[Any]:
    """Return documents whose site name matches the active project."""

    document_list = list(documents)
    resolved_site_name = (site_name or _get_project_setup().current_site_name).strip()
    if not resolved_site_name:
        return document_list

    exact_matches = [
        document
        for document in document_list
        if hasattr(document, "site_name")
        and str(document.site_name).strip().casefold() == resolved_site_name.casefold()
    ]
    if exact_matches:
        return exact_matches

    lowered_site_name = resolved_site_name.casefold()
    filtered_documents = [
        document
        for document in document_list
        if hasattr(document, "site_name")
        and (
            lowered_site_name in str(document.site_name).casefold()
            or str(document.site_name).casefold() in lowered_site_name
        )
    ]
    if filtered_documents:
        return filtered_documents
    return document_list


def _get_known_carrier_names(repository: DocumentRepository) -> List[str]:
    """Return carriers already referenced by waste or compliance data."""

    carrier_names = {ABUCS_NAME}
    carrier_names.update(
        document.carrier_name
        for document in repository.list_documents(
            document_type=CarrierComplianceDocument.document_type
        )
        if isinstance(document, CarrierComplianceDocument)
    )
    carrier_names.update(
        document.carrier_name
        for document in repository.list_documents(
            document_type=WasteTransferNoteDocument.document_type
        )
        if isinstance(document, WasteTransferNoteDocument)
    )
    for waste_register in repository.list_documents(document_type=WasteRegister.document_type):
        if not isinstance(waste_register, WasteRegister):
            continue
        carrier_names.update(
            waste_note.carrier_name for waste_note in waste_register.waste_transfer_notes
        )
    return sorted(carrier_name for carrier_name in carrier_names if carrier_name)


def _get_carrier_compliance_document(
    repository: DocumentRepository,
    carrier_name: str,
    carrier_document_type: CarrierComplianceDocumentType,
) -> Optional[CarrierComplianceDocument]:
    """Return the most relevant compliance document for one carrier/type pair."""

    matching_documents = [
        document
        for document in repository.list_documents(
            document_type=CarrierComplianceDocument.document_type
        )
        if isinstance(document, CarrierComplianceDocument)
        and document.carrier_name.casefold() == carrier_name.casefold()
        and document.carrier_document_type == carrier_document_type
    ]
    if not matching_documents:
        return None

    matching_documents.sort(
        key=lambda document: (
            document.status == DocumentStatus.ACTIVE,
            document.expiry_date,
            document.created_at,
        ),
        reverse=True,
    )
    return matching_documents[0]


def _upsert_carrier_compliance_document(
    repository: DocumentRepository,
    *,
    carrier_name: str,
    carrier_document_type: CarrierComplianceDocumentType,
    expiry_date: date,
    site_name: Optional[str] = None,
) -> CarrierComplianceDocument:
    """Create or update one carrier compliance record in SQLite."""

    existing_document = _get_carrier_compliance_document(
        repository,
        carrier_name,
        carrier_document_type,
    )
    saved_document = CarrierComplianceDocument(
        doc_id=(
            existing_document.doc_id
            if existing_document is not None
            else _build_carrier_compliance_doc_id(carrier_name, carrier_document_type)
        ),
        site_name=(
            existing_document.site_name
            if existing_document is not None
            else (site_name or _get_project_setup().current_site_name)
        ),
        created_at=(
            existing_document.created_at
            if existing_document is not None
            else datetime.now()
        ),
        status=DocumentStatus.ACTIVE,
        carrier_name=carrier_name,
        carrier_document_type=carrier_document_type,
        reference_number=_infer_carrier_reference_number(
            repository,
            carrier_name,
            carrier_document_type,
            existing_document=existing_document,
        ),
        expiry_date=expiry_date,
    )
    repository.save(saved_document)
    return saved_document


def _infer_carrier_reference_number(
    repository: DocumentRepository,
    carrier_name: str,
    carrier_document_type: CarrierComplianceDocumentType,
    *,
    existing_document: Optional[CarrierComplianceDocument] = None,
) -> str:
    """Resolve a stable carrier reference from the saved record or indexed PDF."""

    if existing_document is not None and existing_document.reference_number:
        return existing_document.reference_number

    carrier_doc_files = repository.list_indexed_files(file_category="carrier_doc_pdf")
    for indexed_file in carrier_doc_files:
        if not _file_name_mentions_carrier(indexed_file.file_name, carrier_name):
            continue
        if _file_name_matches_carrier_document_type(
            indexed_file.file_name,
            carrier_document_type,
        ):
            return Path(indexed_file.file_name).stem

    for indexed_file in carrier_doc_files:
        if _file_name_mentions_carrier(indexed_file.file_name, carrier_name):
            return Path(indexed_file.file_name).stem

    return f"{_slugify_identifier(carrier_name).upper()}-{carrier_document_type.value.upper()}"


def _build_carrier_compliance_doc_id(
    carrier_name: str,
    carrier_document_type: CarrierComplianceDocumentType,
) -> str:
    """Return a deterministic document id for carrier compliance records."""

    return f"CCD-{_slugify_identifier(carrier_name)}-{carrier_document_type.value}"


def _file_name_mentions_carrier(file_name: str, carrier_name: str) -> bool:
    """Return True when a carrier-doc filename appears to belong to the carrier."""

    lowered_name = file_name.casefold()
    lowered_carrier_name = carrier_name.casefold()
    carrier_tokens = {
        lowered_carrier_name,
        lowered_carrier_name.replace(" ", "_"),
        lowered_carrier_name.replace(" ", "-"),
        _slugify_identifier(carrier_name),
    }
    return any(token and token in lowered_name for token in carrier_tokens)


def _file_name_matches_carrier_document_type(
    file_name: str,
    carrier_document_type: CarrierComplianceDocumentType,
) -> bool:
    """Return True when the filename suggests the requested carrier doc type."""

    lowered_name = file_name.casefold()
    if carrier_document_type == CarrierComplianceDocumentType.INSURANCE:
        return "insurance" in lowered_name
    return any(token in lowered_name for token in ("licence", "license", "carrier"))


def _slugify_identifier(value: str) -> str:
    """Create a predictable lowercase identifier from UI text."""

    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")


def _get_abucs_status_rows(repository: DocumentRepository) -> List[AbucsStatusRow]:
    """Return a stable two-row Abucs compliance summary."""

    findings = [
        finding
        for finding in check_carrier_compliance(repository)
        if finding.carrier_name.casefold() == ABUCS_NAME.casefold()
    ]
    findings_by_type = {
        finding.carrier_document_type: finding for finding in findings
    }

    rows: List[AbucsStatusRow] = []
    for document_type in CarrierComplianceDocumentType:
        finding = findings_by_type.get(document_type)
        if finding is None:
            rows.append(
                AbucsStatusRow(
                    label=document_type.label,
                    status=ComplianceAlertStatus.CRITICAL,
                    reason="No compliance record saved.",
                )
            )
            continue
        rows.append(
            AbucsStatusRow(
                label=document_type.label,
                status=finding.status,
                reason=finding.reason,
            )
        )
    return rows


if __name__ == "__main__":
    main()
