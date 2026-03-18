"""Streamlit dashboard for the Uplands Lovedean site management portal."""

from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
import html
import json
from pathlib import Path
import re
import subprocess
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote, urlencode, urlparse, urlunparse

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_js_eval import streamlit_js_eval

from uplands_site_command_centre import (
    COSHH_DESTINATION,
    COSHHDocument,
    DATABASE_PATH,
    DailyAttendanceEntryDocument,
    FILE_1_OUTPUT_DIR,
    FILE_2_ATTENDANCE_OUTPUT_DIR,
    FILE_2_ATTENDANCE_SIGNATURES_DIR,
    FILE_2_DIARY_OUTPUT_DIR,
    FILE_2_TBT_ACTIVE_DOCS_DIR,
    FILE_2_CHECKLIST_OUTPUT_DIR,
    FILE_2_TBT_OUTPUT_DIR,
    FILE_2_TBT_SIGNATURES_DIR,
    FILE_3_COMPLETED_INDUCTIONS_DIR,
    FILE_3_COMPETENCY_CARDS_DIR,
    FILE_3_OUTPUT_DIR,
    FILE_3_REVIEW_DIR,
    FILE_3_SIGNATURES_DIR,
    InductionDocument,
    LadderPermit,
    LOVEDEAN_SITE_LATITUDE,
    LOVEDEAN_SITE_LONGITUDE,
    PlantAssetDocument,
    PlantInspectionType,
    PERMITS_DESTINATION,
    PLANT_PENDING_INSPECTION_TEXT,
    PLANT_HIRE_REGISTER_DIR,
    RAMSDocument,
    RAMS_DESTINATION,
    SafetyAsset,
    SITE_CHECK_WEEKDAY_KEYS,
    SITE_CHECK_WEEKDAY_LABELS,
    SiteDiaryDocument,
    CarrierComplianceDocument,
    CarrierComplianceDocumentType,
    BroadcastDispatchDocument,
    ComplianceAlertStatus,
    DocumentNotFoundError,
    DocumentStatus,
    DocumentRepository,
    FileGroup,
    SiteAttendanceRecord,
    SiteAttendanceRegister,
    SiteBroadcastContact,
    SiteWorker,
    TemplateValidationError,
    TOOLBOX_TALK_REGISTER_DIR,
    ValidationError,
    WeeklySiteCheck,
    WasteRegister,
    WasteTransferNoteDocument,
    WASTE_DESTINATION,
    build_live_site_broadcast_contacts,
    build_site_alert_sms_link,
    build_site_alert_sms_links,
    build_pending_toolbox_talk_contacts,
    build_site_worker_roster,
    build_site_gate_access_code,
    build_toolbox_talk_document_view_url,
    build_toolbox_talk_sms_message,
    calculate_haversine_distance_meters,
    check_carrier_compliance,
    complete_daily_attendance_sign_out,
    create_weekly_site_check_checklist_draft,
    create_daily_attendance_sign_in,
    update_daily_attendance_entry,
    create_site_induction_document,
    add_site_induction_evidence_files,
    update_site_induction_document,
    create_ladder_permit_draft,
    file_and_index_all,
    generate_toolbox_talk_register_document,
    generate_site_diary_document,
    detect_public_tunnel_url_from_log,
    ensure_gate_access_secret,
    generate_attendance_register_document,
    generate_site_induction_poster,
    generate_coshh_register_document,
    generate_rams_register_document,
    generate_waste_register_document,
    generate_plant_register_document,
    generate_permit_register_document,
    get_latest_toolbox_talk_document,
    get_site_induction_url,
    get_daily_contractor_headcount,
    get_waste_kpi_sheet_metadata,
    list_waste_transfer_note_source_conflicts,
    get_valid_template_tags,
    get_weekly_site_check_row_definitions,
    format_plant_inspection_reference,
    list_daily_attendance_entries,
    list_toolbox_talk_documents,
    list_toolbox_talk_completions,
    log_toolbox_talk_completion,
    load_app_settings,
    normalize_public_app_url,
    lookup_uk_postcode_details,
    list_broadcast_dispatches,
    read_toolbox_talk_document_bytes,
    park_file_3_document_for_review,
    rebuild_file_3_safety_inventory,
    run_workspace_diagnostic,
    save_toolbox_talk_document,
    save_app_settings,
    set_waste_transfer_note_source_override,
    smart_scan_waste_transfer_note,
    sync_file_4_permit_records,
    update_logged_waste_transfer_note,
    validate_site_gate_access_code,
    is_pending_plant_inspection_reference,
    build_toolbox_talk_url,
    launch_messages_sms_broadcast,
    log_broadcast_dispatch,
    ToolboxTalkDocument,
    ToolboxTalkCompletionDocument,
)


APP_ROOT = Path(__file__).resolve().parent
UPLANDS_LOGO = APP_ROOT / "Home Uplands.png"
NATIONAL_GRID_LOGO = APP_ROOT / "Ng logo.png"
PROJECT_SETUP_PATH = DATABASE_PATH.parent / "project_setup.json"
SITE_DIARY_DRAFTS_PATH = DATABASE_PATH.parent / "site_diary_drafts.json"

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
LOVEDEAN_SITE_POSTCODE = "PO8 0SJ"
LOVEDEAN_SITE_ADDRESS = "National Grid, Broadway Lane, Waterlooville, Hampshire, PO8 0SJ"
ATTENDANCE_FORM_METADATA = (
    "Date Issued: 12-AUG-2013 | Document Type: FORM | Created by: HSEQ Dept."
)
GEOFENCE_RADIUS_METERS = 500
SITE_GATE_CODE_SLOT_MINUTES = 30
MANDATORY_MANUAL_HANDLING_LABEL = "Manual Handling Certificate"
OTHER_INDUCTION_EVIDENCE_OPTION = "🗂️ Other Evidence (Type Below)"
INDUCTION_EVIDENCE_LABEL_ORDER = (
    "CSCS Card",
    MANDATORY_MANUAL_HANDLING_LABEL,
    "Asbestos Certificate",
    "CISRS Card",
    "First Aid Certificate",
    "Fire Warden Certificate",
    "Supervisor Certificate",
    "SMSTS Certificate",
    "CPCS Card",
    "Client Training Evidence",
)
WASTE_MISSING_TONNAGE_REVIEW_OPTIONS = (
    "Weight not shown on supplier ticket",
    "Awaiting monthly waste report",
    "Resolved by manager",
)
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
class File3ReviewCandidate:
    """One live File 3 record that likely needs a manual metadata review."""

    document_type: str
    doc_id: str
    company: str
    title: str
    reference: str
    version: str
    findings: tuple[str, ...]
    source_path: Optional[Path] = None

    @property
    def label(self) -> str:
        """Return a concise selectbox label for one review record."""

        finding_text = ", ".join(self.findings)
        return (
            f"{self.document_type} | {self.company or 'No company'} | "
            f"{self.title or self.reference or self.doc_id} | {finding_text}"
        )


FILE_3_REVIEW_TYPE_FILTERS = ("All", "RAMS", "COSHH")
FILE_3_REVIEW_FINDING_FILTERS = (
    "Any finding",
    "Company",
    "Title / Substance",
    "Supplier",
    "Reference",
    "Version",
)


@dataclass(frozen=True)
class ProjectSetup:
    """Portable project metadata reused across the command centre."""

    current_site_name: str
    job_number: str
    site_address: str
    client_name: str
    public_tunnel_url: str
    site_latitude: float
    site_longitude: float
    geofence_radius_meters: int
    known_sites: List["SavedSiteProfile"] = field(default_factory=list)


@dataclass(frozen=True)
class SavedSiteProfile:
    """One previously used site profile remembered by Project Setup."""

    site_name: str
    site_address: str
    client_name: str
    job_number: str
    site_latitude: float
    site_longitude: float
    geofence_radius_meters: int
    last_used_at: str = ""

    @property
    def label(self) -> str:
        """Return a compact sidebar label for this saved site."""

        postcode = _extract_uk_postcode(self.site_address)
        if postcode:
            return f"{self.site_name} ({postcode})"
        if self.site_address:
            return f"{self.site_name} | {self.site_address[:28]}"
        return self.site_name


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
        label="⚡ FILE 4: Permits & Temp Works",
        number="FILE 4",
        title="Permits & Temp Works",
        subtitle="The motor for UHSF21.09 and live permit issue control.",
    ),
    FileStation(
        label="📅 SITE ATTENDANCE REGISTER (UHSF16.09)",
        number="INDUCTION",
        title="Site Attendance Register (UHSF16.09)",
        subtitle="Daily sign-in, sign-out, and live fire roll backed by induction records.",
    ),
    FileStation(
        label="📢 SITE ALERTS & TBTs",
        number="BROADCAST",
        title="Site Alerts & TBTs",
        subtitle="Instant contact lists, remote toolbox talks, and live sign-off export.",
    ),
]
DEFAULT_FILE_STATION_LABEL = next(
    station.label for station in FILE_STATIONS if station.number == "FILE 1"
)
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
FILE_2_VIEW_OPTIONS: List[tuple[str, str]] = [
    ("checks", "Daily Checks"),
    ("attendance", "Attendance Snapshot"),
    ("diary", "Daily Site Diary"),
    ("plant", "Plant Register"),
    ("tbt", "Toolbox Talks"),
]
FILE_2_VIEW_LABELS: Dict[str, str] = {
    key: label for key, label in FILE_2_VIEW_OPTIONS
}
FILE_2_VIEW_KEYS: set[str] = {key for key, _ in FILE_2_VIEW_OPTIONS}
WEEKLY_SITE_CHECK_MODE_LABELS: Dict[str, str] = {
    "daily": "Daily Check",
    "weekly": "End of Week / Weekly Checks",
}


def _current_week_commencing(reference_date: Optional[date] = None) -> date:
    """Return the Monday date for the active site-check week."""

    resolved_date = reference_date or date.today()
    return resolved_date - timedelta(days=resolved_date.weekday())


def _current_active_day_key(reference_date: Optional[date] = None) -> str:
    """Return the current weekday key used by the File 2 checklist grid."""

    resolved_date = reference_date or date.today()
    return SITE_CHECK_WEEKDAY_KEYS[min(resolved_date.weekday(), len(SITE_CHECK_WEEKDAY_KEYS) - 1)]


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
        if not row_definition.supports_day_key(day_key):
            continue
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


def _weekly_site_check_visible_row_definitions(
    row_definitions: List[Any],
    *,
    checklist_mode: str,
) -> List[Any]:
    """Return only the rows relevant to the selected editor scope."""

    if checklist_mode == "weekly":
        return [
            row_definition
            for row_definition in row_definitions
            if row_definition.supports_weekly_checks()
        ]
    return [
        row_definition
        for row_definition in row_definitions
        if row_definition.supports_daily_checks()
    ]


def _weekly_site_check_active_column_key(
    *,
    checklist_mode: str,
    active_day_key: str,
) -> str:
    """Return the currently editable template column key."""

    return "weekly" if checklist_mode == "weekly" else active_day_key


def _weekly_site_check_signoff_snapshot(
    *,
    daily_initials_map: Dict[str, str],
    daily_time_markers_map: Dict[str, str],
    active_day_key: str,
) -> pd.DataFrame:
    """Return a compact daily sign-off summary for the current checklist week."""

    rows: List[Dict[str, str]] = []
    for day_key in SITE_CHECK_WEEKDAY_KEYS:
        initials_value = str(daily_initials_map.get(day_key, "")).strip()
        time_value = str(daily_time_markers_map.get(day_key, "")).strip()
        rows.append(
            {
                "Day": SITE_CHECK_WEEKDAY_LABELS[day_key],
                "Initials": initials_value or "—",
                "AM/PM": time_value or "—",
                "Status": (
                    "Active"
                    if day_key == active_day_key
                    else ("Signed" if initials_value or time_value else "Pending")
                ),
            }
        )
    return pd.DataFrame(rows)


def _initials_from_name(full_name: str) -> str:
    """Return a compact initials suggestion from a checked-by full name."""

    cleaned_name = str(full_name).strip()
    if not cleaned_name:
        return ""
    parts = [part for part in re.split(r"[\s\-]+", cleaned_name) if part]
    if not parts:
        return ""
    return "".join(part[0] for part in parts[:2]).upper()


def _weekly_site_check_signoff_cache_key(namespace: str, *, field_name: str) -> str:
    """Return the session key used for the non-widget daily sign-off cache."""

    return f"weekly-site-check-signoff-{field_name}-{namespace}"


def _weekly_site_check_signoff_widget_key(namespace: str, *, field_name: str) -> str:
    """Return the session key used for the visible active-day sign-off widget."""

    return f"weekly-site-check-signoff-widget-{field_name}-{namespace}"


def _prefill_weekly_site_check_day_from_previous_day(
    *,
    namespace: str,
    row_definitions: List[Any],
    target_day_key: str,
    valid_template_tags: set[str],
) -> bool:
    """Seed a blank daily column from the previous completed day when available."""

    try:
        target_index = SITE_CHECK_WEEKDAY_KEYS.index(target_day_key)
    except ValueError:
        return False
    if target_index == 0:
        return False

    relevant_row_definitions = [
        row_definition
        for row_definition in row_definitions
        if row_definition.supports_day_key(target_day_key)
        and _weekly_site_check_template_tag(target_day_key, row_definition.row_number)
        in valid_template_tags
    ]
    if not relevant_row_definitions:
        return False

    target_values = [
        st.session_state.get(
            _weekly_site_check_state_key(
                namespace,
                kind="cell",
                row_number=row_definition.row_number,
                day_key=target_day_key,
            )
        )
        for row_definition in relevant_row_definitions
    ]
    if any(value is not None for value in target_values):
        return False

    previous_day_key = SITE_CHECK_WEEKDAY_KEYS[target_index - 1]
    previous_values = [
        st.session_state.get(
            _weekly_site_check_state_key(
                namespace,
                kind="cell",
                row_number=row_definition.row_number,
                day_key=previous_day_key,
            )
        )
        for row_definition in relevant_row_definitions
    ]
    if not any(value is not None for value in previous_values):
        return False

    for row_definition, previous_value in zip(relevant_row_definitions, previous_values):
        st.session_state[
            _weekly_site_check_state_key(
                namespace,
                kind="cell",
                row_number=row_definition.row_number,
                day_key=target_day_key,
            )
        ] = previous_value
    return True


def _recommended_weekly_site_check_day_key(
    *,
    week_commencing: date,
    weekly_site_check: Optional[WeeklySiteCheck],
    daily_initials_map: Optional[Dict[str, str]] = None,
    daily_time_markers_map: Optional[Dict[str, str]] = None,
    reference_date: Optional[date] = None,
) -> str:
    """Return the most sensible active day for the selected checklist week."""

    resolved_reference_date = reference_date or date.today()
    current_week_commencing = _current_week_commencing(resolved_reference_date)
    if week_commencing > current_week_commencing:
        return "mon"

    latest_relevant_index = (
        resolved_reference_date.weekday()
        if week_commencing == current_week_commencing
        else len(SITE_CHECK_WEEKDAY_KEYS) - 1
    )
    latest_relevant_index = min(latest_relevant_index, len(SITE_CHECK_WEEKDAY_KEYS) - 1)

    effective_initials_map = {
        day_key: str(
            (daily_initials_map or {}).get(
                day_key,
                (
                    weekly_site_check.daily_initials.get(day_key, "")
                    if weekly_site_check is not None
                    else ""
                ),
            )
        ).strip()
        for day_key in SITE_CHECK_WEEKDAY_KEYS
    }
    effective_time_map = {
        day_key: str(
            (daily_time_markers_map or {}).get(
                day_key,
                (
                    weekly_site_check.daily_time_markers.get(day_key, "")
                    if weekly_site_check is not None
                    else ""
                ),
            )
        ).strip()
        for day_key in SITE_CHECK_WEEKDAY_KEYS
    }

    for day_index in range(0, latest_relevant_index + 1):
        day_key = SITE_CHECK_WEEKDAY_KEYS[day_index]
        if not effective_initials_map.get(day_key) or not effective_time_map.get(day_key):
            return day_key

    if week_commencing == current_week_commencing:
        return SITE_CHECK_WEEKDAY_KEYS[latest_relevant_index]
    if weekly_site_check is not None:
        return weekly_site_check.active_day_key
    return SITE_CHECK_WEEKDAY_KEYS[-1]


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
    week_commencing: date,
    weekly_site_check: Optional[WeeklySiteCheck],
    row_definitions: List[Any],
) -> None:
    """Load one week of File 2 matrix state into Streamlit session state once."""

    loaded_key = "weekly-site-check-editor-loaded"
    namespace_changed = st.session_state.get(loaded_key) != namespace

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
            stored_value = row_state.get_value(day_key) if row_state is not None else None
            if namespace_changed or state_key not in st.session_state:
                st.session_state[state_key] = stored_value

    for day_key in SITE_CHECK_WEEKDAY_KEYS:
        initials_key = _weekly_site_check_state_key(
            namespace,
            kind="initials",
            day_key=day_key,
        )
        time_key = _weekly_site_check_state_key(
            namespace,
            kind="time",
            day_key=day_key,
        )
        stored_initials = (
            weekly_site_check.daily_initials.get(day_key, "")
            if weekly_site_check is not None
            else ""
        )
        stored_time = (
            weekly_site_check.daily_time_markers.get(day_key, "")
            if weekly_site_check is not None
            else ""
        )
        if namespace_changed or initials_key not in st.session_state:
            st.session_state[initials_key] = stored_initials
        if namespace_changed or time_key not in st.session_state:
            st.session_state[time_key] = stored_time

    checked_by_key = _weekly_site_check_state_key(namespace, kind="checked-by")
    active_day_key = _weekly_site_check_state_key(namespace, kind="active-day")
    stored_checked_by = (
        weekly_site_check.checked_by if weekly_site_check is not None else SITE_MANAGER_NAME
    )
    stored_active_day = (
        weekly_site_check.active_day_key
        if weekly_site_check is not None
        else _recommended_weekly_site_check_day_key(
            week_commencing=week_commencing,
            weekly_site_check=weekly_site_check,
        )
    )
    if namespace_changed or checked_by_key not in st.session_state:
        st.session_state[checked_by_key] = stored_checked_by
    if namespace_changed or active_day_key not in st.session_state:
        st.session_state[active_day_key] = stored_active_day
    st.session_state[loaded_key] = namespace


def _default_project_setup() -> ProjectSetup:
    """Return the fallback project metadata used before setup is saved."""

    return ProjectSetup(
        current_site_name=PROJECT_NAME,
        job_number="",
        site_address=LOVEDEAN_SITE_ADDRESS,
        client_name="National Grid",
        public_tunnel_url="",
        site_latitude=LOVEDEAN_SITE_LATITUDE,
        site_longitude=LOVEDEAN_SITE_LONGITUDE,
        geofence_radius_meters=GEOFENCE_RADIUS_METERS,
        known_sites=[],
    )


def _is_lovedean_site_name(site_name: str) -> bool:
    """Return whether one site name is the canonical Lovedean profile."""

    return str(site_name or "").strip().casefold() == PROJECT_NAME.casefold()


def _normalize_saved_site_profile(profile: SavedSiteProfile) -> SavedSiteProfile:
    """Keep the canonical Lovedean profile locked to its real postcode and coordinates."""

    if not _is_lovedean_site_name(profile.site_name):
        return profile
    return replace(
        profile,
        site_address=LOVEDEAN_SITE_ADDRESS,
        client_name=profile.client_name.strip() or "National Grid",
        site_latitude=LOVEDEAN_SITE_LATITUDE,
        site_longitude=LOVEDEAN_SITE_LONGITUDE,
    )


def _normalize_project_setup(project_setup: ProjectSetup) -> ProjectSetup:
    """Keep the live Lovedean setup aligned to the real site identity."""

    if not _is_lovedean_site_name(project_setup.current_site_name):
        return project_setup
    return replace(
        project_setup,
        site_address=LOVEDEAN_SITE_ADDRESS,
        client_name=project_setup.client_name.strip() or "National Grid",
        site_latitude=LOVEDEAN_SITE_LATITUDE,
        site_longitude=LOVEDEAN_SITE_LONGITUDE,
    )


def _saved_site_profile_from_payload(payload: Any) -> Optional[SavedSiteProfile]:
    """Return one validated saved-site profile from JSON payload data."""

    if not isinstance(payload, dict):
        return None
    site_name = str(payload.get("site_name") or "").strip()
    if not site_name:
        return None
    return _normalize_saved_site_profile(
        SavedSiteProfile(
        site_name=site_name,
        site_address=str(payload.get("site_address") or "").strip(),
        client_name=str(payload.get("client_name") or "").strip(),
        job_number=str(payload.get("job_number") or "").strip(),
        site_latitude=_coerce_float(payload.get("site_latitude"), LOVEDEAN_SITE_LATITUDE),
        site_longitude=_coerce_float(
            payload.get("site_longitude"),
            LOVEDEAN_SITE_LONGITUDE,
        ),
        geofence_radius_meters=max(
            1,
            int(
                round(
                    _coerce_float(
                        payload.get("geofence_radius_meters"),
                        float(GEOFENCE_RADIUS_METERS),
                    )
                )
            ),
        ),
        last_used_at=str(payload.get("last_used_at") or "").strip(),
        )
    )


def _saved_site_profile_from_project_setup(
    project_setup: ProjectSetup,
    *,
    last_used_at: Optional[str] = None,
) -> SavedSiteProfile:
    """Build one saved-site profile from the active project setup."""

    return _normalize_saved_site_profile(
        SavedSiteProfile(
        site_name=project_setup.current_site_name.strip() or PROJECT_NAME,
        site_address=project_setup.site_address.strip(),
        client_name=project_setup.client_name.strip(),
        job_number=project_setup.job_number.strip(),
        site_latitude=float(project_setup.site_latitude),
        site_longitude=float(project_setup.site_longitude),
        geofence_radius_meters=max(1, int(project_setup.geofence_radius_meters)),
        last_used_at=last_used_at or datetime.now().isoformat(timespec="seconds"),
        )
    )


def _merge_known_site_profiles(
    project_setup: ProjectSetup,
    known_sites: Optional[List[SavedSiteProfile]] = None,
) -> List[SavedSiteProfile]:
    """Return the saved-site list with the current site remembered and deduped."""

    merged_profiles = [
        _normalize_saved_site_profile(profile)
        for profile in list(known_sites if known_sites is not None else project_setup.known_sites)
    ]
    current_profile = _saved_site_profile_from_project_setup(project_setup)
    deduped_profiles: Dict[str, SavedSiteProfile] = {}
    for profile in merged_profiles + [current_profile]:
        profile_key = (
            profile.site_name.casefold().strip(),
            profile.site_address.casefold().strip(),
        )
        existing_profile = deduped_profiles.get(str(profile_key))
        if existing_profile is None or profile.last_used_at >= existing_profile.last_used_at:
            deduped_profiles[str(profile_key)] = profile

    return sorted(
        deduped_profiles.values(),
        key=lambda profile: (
            profile.last_used_at or "",
            profile.site_name.casefold(),
        ),
        reverse=True,
    )


def _replace_or_append_postcode(existing_address: str, normalized_postcode: str) -> str:
    """Replace the postcode inside an address when present, otherwise append it once."""

    cleaned_address = str(existing_address).strip()
    if not normalized_postcode:
        return cleaned_address
    if not cleaned_address:
        return normalized_postcode

    existing_postcode = _extract_uk_postcode(cleaned_address)
    if existing_postcode:
        updated_address = re.sub(
            r"\b([A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b",
            normalized_postcode,
            cleaned_address,
            flags=re.IGNORECASE,
        )
        updated_address = re.sub(
            rf"(?:\s*,?\s*{re.escape(normalized_postcode)}){{2,}}",
            f" {normalized_postcode}",
            updated_address,
            flags=re.IGNORECASE,
        )
        return re.sub(r"\s{2,}", " ", updated_address).strip(" ,")
    if normalized_postcode.casefold() in cleaned_address.casefold():
        return cleaned_address
    return f"{cleaned_address}, {normalized_postcode}".strip(", ")


def _strip_uk_postcode(raw_text: str) -> str:
    """Remove any postcode token from free text while keeping the address readable."""

    cleaned_text = str(raw_text or "").strip()
    if not cleaned_text:
        return ""
    updated_text = re.sub(
        r"\b([A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b",
        "",
        cleaned_text,
        flags=re.IGNORECASE,
    )
    updated_text = re.sub(r"\s*,\s*,+", ", ", updated_text)
    updated_text = re.sub(r"\s{2,}", " ", updated_text)
    return updated_text.strip(" ,")


def _build_site_address_from_postcode_result(
    postcode_result: Dict[str, Any],
    site_or_building_name: str = "",
) -> str:
    """Return a clean site address built from one postcode lookup result."""

    address_parts: List[str] = []
    leading_line = str(site_or_building_name or "").strip()
    if leading_line:
        address_parts.append(leading_line)
    formatted_address = str(postcode_result.get("formatted_address") or "").strip()
    if formatted_address:
        address_parts.append(formatted_address)
    return ", ".join(address_parts).strip(", ")


def _matching_known_sites_for_postcode(
    project_setup: ProjectSetup,
    normalized_postcode: str,
) -> List[SavedSiteProfile]:
    """Return saved sites already remembered for one postcode."""

    postcode_key = str(normalized_postcode or "").strip().casefold()
    if not postcode_key:
        return []
    return [
        profile
        for profile in project_setup.known_sites
        if _extract_uk_postcode(profile.site_address).casefold() == postcode_key
    ]


def _clear_project_setup_postcode_state() -> None:
    """Queue a safe reset of the staged postcode search state."""

    st.session_state["project_setup_postcode_clear_pending"] = True


def _flush_project_setup_postcode_state_clear() -> None:
    """Apply any queued postcode-state reset before widgets are created."""

    if not st.session_state.pop("project_setup_postcode_clear_pending", False):
        return

    for state_key in (
        "project_setup_postcode_result",
        "project_setup_postcode_resolution_choice",
        "project_setup_postcode_site_name",
    ):
        st.session_state.pop(state_key, None)


def _load_project_setup() -> ProjectSetup:
    """Load persisted project metadata from disk."""

    default_setup = _default_project_setup()
    try:
        payload = json.loads(PROJECT_SETUP_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default_setup
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return default_setup

    known_sites_payload = payload.get("known_sites", [])
    known_sites = [
        profile
        for profile in (
            _saved_site_profile_from_payload(item)
            for item in (known_sites_payload if isinstance(known_sites_payload, list) else [])
        )
        if profile is not None
    ]

    loaded_setup = _normalize_project_setup(
        ProjectSetup(
        current_site_name=str(payload.get("current_site_name") or default_setup.current_site_name).strip()
        or default_setup.current_site_name,
        job_number=str(payload.get("job_number") or "").strip(),
        site_address=str(payload.get("site_address") or "").strip(),
        client_name=str(payload.get("client_name") or default_setup.client_name).strip()
        or default_setup.client_name,
        public_tunnel_url=normalize_public_app_url(
            str(payload.get("public_tunnel_url") or "").strip()
        ),
        site_latitude=_coerce_float(
            payload.get("site_latitude"),
            default_setup.site_latitude,
        ),
        site_longitude=_coerce_float(
            payload.get("site_longitude"),
            default_setup.site_longitude,
        ),
        geofence_radius_meters=max(
            1,
            int(
                round(
                    _coerce_float(
                        payload.get("geofence_radius_meters"),
                        float(default_setup.geofence_radius_meters),
                    )
                )
            ),
        ),
        known_sites=known_sites,
        )
    )
    return replace(
        loaded_setup,
        known_sites=_merge_known_site_profiles(loaded_setup, known_sites),
    )


def _save_project_setup(project_setup: ProjectSetup) -> ProjectSetup:
    """Persist project metadata to a small JSON file."""

    project_setup = _normalize_project_setup(project_setup)
    project_setup_to_save = replace(
        project_setup,
        public_tunnel_url=normalize_public_app_url(project_setup.public_tunnel_url),
        known_sites=_merge_known_site_profiles(project_setup),
    )
    PROJECT_SETUP_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROJECT_SETUP_PATH.write_text(
        json.dumps(
            {
                "current_site_name": project_setup_to_save.current_site_name,
                "job_number": project_setup_to_save.job_number,
                "site_address": project_setup_to_save.site_address,
                "client_name": project_setup_to_save.client_name,
                "public_tunnel_url": project_setup_to_save.public_tunnel_url,
                "site_latitude": project_setup_to_save.site_latitude,
                "site_longitude": project_setup_to_save.site_longitude,
                "geofence_radius_meters": project_setup_to_save.geofence_radius_meters,
                "known_sites": [
                    {
                        "site_name": profile.site_name,
                        "site_address": profile.site_address,
                        "client_name": profile.client_name,
                        "job_number": profile.job_number,
                        "site_latitude": profile.site_latitude,
                        "site_longitude": profile.site_longitude,
                        "geofence_radius_meters": profile.geofence_radius_meters,
                        "last_used_at": profile.last_used_at,
                    }
                    for profile in project_setup_to_save.known_sites
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return project_setup_to_save


def _load_site_diary_drafts_payload() -> Dict[str, Dict[str, str]]:
    """Load any saved Site Diary draft text fields from disk."""

    try:
        payload = json.loads(SITE_DIARY_DRAFTS_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_site_diary_drafts_payload(payload: Dict[str, Dict[str, str]]) -> None:
    """Persist Site Diary draft text fields to disk."""

    SITE_DIARY_DRAFTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SITE_DIARY_DRAFTS_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _build_site_diary_draft_storage_key(site_name: str, target_date: date) -> str:
    """Return the stable storage key for one site/date diary draft."""

    return f"{site_name.strip()}::{target_date.isoformat()}"


def _load_site_diary_text_draft(
    site_name: str,
    target_date: date,
) -> Dict[str, str]:
    """Return any saved Site Diary draft text values for one site/date."""

    payload = _load_site_diary_drafts_payload()
    raw_entry = payload.get(_build_site_diary_draft_storage_key(site_name, target_date), {})
    if not isinstance(raw_entry, dict):
        return {}
    return {
        "incidents_details": str(raw_entry.get("incidents_details", "") or ""),
        "area_handovers": str(raw_entry.get("area_handovers", "") or ""),
        "todays_comments": str(raw_entry.get("todays_comments", "") or ""),
    }


def _save_site_diary_text_draft(
    site_name: str,
    target_date: date,
    *,
    incidents_details: str,
    area_handovers: str,
    todays_comments: str,
) -> None:
    """Persist the editable Site Diary text draft for one site/date."""

    payload = _load_site_diary_drafts_payload()
    payload[_build_site_diary_draft_storage_key(site_name, target_date)] = {
        "incidents_details": str(incidents_details or ""),
        "area_handovers": str(area_handovers or ""),
        "todays_comments": str(todays_comments or ""),
    }
    _save_site_diary_drafts_payload(payload)


def _clear_site_diary_text_draft(site_name: str, target_date: date) -> None:
    """Remove any saved Site Diary draft text for one site/date."""

    payload = _load_site_diary_drafts_payload()
    draft_key = _build_site_diary_draft_storage_key(site_name, target_date)
    if draft_key not in payload:
        return
    del payload[draft_key]
    _save_site_diary_drafts_payload(payload)


def _persist_site_diary_text_draft(
    site_name: str,
    target_date: date,
    incidents_state_key: str,
    handovers_state_key: str,
    comments_state_key: str,
) -> None:
    """Persist the current diary text widgets as a recoverable draft."""

    _save_site_diary_text_draft(
        site_name,
        target_date,
        incidents_details=str(st.session_state.get(incidents_state_key, "") or ""),
        area_handovers=str(st.session_state.get(handovers_state_key, "") or ""),
        todays_comments=str(st.session_state.get(comments_state_key, "") or ""),
    )


def _site_diary_state_keys_for_date(target_date: date) -> Dict[str, str]:
    """Return the widget/session-state keys used by one diary date."""

    date_suffix = target_date.isoformat()
    return {
        "contractors": f"file2_site_diary_contractors_{date_suffix}",
        "uplands_days": f"file2_site_diary_uplands_days_{date_suffix}",
        "uplands_nights": f"file2_site_diary_uplands_nights_{date_suffix}",
        "skip_exchange": f"file2_site_diary_skip_exchange_{date_suffix}",
        "fire_day_on": f"file2_site_diary_fire_day_on_{date_suffix}",
        "fire_day_off": f"file2_site_diary_fire_day_off_{date_suffix}",
        "fire_night_on": f"file2_site_diary_fire_night_on_{date_suffix}",
        "fire_night_off": f"file2_site_diary_fire_night_off_{date_suffix}",
        "weather_dry": f"file2_site_diary_weather_dry_{date_suffix}",
        "weather_mixed": f"file2_site_diary_weather_mixed_{date_suffix}",
        "weather_wet": f"file2_site_diary_weather_wet_{date_suffix}",
        "incidents": f"file2_site_diary_incidents_{date_suffix}",
        "hs_reported_tick": f"file2_site_diary_hs_reported_tick_{date_suffix}",
        "visitors": f"file2_site_diary_visitors_{date_suffix}",
        "handovers": f"file2_site_diary_handovers_{date_suffix}",
        "comments": f"file2_site_diary_comments_{date_suffix}",
        "generate": f"file2_site_diary_generate_{date_suffix}",
    }


def _queue_site_diary_form_reset(target_date: date) -> None:
    """Queue a safe reset of the diary widgets for one date."""

    st.session_state["file2_site_diary_reset_pending"] = target_date.isoformat()


def _apply_site_diary_form_reset_if_pending(site_name: str) -> None:
    """Apply any queued diary reset before the widgets for that date are created."""

    raw_pending_date = str(
        st.session_state.pop("file2_site_diary_reset_pending", "") or ""
    ).strip()
    if not raw_pending_date:
        return

    try:
        pending_date = date.fromisoformat(raw_pending_date)
    except ValueError:
        return

    for state_key in _site_diary_state_keys_for_date(pending_date).values():
        st.session_state.pop(state_key, None)
    _clear_site_diary_text_draft(site_name, pending_date)
    st.session_state["file2_site_diary_flash"] = (
        f"Diary form reset for {pending_date:%d/%m/%Y}."
    )


def _coerce_float(raw_value: Any, fallback: float) -> float:
    """Return a float value or the supplied fallback."""

    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return float(fallback)


def _extract_uk_postcode(raw_text: str) -> str:
    """Return one UK postcode candidate from free text."""

    postcode_matches = re.findall(
        r"\b([A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2})\b",
        str(raw_text or "").upper(),
    )
    if not postcode_matches:
        return ""
    compact_postcode = postcode_matches[-1].replace(" ", "")
    if len(compact_postcode) < 5:
        return ""
    return f"{compact_postcode[:-3]} {compact_postcode[-3:]}".strip()


def _get_project_setup() -> ProjectSetup:
    """Return the cached project metadata for the active session."""

    cached_setup = st.session_state.get("project_setup")
    if isinstance(cached_setup, ProjectSetup):
        return cached_setup

    loaded_setup = _load_project_setup()
    root_settings = load_app_settings()
    loaded_setup = replace(
        loaded_setup,
        public_tunnel_url=normalize_public_app_url(loaded_setup.public_tunnel_url),
    )
    if not loaded_setup.public_tunnel_url and root_settings["public_tunnel_url"]:
        loaded_setup = replace(
            loaded_setup,
            public_tunnel_url=normalize_public_app_url(root_settings["public_tunnel_url"]),
        )
    st.session_state["project_setup"] = loaded_setup
    return loaded_setup


def _is_tunnel_running() -> bool:
    """Return True when cloudflared is currently running."""

    process_result = subprocess.run(
        ["pgrep", "cloudflared"],
        check=False,
        capture_output=True,
        text=True,
    )
    return process_result.returncode == 0 and bool(process_result.stdout.strip())


def _synchronise_public_tunnel_settings(project_setup: ProjectSetup) -> ProjectSetup:
    """Keep the root settings file and project setup tunnel URL aligned."""

    detected_tunnel_url = detect_public_tunnel_url_from_log()
    saved_tunnel_url = load_app_settings()["public_tunnel_url"]
    resolved_tunnel_url = normalize_public_app_url(
        detected_tunnel_url
        or project_setup.public_tunnel_url.strip()
        or saved_tunnel_url
    )

    if resolved_tunnel_url != saved_tunnel_url:
        save_app_settings(public_tunnel_url=resolved_tunnel_url)

    if resolved_tunnel_url != project_setup.public_tunnel_url:
        project_setup = replace(project_setup, public_tunnel_url=resolved_tunnel_url)
        project_setup = _save_project_setup(project_setup)
        st.session_state["project_setup"] = project_setup

    return project_setup


def _reset_ladder_permit_form_state() -> None:
    """Reset the File 4 permit helper fields back to their default values."""

    st.session_state["ladder_permit_company_context_worker"] = ""
    st.session_state["ladder_permit_company_selection"] = ""
    st.session_state["ladder_permit_worker_company_override"] = ""
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


def _queue_ladder_permit_form_reset() -> None:
    """Queue a safe File 4 permit reset for the next rerun before widgets render."""

    st.session_state["ladder_permit_reset_pending"] = True


def _reset_site_induction_form_state() -> None:
    """Reset the induction kiosk fields for the next operative."""

    state_defaults = {
        "site_induction_full_name": "",
        "site_induction_home_address": "",
        "site_induction_contact_number": "",
        "site_induction_company_selection": "-- Select Company --",
        "site_induction_new_company_name": "",
        "site_induction_occupation": "",
        "site_induction_emergency_contact": "",
        "site_induction_emergency_tel": "",
        "site_induction_medical": "",
        "site_induction_cscs_number": "",
        "site_induction_cscs_expiry": None,
        "site_induction_asbestos_cert": False,
        "site_induction_asbestos_cert_choice": "No",
        "site_induction_erect_scaffold": False,
        "site_induction_erect_scaffold_choice": "No",
        "site_induction_cisrs_no": "",
        "site_induction_cisrs_expiry": None,
        "site_induction_operate_plant": False,
        "site_induction_operate_plant_choice": "No",
        "site_induction_cpcs_no": "",
        "site_induction_cpcs_expiry": None,
        "site_induction_client_training_desc": "",
        "site_induction_client_training_date": None,
        "site_induction_client_training_expiry": None,
        "site_induction_first_aider": False,
        "site_induction_first_aider_choice": "No",
        "site_induction_fire_warden": False,
        "site_induction_fire_warden_choice": "No",
        "site_induction_supervisor": False,
        "site_induction_supervisor_choice": "No",
        "site_induction_smsts": False,
        "site_induction_smsts_choice": "No",
        "site_induction_competency_expiry_date": date.today() + timedelta(days=365),
    }
    for state_key, state_value in state_defaults.items():
        st.session_state[state_key] = state_value
    for transient_key in (
        "site_induction_competency_cards",
        "site_induction_cscs_card_upload",
        "site_induction_manual_handling_upload",
        "site_induction_asbestos_card_upload",
        "site_induction_cisrs_card_upload",
        "site_induction_cpcs_card_upload",
        "site_induction_client_training_upload",
        "site_induction_first_aider_upload",
        "site_induction_fire_warden_upload",
        "site_induction_supervisor_upload",
        "site_induction_smsts_upload",
        "site_induction_kiosk_complete_doc_id",
        "site_induction_view_doc_id",
    ):
        st.session_state.pop(transient_key, None)


def _build_site_induction_competency_file_payloads(
    labelled_uploads: List[tuple[str, Any]],
) -> List[Dict[str, Any]]:
    """Return saved-upload payloads from the labelled induction card uploaders."""

    payloads: List[Dict[str, Any]] = []
    for competency_label, uploaded_file in labelled_uploads:
        if uploaded_file is None:
            continue
        uploaded_files = (
            list(uploaded_file)
            if isinstance(uploaded_file, (list, tuple))
            else [uploaded_file]
        )
        for uploaded_item in uploaded_files:
            if uploaded_item is None:
                continue
            file_name = Path(str(getattr(uploaded_item, "name", "") or "")).name
            if not file_name:
                continue
            try:
                file_bytes = uploaded_item.getvalue()
            except Exception:
                file_bytes = b""
            if not file_bytes:
                continue
            payloads.append(
                {
                    "label": competency_label,
                    "name": file_name,
                    "bytes": file_bytes,
                }
            )
    return payloads


def _render_site_induction_yes_no_field(
    label: str,
    *,
    key: str,
    disabled: bool = False,
) -> bool:
    """Render one induction yes/no control while keeping a clean boolean state value."""

    choice_key = f"{key}_choice"
    if st.session_state.get(choice_key) not in {"Yes", "No"}:
        st.session_state[choice_key] = "Yes" if bool(st.session_state.get(key, False)) else "No"
    st.markdown(
        f"<div class='site-induction-binary-label'>{html.escape(label)}</div>",
        unsafe_allow_html=True,
    )
    choice = st.radio(
        label,
        options=["No", "Yes"],
        key=choice_key,
        horizontal=True,
        disabled=disabled,
        label_visibility="collapsed",
    )
    resolved_value = choice == "Yes"
    st.session_state[key] = resolved_value
    return resolved_value


def _reset_site_attendance_form_state() -> None:
    """Reset the live UHSF16.09 sign-in/sign-out fields for the next operative."""

    state_defaults = {
        "site_attendance_action_mode": "sign_in",
        "site_attendance_worker_search": "",
        "site_attendance_sign_out_search": "",
        "site_attendance_prefill_induction_doc_id": "",
        "site_attendance_selected_induction_doc_id": "",
        "site_attendance_vehicle_registration_context_doc_id": "",
        "site_attendance_distance_travelled_context_doc_id": "",
        "site_attendance_vehicle_registration": "",
        "site_attendance_distance_travelled": "",
        "site_attendance_selected_sign_out_doc_id": "",
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


def _is_tbt_kiosk_requested() -> bool:
    """Return True when the current URL explicitly requests the mobile TBT signer."""

    raw_station_value = st.query_params.get("station")
    if not raw_station_value:
        return False
    return str(raw_station_value).strip().casefold() == "tbt"


def _get_tbt_topic_from_query_params() -> str:
    """Return the requested toolbox talk topic from the current URL."""

    raw_topic_value = st.query_params.get("topic")
    if not raw_topic_value:
        return ""
    return str(raw_topic_value).strip()


def _is_kiosk_mode_requested() -> bool:
    """Return True when the current URL explicitly requests kiosk mode."""

    raw_mode_value = st.query_params.get("mode")
    if not raw_mode_value:
        return False
    return str(raw_mode_value).strip().casefold() == "kiosk"


def _get_kiosk_view_from_query_params() -> str:
    """Return the requested kiosk subview from the current URL."""

    raw_kiosk_view = st.query_params.get("kiosk_view")
    if not raw_kiosk_view:
        return ""
    resolved_kiosk_view = str(raw_kiosk_view).strip().casefold()
    return resolved_kiosk_view if resolved_kiosk_view in {"attendance", "induction"} else ""


def _clear_kiosk_geolocation_query_params() -> None:
    """Remove any previously captured kiosk geolocation query params."""

    for key in ("geo_lat", "geo_lng", "geo_acc", "geo_error", "geo_nonce", "geo_source"):
        if key in st.query_params:
            del st.query_params[key]


def _clear_project_setup_geolocation_query_params() -> None:
    """Remove any previously captured project-setup geolocation query params."""

    for key in ("setup_geo_lat", "setup_geo_lng", "setup_geo_acc", "setup_geo_error"):
        if key in st.query_params:
            del st.query_params[key]


def _clear_site_diary_dictation_query_params() -> None:
    """Remove any previously captured Site Diary dictation query params."""

    for key in (
        "dictation_target",
        "dictation_text",
        "dictation_error",
        "dictation_nonce",
    ):
        if key in st.query_params:
            del st.query_params[key]


def _get_file_2_view_from_query_params() -> str:
    """Return the requested File 2 subview from the current URL, if valid."""

    raw_file_2_view = st.query_params.get("file2_view")
    if not raw_file_2_view:
        return ""
    resolved_view = str(raw_file_2_view).strip().casefold()
    return resolved_view if resolved_view in FILE_2_VIEW_KEYS else ""


def _sync_file_2_view_query_param(view_key: str) -> None:
    """Persist the active File 2 subview into the current URL."""

    if view_key in FILE_2_VIEW_KEYS:
        st.query_params["file2_view"] = view_key
    elif "file2_view" in st.query_params:
        del st.query_params["file2_view"]


def _sync_manager_station_query_params(station_label: str) -> None:
    """Persist the active manager station into the current URL."""

    matched_station = next(
        (station for station in FILE_STATIONS if station.label == station_label),
        None,
    )
    if matched_station is None:
        return

    st.query_params["station"] = matched_station.number
    for transient_key in ("mode", "kiosk_view", "topic"):
        if transient_key in st.query_params:
            del st.query_params[transient_key]


def _site_diary_dictation_friendly_label(target_state_key: str) -> str:
    """Return the visible diary-field label for one dictation target key."""

    lowered_key = target_state_key.casefold()
    if "incidents" in lowered_key:
        return "Incidents Details"
    if "handovers" in lowered_key:
        return "Area Handovers"
    if "comments" in lowered_key:
        return "Today's Comments"
    return "Daily Site Diary"


def _extract_site_diary_date_from_state_key(target_state_key: str) -> Optional[date]:
    """Return the encoded diary date from one diary field state key, if present."""

    match = re.search(r"(\d{4}-\d{2}-\d{2})$", target_state_key.strip())
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def _apply_site_diary_context_query_params() -> None:
    """Seed the manager Site Diary context from the current URL when present."""

    requested_file_2_view = _get_file_2_view_from_query_params()
    if requested_file_2_view and "file2_active_view" not in st.session_state:
        st.session_state["file2_active_view"] = requested_file_2_view

    raw_diary_date = st.query_params.get("site_diary_date")
    if not raw_diary_date:
        return

    try:
        resolved_diary_date = date.fromisoformat(str(raw_diary_date).strip())
    except ValueError:
        return

    if "file2_site_diary_date" not in st.session_state:
        st.session_state["file2_site_diary_date"] = resolved_diary_date


def _apply_site_diary_dictation_query_payload(site_name: str) -> None:
    """Apply any popup dictation transcript returned through the current URL."""

    raw_target = st.query_params.get("dictation_target")
    raw_nonce = st.query_params.get("dictation_nonce")
    if not raw_target or not raw_nonce:
        return

    target_state_key = str(raw_target).strip()
    nonce = str(raw_nonce).strip()
    file_2_station_label = next(
        station.label for station in FILE_STATIONS if station.number == "FILE 2"
    )
    st.session_state["active_file_station"] = file_2_station_label
    st.session_state["file2_active_view"] = "diary"
    st.session_state["file2_site_diary_scroll_pending"] = True
    diary_target_date = _extract_site_diary_date_from_state_key(target_state_key)
    if diary_target_date is not None:
        draft_text_fields = _load_site_diary_text_draft(site_name, diary_target_date)
        for field_name, draft_value in draft_text_fields.items():
            draft_state_key = (
                f"file2_site_diary_{field_name.replace('incidents_details', 'incidents').replace('area_handovers', 'handovers').replace('todays_comments', 'comments')}_{diary_target_date.isoformat()}"
            )
            if draft_value and draft_state_key not in st.session_state:
                st.session_state[draft_state_key] = draft_value
        st.session_state["file2_site_diary_date"] = diary_target_date
        st.query_params["site_diary_date"] = diary_target_date.isoformat()
    _sync_manager_station_query_params(file_2_station_label)
    _sync_file_2_view_query_param("diary")
    transcript = str(st.query_params.get("dictation_text", "") or "").strip()
    error_message = str(st.query_params.get("dictation_error", "") or "").strip()

    try:
        _apply_site_diary_dictation_result(
            {
                "target": target_state_key,
                "transcript": transcript,
                "error": error_message,
                "nonce": nonce,
            },
            friendly_label=_site_diary_dictation_friendly_label(target_state_key),
        )
        if diary_target_date is not None:
            _persist_site_diary_text_draft(
                site_name,
                diary_target_date,
                f"file2_site_diary_incidents_{diary_target_date.isoformat()}",
                f"file2_site_diary_handovers_{diary_target_date.isoformat()}",
                f"file2_site_diary_comments_{diary_target_date.isoformat()}",
            )
    finally:
        _clear_site_diary_dictation_query_params()


def _get_site_gate_secret() -> str:
    """Return the persistent secret used to build short-lived site gate codes."""

    return ensure_gate_access_secret()


def _get_site_gate_code(site_name: str) -> tuple[str, int]:
    """Return the current six-digit site gate code and minutes until refresh."""

    _get_site_gate_secret()
    return build_site_gate_access_code(
        site_name,
        slot_minutes=SITE_GATE_CODE_SLOT_MINUTES,
    )


def _validate_site_gate_code(site_name: str, submitted_code: str) -> bool:
    """Return True when the submitted fallback gate code is currently valid."""

    _get_site_gate_secret()
    return validate_site_gate_access_code(
        site_name,
        submitted_code,
        slot_minutes=SITE_GATE_CODE_SLOT_MINUTES,
        accepted_previous_slots=1,
    )


def _clear_kiosk_geofence_session_verification() -> None:
    """Drop any session-level kiosk verification state for the current browser session."""

    for key in (
        "site_attendance_geofence_verified_site",
        "site_attendance_geofence_verified_method",
        "site_attendance_geofence_verified_note",
        "site_attendance_geofence_verified_distance_meters",
        "site_attendance_geofence_verified_accuracy_meters",
    ):
        st.session_state.pop(key, None)


def _get_kiosk_geofence_session_verification(
    project_setup: ProjectSetup,
) -> Optional[Dict[str, Any]]:
    """Return the active kiosk verification state for this site, if one exists."""

    verified_site_name = str(
        st.session_state.get("site_attendance_geofence_verified_site", "") or ""
    ).strip()
    if not verified_site_name:
        return None
    if verified_site_name.casefold() != project_setup.current_site_name.casefold():
        _clear_kiosk_geofence_session_verification()
        return None
    return {
        "method": str(
            st.session_state.get("site_attendance_geofence_verified_method", "") or ""
        ).strip(),
        "note": str(
            st.session_state.get("site_attendance_geofence_verified_note", "") or ""
        ).strip(),
        "distance_meters": st.session_state.get(
            "site_attendance_geofence_verified_distance_meters"
        ),
        "accuracy_meters": st.session_state.get(
            "site_attendance_geofence_verified_accuracy_meters"
        ),
    }


def _set_kiosk_geofence_session_verification(
    project_setup: ProjectSetup,
    *,
    method: str,
    note: str,
    distance_meters: Optional[float],
    accuracy_meters: Optional[float],
) -> None:
    """Persist one successful kiosk verification in Streamlit session state."""

    st.session_state["site_attendance_geofence_verified_site"] = (
        project_setup.current_site_name
    )
    st.session_state["site_attendance_geofence_verified_method"] = method.strip()
    st.session_state["site_attendance_geofence_verified_note"] = note.strip()
    st.session_state["site_attendance_geofence_verified_distance_meters"] = (
        float(distance_meters) if distance_meters is not None else None
    )
    st.session_state["site_attendance_geofence_verified_accuracy_meters"] = (
        float(accuracy_meters) if accuracy_meters is not None else None
    )


def _format_kiosk_verification_message(verification_state: Mapping[str, Any]) -> str:
    """Return a human-readable success message for the current kiosk gate state."""

    method = str(verification_state.get("method") or "").strip().casefold()
    distance_meters = verification_state.get("distance_meters")
    accuracy_meters = verification_state.get("accuracy_meters")
    distance_suffix = (
        f" Distance {float(distance_meters):.0f}m."
        if isinstance(distance_meters, (int, float))
        else ""
    )
    accuracy_suffix = (
        f" GPS accuracy ±{float(accuracy_meters):.0f}m."
        if isinstance(accuracy_meters, (int, float))
        else ""
    )
    if method == "trusted_device":
        return (
            "✅ Gate already verified in this browser session. "
            "Using the last on-site GPS check."
            f"{distance_suffix}{accuracy_suffix}"
        )
    if method == "gate_code":
        return (
            "✅ Gate code accepted. The register is unlocked for this device session."
        )
    return f"✅ GPS Verified: You are on site.{distance_suffix}{accuracy_suffix}"


def _build_kiosk_geolocation_capture_path(
    *,
    kiosk_view: str,
    project_setup: ProjectSetup,
) -> str:
    """Return the top-level GPS capture page URL for kiosk attendance."""

    return_target = f"/?station=induction&mode=kiosk&kiosk_view={kiosk_view}"
    return_query = urlencode(
        {
            "v": "20260315c",
            "prefix": "geo",
            "return": return_target,
            "site_name": project_setup.current_site_name,
            "site_lat": f"{project_setup.site_latitude:.6f}",
            "site_lng": f"{project_setup.site_longitude:.6f}",
            "site_radius": str(int(project_setup.geofence_radius_meters)),
        }
    )
    return f"/gps/geo-capture.html?{return_query}"


def _build_kiosk_geolocation_capture_url(
    *,
    public_url: str,
    kiosk_view: str,
    project_setup: ProjectSetup,
) -> str:
    """Return an absolute GPS-capture URL for the current environment."""

    base_url = public_url.strip()
    if base_url and "://" not in base_url:
        base_url = f"https://{base_url}"
    if not base_url:
        base_url = get_site_induction_url()
    parsed_base_url = urlparse(base_url)
    normalized_base = parsed_base_url._replace(
        path="",
        params="",
        query="",
        fragment="",
    )
    return urlunparse(normalized_base) + _build_kiosk_geolocation_capture_path(
        kiosk_view=kiosk_view,
        project_setup=project_setup,
    )


def _build_project_setup_geolocation_capture_url(*, public_url: str) -> str:
    """Return an absolute GPS-capture URL for project setup."""

    base_url = public_url.strip()
    if base_url and "://" not in base_url:
        base_url = f"https://{base_url}"
    if not base_url:
        base_url = get_site_induction_url()
    parsed_base_url = urlparse(base_url)
    normalized_base = parsed_base_url._replace(
        path="",
        params="",
        query="",
        fragment="",
    )
    return (
        urlunparse(normalized_base)
        + "/gps/geo-capture.html?v=20260315c&prefix=setup_geo&return=%2F"
    )


def _sync_kiosk_query_params(*, kiosk_view: str) -> None:
    """Force the current URL back onto the locked kiosk induction route."""

    st.query_params["station"] = "induction"
    st.query_params["mode"] = "kiosk"
    st.query_params["kiosk_view"] = kiosk_view
    if "topic" in st.query_params:
        del st.query_params["topic"]


def _route_kiosk_to_induction_station(*, kiosk_view: str) -> None:
    """Force the app back onto the locked kiosk station and requested subview."""

    st.session_state["site_kiosk_lock"] = True
    st.session_state["is_kiosk"] = True
    st.session_state["site_kiosk_active_view"] = kiosk_view
    st.session_state["active_file_station"] = next(
        station.label for station in FILE_STATIONS if station.number == "INDUCTION"
    )
    _sync_kiosk_query_params(kiosk_view=kiosk_view)


def main() -> None:
    """Render the Streamlit portal."""

    st.set_page_config(
        page_title=SITE_TITLE,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_styles()
    _apply_site_diary_context_query_params()
    project_setup = _synchronise_public_tunnel_settings(_get_project_setup())
    project_setup = _apply_project_setup_geolocation_query_payload(project_setup)
    _apply_site_diary_dictation_query_payload(project_setup.current_site_name)
    repository = _build_repository()
    requested_station_label = _get_station_label_from_query_params()
    requested_kiosk_view = _get_kiosk_view_from_query_params()
    kiosk_active_view = (
        requested_kiosk_view
        or str(st.session_state.get("site_kiosk_active_view", "")).strip().lower()
    )
    kiosk_lock_active = bool(st.session_state.get("site_kiosk_lock", False))
    is_kiosk = _is_kiosk_mode_requested() or kiosk_lock_active or kiosk_active_view in {
        "attendance",
        "induction",
    }
    is_tbt_kiosk = _is_tbt_kiosk_requested()
    st.session_state["is_kiosk"] = is_kiosk or is_tbt_kiosk
    if is_kiosk:
        st.session_state["site_kiosk_lock"] = True
        if kiosk_active_view not in {"attendance", "induction"}:
            kiosk_active_view = "attendance"
        st.session_state["site_kiosk_active_view"] = kiosk_active_view
        if (
            not _is_kiosk_mode_requested()
            or requested_station_label
            != next(
                station.label
                for station in FILE_STATIONS
                if station.number == "INDUCTION"
            )
            or requested_kiosk_view != kiosk_active_view
        ):
            _sync_kiosk_query_params(kiosk_view=kiosk_active_view)
    else:
        st.session_state.pop("site_kiosk_lock", None)
    if is_tbt_kiosk:
        _render_toolbox_talk_kiosk(
            repository,
            project_setup,
            topic=_get_tbt_topic_from_query_params(),
        )
        return
    if (
        "active_file_station" not in st.session_state
        or st.session_state["active_file_station"]
        not in {station.label for station in FILE_STATIONS}
    ):
        st.session_state["active_file_station"] = DEFAULT_FILE_STATION_LABEL
    if is_kiosk:
        st.session_state["active_file_station"] = next(
            station.label for station in FILE_STATIONS if station.number == "INDUCTION"
        )
    elif requested_station_label is not None:
        st.session_state["active_file_station"] = requested_station_label
    active_station_label = str(st.session_state["active_file_station"])

    if not is_kiosk:
        with st.sidebar:
            _render_sidebar(repository, active_station_label, project_setup)
        _inject_sidebar_reopen_bridge(enabled=True)
    else:
        _inject_sidebar_reopen_bridge(enabled=False)

    if not is_kiosk:
        active_station_label = _render_file_station_navigation()
    _render_active_station(
        repository,
        active_station_label,
        project_setup,
        is_kiosk=is_kiosk,
    )


def _build_repository() -> DocumentRepository:
    """Return a repository bound to the configured workspace database."""

    repository = DocumentRepository(DATABASE_PATH)
    repository.create_schema()
    return repository


def _inject_styles() -> None:
    """Apply the Lovedean portal styling."""

    sidebar_width_rem = float(st.session_state.get("sidebar_width_rem", 24))
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
                transition:
                    box-shadow 180ms ease,
                    border-color 180ms ease,
                    min-width 180ms ease,
                    max-width 180ms ease;
            }}
            section[data-testid="stSidebar"][aria-expanded="false"] {{
                background: transparent !important;
                border-right: none !important;
                box-shadow: none !important;
            }}
            section[data-testid="stSidebar"] * {{
                color: {TEXT_DARK};
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {{
                padding-top: 0.35rem;
                padding-bottom: 1.25rem;
            }}
            [data-testid="stSidebarCollapseButton"],
            [data-testid="collapsedControl"] {{
                align-items: center !important;
                backdrop-filter: blur(10px) !important;
                background: rgba(255, 255, 255, 0.96) !important;
                border: 1px solid #d9dde5 !important;
                border-radius: 999px !important;
                box-shadow: 0 10px 24px rgba(18, 24, 38, 0.12) !important;
                color: {TEXT_DARK} !important;
                display: inline-flex !important;
                height: 2.5rem !important;
                justify-content: center !important;
                -webkit-text-fill-color: {TEXT_DARK} !important;
                opacity: 1 !important;
                padding: 0 !important;
                transition:
                    transform 160ms ease,
                    box-shadow 160ms ease,
                    border-color 160ms ease,
                    background-color 160ms ease !important;
                width: 2.5rem !important;
                z-index: 10000 !important;
            }}
            [data-testid="collapsedControl"] {{
                left: 0.9rem !important;
                margin: 0 !important;
                position: fixed !important;
                top: 0.95rem !important;
            }}
            [data-testid="stSidebarCollapseButton"]:hover,
            [data-testid="collapsedControl"]:hover {{
                background: #ffffff !important;
                border-color: rgba(209, 34, 142, 0.42) !important;
                box-shadow: 0 14px 28px rgba(18, 24, 38, 0.16) !important;
                transform: translateY(-1px) !important;
            }}
            [data-testid="stSidebarCollapseButton"] svg,
            [data-testid="collapsedControl"] svg,
            [data-testid="stSidebarCollapseButton"] path,
            [data-testid="collapsedControl"] path {{
                color: {TEXT_DARK} !important;
                fill: {TEXT_DARK} !important;
                stroke: {TEXT_DARK} !important;
                -webkit-text-fill-color: {TEXT_DARK} !important;
            }}
            [data-testid="stSidebarCollapseButton"] svg,
            [data-testid="collapsedControl"] svg {{
                height: 1rem !important;
                width: 1rem !important;
            }}
            @media (min-width: 768px) {{
                section[data-testid="stSidebar"][aria-expanded="true"] {{
                    min-width: {sidebar_width_rem:.1f}rem !important;
                    max-width: {sidebar_width_rem:.1f}rem !important;
                }}
            }}
            @media (max-width: 767.98px) {{
                section[data-testid="stSidebar"] {{
                    min-width: auto !important;
                    max-width: none !important;
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
                background-color: #ffffff !important;
                color: #31333F !important;
                -webkit-text-fill-color: #31333F !important;
                border: 1.5px solid #d7dde8 !important;
                border-radius: 12px !important;
                box-shadow: 0 1px 2px rgba(18, 24, 38, 0.04) !important;
                color-scheme: light !important;
                background-clip: padding-box !important;
                -webkit-box-shadow: 0 0 0 1000px #ffffff inset !important;
                box-shadow: inset 0 0 0 1000px #ffffff !important;
                filter: none !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stTextInput"] input,
            [data-testid="stAppViewContainer"] > .main div[data-testid="stTextInput"] > div > div input,
            [data-testid="stAppViewContainer"] > .main input[type="text"],
            [data-testid="stAppViewContainer"] > .main input[type="search"],
            [data-testid="stAppViewContainer"] > .main textarea,
            section[data-testid="stSidebar"] input[type="text"],
            section[data-testid="stSidebar"] input[type="search"],
            section[data-testid="stSidebar"] textarea {{
                font-size: 1rem !important;
                line-height: 1.5 !important;
                font-weight: 600 !important;
                padding: 0.78rem 0.95rem !important;
                letter-spacing: 0.01em !important;
            }}
            [data-testid="stAppViewContainer"] > .main textarea,
            section[data-testid="stSidebar"] textarea {{
                min-height: 8rem !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-baseweb="input"] > div:focus-within,
            [data-testid="stAppViewContainer"] > .main div[data-baseweb="base-input"] > div:focus-within,
            [data-testid="stAppViewContainer"] > .main textarea:focus,
            section[data-testid="stSidebar"] div[data-baseweb="input"] > div:focus-within,
            section[data-testid="stSidebar"] div[data-baseweb="base-input"] > div:focus-within,
            section[data-testid="stSidebar"] textarea:focus {{
                border-color: rgba(209, 34, 142, 0.85) !important;
                box-shadow: 0 0 0 0.2rem rgba(209, 34, 142, 0.12) !important;
            }}
            [data-testid="stAppViewContainer"] > .main input::placeholder,
            [data-testid="stAppViewContainer"] > .main textarea::placeholder,
            section[data-testid="stSidebar"] input::placeholder,
            section[data-testid="stSidebar"] textarea::placeholder {{
                color: #8a94a6 !important;
                opacity: 1 !important;
                font-weight: 500 !important;
            }}
            [data-testid="stAppViewContainer"] label,
            [data-testid="stAppViewContainer"] label p,
            [data-testid="stAppViewContainer"] div[data-testid="stMarkdownContainer"] label,
            section[data-testid="stSidebar"] label,
            section[data-testid="stSidebar"] label p {{
                color: #344054 !important;
                font-weight: 700 !important;
                letter-spacing: 0.01em !important;
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
                background: linear-gradient(180deg, rgba(255,255,255,0.98), rgba(250,251,253,0.96)) !important;
                border: 1px solid #e6eaf1 !important;
                border-radius: 16px !important;
                box-shadow: 0 10px 24px rgba(18, 24, 38, 0.05) !important;
                color-scheme: light !important;
                margin-top: 0.9rem;
                margin-bottom: 0.25rem;
                overflow: hidden;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] > details,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] {{
                background: #ffffff !important;
                color-scheme: light !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"][open],
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"]:has(details[open]) {{
                border-color: rgba(209, 34, 142, 0.18) !important;
                box-shadow: 0 14px 30px rgba(18, 24, 38, 0.08) !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary {{
                background:
                    radial-gradient(circle at top left, rgba(209, 34, 142, 0.08), transparent 34%),
                    radial-gradient(circle at top right, rgba(91, 141, 239, 0.08), transparent 34%),
                    linear-gradient(180deg, rgba(255,255,255,0.98), rgba(246,248,251,0.98)) !important;
                border-bottom: 1px solid #e8ebf1 !important;
                color: {TEXT_DARK} !important;
                color-scheme: light !important;
                min-height: 3.25rem !important;
                padding: 0.7rem 0.9rem !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary:hover,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary:focus,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary:hover,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary:focus {{
                background:
                    radial-gradient(circle at top left, rgba(209, 34, 142, 0.11), transparent 36%),
                    radial-gradient(circle at top right, rgba(91, 141, 239, 0.11), transparent 36%),
                    linear-gradient(180deg, rgba(255,255,255,1), rgba(243,246,251,1)) !important;
                color: {TEXT_DARK} !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary *,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary * {{
                background-color: transparent !important;
                color: {TEXT_DARK} !important;
                -webkit-text-fill-color: {TEXT_DARK} !important;
                opacity: 1 !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary p,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary p {{
                color: {TEXT_DARK} !important;
                font-size: 0.97rem;
                font-weight: 900;
                letter-spacing: -0.01em;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary svg,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] summary path,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary svg,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] summary path {{
                color: {TEXT_MUTED} !important;
                fill: {TEXT_MUTED} !important;
                stroke: {TEXT_MUTED} !important;
            }}
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] div[data-testid="stExpander"] .streamlit-expanderContent,
            section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] details[data-testid="stExpander"] .streamlit-expanderContent {{
                background: #ffffff !important;
                border: none;
                color-scheme: light !important;
                padding: 0.9rem 0.95rem 1rem !important;
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
            .sidebar-tool-card {{
                background: linear-gradient(180deg, rgba(255,255,255,0.96) 0%, rgba(250,250,252,0.96) 100%);
                border: 1px solid #e8ebf1;
                border-radius: 14px;
                box-shadow: 0 8px 18px rgba(18, 24, 38, 0.05);
                padding: 0.9rem 0.95rem;
                margin-top: 0.75rem;
                margin-bottom: 0.9rem;
            }}
            .sidebar-section-card {{
                background: rgba(255, 255, 255, 0.88);
                border: 1px solid #eceef2;
                border-radius: 16px;
                box-shadow: 0 8px 20px rgba(18, 24, 38, 0.04);
                margin-top: 0.9rem;
                margin-bottom: 1rem;
                padding: 0.95rem 1rem;
            }}
            .sidebar-section-title {{
                color: {TEXT_DARK};
                font-size: 0.88rem;
                font-weight: 900;
                letter-spacing: 0.02em;
                margin-bottom: 0.2rem;
            }}
            .sidebar-section-copy {{
                color: {TEXT_MUTED};
                font-size: 0.87rem;
                line-height: 1.5;
                margin-bottom: 0.75rem;
            }}
            .sidebar-tool-title {{
                color: {TEXT_DARK};
                font-size: 0.92rem;
                font-weight: 800;
                margin-bottom: 0.28rem;
            }}
            .sidebar-tool-copy {{
                color: {TEXT_MUTED};
                font-size: 0.88rem;
                line-height: 1.5;
            }}
            .form-section-kicker {{
                color: #5c6884;
                font-size: 0.74rem;
                font-weight: 800;
                letter-spacing: 0.12em;
                margin-bottom: 0.35rem;
                text-transform: uppercase;
            }}
            .form-section-copy {{
                color: {TEXT_MUTED};
                font-size: 0.92rem;
                line-height: 1.5;
                margin-bottom: 0.8rem;
            }}
            div[data-testid="stCodeBlock"] {{
                border: 1px solid #e3e7ee !important;
                border-radius: 14px !important;
                overflow: hidden !important;
                box-shadow: 0 8px 20px rgba(18, 24, 38, 0.05) !important;
            }}
            div[data-testid="stCodeBlock"] pre {{
                font-size: 0.95rem !important;
                line-height: 1.55 !important;
                padding: 0.9rem 1rem !important;
            }}
            div[data-baseweb="tab-list"] {{
                gap: 0.55rem !important;
                margin-bottom: 0.9rem !important;
            }}
            button[role="tab"] {{
                background: rgba(255,255,255,0.92) !important;
                border: 1px solid #dde3ec !important;
                border-radius: 999px !important;
                box-shadow: 0 4px 12px rgba(18, 24, 38, 0.04) !important;
                color: #344054 !important;
                font-weight: 800 !important;
                min-height: 2.6rem !important;
                padding: 0.45rem 0.95rem !important;
            }}
            button[role="tab"][aria-selected="true"] {{
                background: linear-gradient(135deg, rgba(209,34,142,0.10), rgba(91,141,239,0.12)) !important;
                border-color: rgba(209, 34, 142, 0.45) !important;
                color: {TEXT_DARK} !important;
            }}
            .stProgress > div > div > div > div {{
                background-color: {UPLANDS_PINK};
            }}
            .site-induction-binary-label {{
                margin: 0.2rem 0 0.3rem;
                color: {TEXT_DARK};
                font-size: 0.98rem;
                font-weight: 800;
                line-height: 1.35;
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
                align-items: stretch;
                background:
                    radial-gradient(circle at top left, rgba(209, 34, 142, 0.11), transparent 34%),
                    radial-gradient(circle at top right, rgba(91, 141, 239, 0.11), transparent 30%),
                    linear-gradient(#ffffff, #ffffff) padding-box,
                    linear-gradient(135deg, rgba(209, 34, 142, 0.88), rgba(91, 141, 239, 0.88)) border-box;
                border: 1px solid transparent;
                border-radius: 20px;
                box-shadow: 0 18px 38px rgba(18, 24, 38, 0.07);
                display: grid;
                gap: 1rem;
                grid-template-columns: auto 1fr;
                margin-bottom: 1.25rem;
                padding: 1.35rem 1.45rem;
            }}
            .station-hero-icon {{
                align-items: center;
                background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,250,252,0.94));
                border: 1px solid rgba(233, 236, 243, 0.95);
                border-radius: 20px;
                box-shadow: 0 12px 24px rgba(18, 24, 38, 0.06);
                color: {TEXT_DARK};
                display: inline-flex;
                font-size: 2rem;
                height: 4.6rem;
                justify-content: center;
                width: 4.6rem;
            }}
            .station-hero-copy {{
                min-width: 0;
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
                margin-bottom: 0.1rem;
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
            .workspace-hero {{
                background:
                    radial-gradient(circle at top left, rgba(209, 34, 142, 0.10), transparent 38%),
                    radial-gradient(circle at top right, rgba(91, 141, 239, 0.10), transparent 32%),
                    linear-gradient(180deg, rgba(255,255,255,0.98), rgba(252,250,252,0.98));
                border: 1px solid #e8ebf1;
                border-radius: 18px;
                box-shadow: 0 14px 32px rgba(18, 24, 38, 0.06);
                margin-bottom: 1rem;
                padding: 1.15rem 1.2rem;
            }}
            .workspace-hero-top {{
                align-items: center;
                display: flex;
                gap: 0.8rem;
                margin-bottom: 0.65rem;
            }}
            .workspace-hero-icon {{
                align-items: center;
                background: rgba(255,255,255,0.88);
                border: 1px solid #eceff4;
                border-radius: 16px;
                display: inline-flex;
                font-size: 1.35rem;
                height: 3rem;
                justify-content: center;
                width: 3rem;
            }}
            .workspace-hero-kicker {{
                color: #5c6884;
                font-size: 0.76rem;
                font-weight: 800;
                letter-spacing: 0.14em;
                margin-bottom: 0.18rem;
                text-transform: uppercase;
            }}
            .workspace-hero-title {{
                color: {TEXT_DARK};
                font-size: 1.35rem;
                font-weight: 900;
                letter-spacing: -0.03em;
                line-height: 1.08;
            }}
            .workspace-hero-caption {{
                color: {TEXT_MUTED};
                font-size: 0.96rem;
                line-height: 1.55;
            }}
            .broadcast-status-badge-row {{
                display: flex;
                flex-wrap: wrap;
                gap: 0.55rem;
                margin: 0.2rem 0 0.95rem 0;
            }}
            .broadcast-status-badge {{
                align-items: center;
                border-radius: 999px;
                display: inline-flex;
                font-size: 0.84rem;
                font-weight: 800;
                letter-spacing: 0.01em;
                padding: 0.45rem 0.8rem;
            }}
            .broadcast-status-badge-success {{
                background: rgba(22, 163, 74, 0.12);
                border: 1px solid rgba(22, 163, 74, 0.22);
                color: #15803d;
            }}
            .broadcast-status-badge-warning {{
                background: rgba(245, 158, 11, 0.14);
                border: 1px solid rgba(245, 158, 11, 0.24);
                color: #b45309;
            }}
            .broadcast-status-badge-danger {{
                background: rgba(217, 45, 32, 0.11);
                border: 1px solid rgba(217, 45, 32, 0.2);
                color: #b42318;
            }}
            .broadcast-status-badge-neutral {{
                background: rgba(91, 141, 239, 0.1);
                border: 1px solid rgba(91, 141, 239, 0.18);
                color: #2456c7;
            }}
            .workspace-gap {{
                height: 0.45rem;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stExpander"] .streamlit-expanderContent,
            [data-testid="stAppViewContainer"] > .main details[data-testid="stExpander"] .streamlit-expanderContent {{
                padding: 0.55rem 0.15rem 0.2rem 0.15rem !important;
            }}
            [data-testid="stAppViewContainer"] > .main div[data-testid="stVerticalBlock"] > div:has(> div[data-testid="stExpander"]) {{
                margin-top: 0.45rem;
                margin-bottom: 0.45rem;
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
                margin-top: 1rem;
                margin-bottom: 0.55rem;
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
                margin-top: 0.7rem;
            }}
            .sidebar-group-divider {{
                background: linear-gradient(90deg, rgba(209, 34, 142, 0.08), rgba(91, 141, 239, 0.08));
                border: 1px solid rgba(223, 227, 234, 0.92);
                border-radius: 999px;
                height: 0.4rem;
                margin: 0.95rem 0 1rem;
            }}
            .station-nav-group-title {{
                color: {TEXT_MUTED};
                font-size: 0.78rem;
                font-weight: 800;
                letter-spacing: 0.14em;
                margin-bottom: 0.55rem;
                text-transform: uppercase;
            }}
            .station-nav-group-copy {{
                color: {TEXT_MUTED};
                font-size: 0.92rem;
                line-height: 1.5;
                margin-bottom: 0.85rem;
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
            .dispatch-audit-card {{
                background: #ffffff;
                border: 1px solid {CARD_BORDER};
                border-radius: 16px;
                box-shadow: 0 8px 18px rgba(18, 24, 38, 0.06);
                padding: 1rem 1.05rem;
                min-height: 140px;
            }}
            .dispatch-audit-label {{
                color: {TEXT_MUTED};
                font-size: 0.76rem;
                font-weight: 800;
                letter-spacing: 0.08em;
                margin-bottom: 0.7rem;
                text-transform: uppercase;
            }}
            .dispatch-audit-value {{
                color: {TEXT_DARK};
                font-size: 1.9rem;
                font-weight: 800;
                line-height: 1.18;
                margin-bottom: 0.45rem;
                word-break: break-word;
            }}
            .dispatch-audit-value-compact {{
                font-size: 1.38rem;
                line-height: 1.28;
            }}
            .dispatch-audit-copy {{
                color: {TEXT_MUTED};
                font-size: 0.9rem;
                line-height: 1.45;
            }}
            .dispatch-message-box {{
                background: #f8fafc;
                border: 1px solid {CARD_BORDER};
                border-radius: 14px;
                color: {TEXT_DARK};
                font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
                font-size: 0.96rem;
                line-height: 1.55;
                padding: 1rem;
                white-space: pre-wrap;
                word-break: break-word;
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


def _inject_sidebar_reopen_bridge(*, enabled: bool) -> None:
    """Keep a reliable floating reopen control available for the manager sidebar."""

    components.html(
        f"""
        <script>
        (function() {{
            const enabled = {str(enabled).lower()};
            const doc = window.parent.document;
            const buttonId = "uplands-sidebar-reopen";
            const styleId = "uplands-sidebar-reopen-style";

            function ensureStyle() {{
                if (doc.getElementById(styleId)) return;
                const style = doc.createElement("style");
                style.id = styleId;
                style.textContent = `
                    #${{buttonId}} {{
                        align-items: center;
                        backdrop-filter: blur(14px);
                        background: rgba(255, 255, 255, 0.97);
                        border: 1px solid rgba(217, 221, 229, 0.96);
                        border-radius: 999px;
                        box-shadow: 0 14px 30px rgba(18, 24, 38, 0.14);
                        color: {TEXT_DARK};
                        cursor: pointer;
                        display: none;
                        font: 800 0.94rem/1 "Avenir Next", "Segoe UI", sans-serif;
                        gap: 0.45rem;
                        left: 0.95rem;
                        min-height: 2.6rem;
                        padding: 0.65rem 0.95rem;
                        position: fixed;
                        top: 0.95rem;
                        transition: transform 160ms ease, box-shadow 160ms ease, border-color 160ms ease;
                        z-index: 10001;
                    }}
                    #${{buttonId}}:hover {{
                        border-color: rgba(209, 34, 142, 0.42);
                        box-shadow: 0 18px 36px rgba(18, 24, 38, 0.18);
                        transform: translateY(-1px);
                    }}
                    #${{buttonId}} .uplands-sidebar-reopen-arrow {{
                        font-size: 1rem;
                        line-height: 1;
                    }}
                `;
                doc.head.appendChild(style);
            }}

            function ensureButton() {{
                let button = doc.getElementById(buttonId);
                if (!button) {{
                    button = doc.createElement("button");
                    button.id = buttonId;
                    button.type = "button";
                    button.innerHTML = '<span class="uplands-sidebar-reopen-arrow">☰</span><span>Show Sidebar</span>';
                    button.addEventListener("click", function() {{
                        const nativeControl =
                            doc.querySelector('[data-testid="stExpandSidebarButton"]') ||
                            doc.querySelector('[data-testid="collapsedControl"]') ||
                            doc.querySelector('[data-testid="stSidebarCollapseButton"]') ||
                            doc.querySelector('button[data-testid="stExpandSidebarButton"]') ||
                            doc.querySelector('button[aria-label*="sidebar" i]') ||
                            doc.querySelector('button[title*="sidebar" i]');
                        if (nativeControl) {{
                            nativeControl.dispatchEvent(
                                new MouseEvent("click", {{
                                    bubbles: true,
                                    cancelable: true,
                                    view: window.parent,
                                }})
                            );
                        }}
                    }});
                    doc.body.appendChild(button);
                }}
                return button;
            }}

            function update() {{
                const button = ensureButton();
                if (!enabled) {{
                    button.style.display = "none";
                    return;
                }}
                const sidebar = doc.querySelector('section[data-testid="stSidebar"]');
                const isCollapsed = !!sidebar && sidebar.getAttribute("aria-expanded") === "false";
                button.style.display = isCollapsed ? "inline-flex" : "none";
            }}

            ensureStyle();
            update();
            const observer = new MutationObserver(update);
            observer.observe(doc.body, {{ childList: true, subtree: true, attributes: true }});
            window.addEventListener("beforeunload", function() {{
                observer.disconnect();
            }});
            window.setTimeout(update, 150);
            window.setTimeout(update, 600);
            window.setTimeout(update, 1200);
        }})();
        </script>
        """,
        height=0,
        width=0,
    )


def _render_sidebar(
    repository: DocumentRepository,
    station_label: str,
    project_setup: ProjectSetup,
) -> None:
    """Render the branded sidebar and sync controls."""

    station = _get_file_station(station_label)
    if UPLANDS_LOGO.exists():
        st.image(str(UPLANDS_LOGO), width=220)
    if NATIONAL_GRID_LOGO.exists():
        st.image(str(NATIONAL_GRID_LOGO), width=120)

    _render_sidebar_overview_card(station, project_setup)

    st.markdown(
        (
            "<div class='sidebar-section-card'>"
            "<div class='sidebar-section-title'>Workspace Operations</div>"
            "<div class='sidebar-section-copy'>"
            "Run intake, check the live workspace state, and keep the command centre aligned before moving into station-specific work."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    if st.button("🔄 SYNC WORKSPACE", width="stretch"):
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

    st.markdown(
        (
            "<div class='sidebar-section-card'>"
            "<div class='sidebar-section-title'>Workspace Status</div>"
            "<div class='sidebar-section-copy'>"
            "A quick sense check that each live file station is available before you drill into the detail."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
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

    st.markdown("<div class='sidebar-group-divider'></div>", unsafe_allow_html=True)

    _render_sidebar_project_setup(project_setup)
    _render_sidebar_view_settings()
    _render_sidebar_tunnel_status(project_setup)
    _render_workspace_doctor()

    st.markdown(
        (
            "<div class='sidebar-section-card'>"
            "<div class='sidebar-section-title'>Station Tools</div>"
            "<div class='sidebar-section-copy'>"
            "These controls change with the active workspace, so the left rail stays focused on the task you are doing right now."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
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
    elif station.number == "BROADCAST":
        _render_sidebar_site_broadcast_quick_actions(repository, project_setup)
    elif station.number == "INDUCTION":
        _render_sidebar_site_induction_quick_actions(project_setup)
    else:
        _render_sidebar_file_2_quick_actions(repository, project_setup)


def _render_sidebar_overview_card(
    station: FileStation,
    project_setup: ProjectSetup,
) -> None:
    """Render a compact manager overview card at the top of the sidebar."""

    project_bits = [
        project_setup.current_site_name or "Site not set",
        f"Client: {project_setup.client_name}" if project_setup.client_name else "",
        f"Job: {project_setup.job_number}" if project_setup.job_number else "",
    ]
    project_meta = " | ".join(bit for bit in project_bits if bit)
    st.markdown(
        (
            "<div class='sidebar-tool-card'>"
            "<div class='sidebar-tool-title'>Manager Console</div>"
            f"<div class='sidebar-tool-copy'>Active workspace: <strong>{html.escape(station.number)}</strong> · "
            f"{html.escape(station.title)}</div>"
            f"<div class='sidebar-tool-copy'>{html.escape(project_meta)}</div>"
            "<div class='sidebar-tool-copy'>"
            "Use the sidebar for global controls only. Station-specific actions live inside each page so the workflow stays easier to follow."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_sidebar_view_settings() -> None:
    """Render sidebar layout controls for manager mode."""

    if st.session_state.pop("sidebar_width_reset_pending", False):
        st.session_state["sidebar_width_rem"] = 24

    with st.expander("View Settings", expanded=False):
        st.caption(
            "Adjust the sidebar width for admin work. Collapse and re-open now use the cleaner native sidebar behavior."
        )
        sidebar_width = st.slider(
            "Sidebar Width",
            min_value=20,
            max_value=32,
            value=int(round(float(st.session_state.get("sidebar_width_rem", 24)))),
            step=1,
            key="sidebar_width_rem",
        )
        st.caption(f"Current width: {sidebar_width}rem")
        if st.button(
            "↺ Reset Sidebar Width",
            key="reset_sidebar_width",
            width="stretch",
            type="secondary",
        ):
            st.session_state["sidebar_width_reset_pending"] = True
            st.rerun()


def _render_sidebar_project_setup(project_setup: ProjectSetup) -> None:
    """Render the persisted project metadata form."""

    _flush_project_setup_postcode_state_clear()

    with st.expander("Project Setup", expanded=False):
        st.markdown(
            (
                "<div class='sidebar-tool-card'>"
                "<div class='sidebar-tool-title'>Live Project Controls</div>"
                "<div class='sidebar-tool-copy'>"
                "This is the operational source of truth for the active site, tunnel, and geo-fence. "
                "Use the quick setup tools below when the job moves."
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        flash_message = st.session_state.pop("project_setup_flash", None)
        flash_level = str(st.session_state.pop("project_setup_flash_level", "success"))
        if flash_message is not None:
            if flash_level == "error":
                st.error(flash_message)
            elif flash_level == "warning":
                st.warning(flash_message)
            elif flash_level == "info":
                st.info(flash_message)
            else:
                st.success(flash_message)

        with st.form("project_setup_form", clear_on_submit=False):
            st.markdown("<div class='form-section-kicker'>Project Identity</div>", unsafe_allow_html=True)
            st.markdown(
                "<div class='form-section-copy'>Set the live site details that drive the register headers, poster text, and document outputs.</div>",
                unsafe_allow_html=True,
            )
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
            public_tunnel_url = st.text_input(
                "Public Tunnel URL",
                value=project_setup.public_tunnel_url,
                help="Optional public URL for Cloudflare Tunnel or Ngrok. The induction poster QR will use this when set.",
            )
            st.markdown("<div class='form-section-kicker'>Geo-Fence</div>", unsafe_allow_html=True)
            st.markdown(
                "<div class='form-section-copy'>These coordinates define where operatives are allowed to sign in. They can be set manually or by the quick tools below.</div>",
                unsafe_allow_html=True,
            )
            geofence_columns = st.columns(3, gap="small")
            with geofence_columns[0]:
                site_latitude = st.number_input(
                    "Site Latitude",
                    value=float(project_setup.site_latitude),
                    format="%.6f",
                    help="Change this when the project moves to a different site.",
                )
            with geofence_columns[1]:
                site_longitude = st.number_input(
                    "Site Longitude",
                    value=float(project_setup.site_longitude),
                    format="%.6f",
                    help="Change this when the project moves to a different site.",
                )
            with geofence_columns[2]:
                geofence_radius_meters = st.number_input(
                    "Geo-Fence Radius (m)",
                    min_value=1,
                    value=int(project_setup.geofence_radius_meters),
                    step=25,
                    help="Operatives must be within this distance to sign in or out.",
                )
            submitted = st.form_submit_button(
                "💾 Save Project Setup",
                width="stretch",
            )

        if submitted:
            saved_setup = ProjectSetup(
                current_site_name=current_site_name.strip() or PROJECT_NAME,
                job_number=job_number.strip(),
                site_address=site_address.strip(),
                client_name=client_name.strip(),
                public_tunnel_url=public_tunnel_url.strip(),
                site_latitude=float(site_latitude),
                site_longitude=float(site_longitude),
                geofence_radius_meters=max(1, int(geofence_radius_meters)),
                known_sites=project_setup.known_sites,
            )
            saved_setup = _save_project_setup(saved_setup)
            save_app_settings(public_tunnel_url=saved_setup.public_tunnel_url)
            st.session_state["project_setup"] = saved_setup
            st.session_state["project_setup_flash"] = "Project setup saved."
            st.session_state["project_setup_flash_level"] = "success"
            st.rerun()

        st.markdown("<div class='form-section-kicker'>Quick Geo-Fence Setup</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='form-section-copy'>Use your current device location on site, or resolve a postcode before you arrive.</div>",
            unsafe_allow_html=True,
        )
        st.link_button(
            "📍 Use Current Device Location",
            _build_project_setup_geolocation_capture_url(
                public_url=project_setup.public_tunnel_url
            ),
            type="secondary",
            width="stretch",
        )
        st.markdown(
            (
                "<div class='sidebar-tool-card'>"
                "<div class='sidebar-tool-title'>Current Geo-Fence</div>"
                "<div class='sidebar-tool-copy'>"
                f"Radius: <strong>{project_setup.geofence_radius_meters}m</strong><br>"
                f"Lat: {project_setup.site_latitude:.5f}<br>"
                f"Lng: {project_setup.site_longitude:.5f}"
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

        with st.form("project_setup_postcode_lookup_form", clear_on_submit=False):
            st.markdown(
                "<div class='form-section-kicker'>Find Site by Postcode</div>",
                unsafe_allow_html=True,
            )
            postcode_lookup = st.text_input(
                "Search UK postcode",
                key="project_setup_postcode_lookup",
                placeholder="e.g. CF44 9TZ",
                help=(
                    "We will resolve the postcode first, then let you pick a known site "
                    "or type the building / unit once before applying it."
                ),
            )
            postcode_submitted = st.form_submit_button(
                "🔎 Search Postcode",
                width="stretch",
            )

        extracted_site_postcode = _extract_uk_postcode(project_setup.site_address)
        if extracted_site_postcode:
            st.caption(f"Detected site postcode: {extracted_site_postcode}")
            if st.button(
                "📍 Use Postcode from Site Address",
                key="project_setup_use_site_address_postcode",
                width="stretch",
                type="secondary",
            ):
                postcode_result = lookup_uk_postcode_details(extracted_site_postcode)
                if postcode_result is None:
                    st.session_state["project_setup_flash"] = (
                        f"Could not resolve postcode from Site Address: {extracted_site_postcode}"
                    )
                    st.session_state["project_setup_flash_level"] = "warning"
                    st.rerun()

                st.session_state["project_setup_postcode_result"] = postcode_result
                prefilled_site_name = _strip_uk_postcode(project_setup.site_address)
                if not prefilled_site_name:
                    prefilled_site_name = project_setup.current_site_name
                st.session_state["project_setup_postcode_site_name"] = prefilled_site_name
                st.session_state["project_setup_flash"] = (
                    f"Postcode resolved: {postcode_result['formatted_address']}"
                )
                st.session_state["project_setup_flash_level"] = "info"
                st.rerun()

        if postcode_submitted:
            postcode_result = lookup_uk_postcode_details(postcode_lookup)
            if postcode_result is None:
                st.session_state["project_setup_flash"] = (
                    "Postcode lookup failed. Check the postcode and try again."
                )
                st.session_state["project_setup_flash_level"] = "warning"
                _clear_project_setup_postcode_state()
                st.rerun()

            st.session_state["project_setup_postcode_result"] = postcode_result
            st.session_state["project_setup_postcode_site_name"] = ""
            st.session_state["project_setup_flash"] = (
                f"Postcode resolved: {postcode_result['formatted_address']}"
            )
            st.session_state["project_setup_flash_level"] = "info"
            st.rerun()

        postcode_result = st.session_state.get("project_setup_postcode_result")
        if isinstance(postcode_result, dict) and postcode_result.get("postcode"):
            normalized_postcode = str(postcode_result.get("postcode") or "").strip()
            matching_known_sites = _matching_known_sites_for_postcode(
                project_setup,
                normalized_postcode,
            )
            st.markdown(
                "<div class='form-section-kicker'>Resolve This Site</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                (
                    "<div class='form-section-copy'>"
                    "We found the postcode. Choose a remembered site at this postcode, "
                    "or type the site / building name once and we will fill the rest of the address for you."
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            st.markdown(
                (
                    "<div class='sidebar-tool-card'>"
                    "<div class='sidebar-tool-title'>Resolved Postcode</div>"
                    "<div class='sidebar-tool-copy'>"
                    f"{html.escape(str(postcode_result.get('formatted_address') or normalized_postcode))}<br>"
                    f"Lat: {float(postcode_result.get('latitude') or 0.0):.5f}<br>"
                    f"Lng: {float(postcode_result.get('longitude') or 0.0):.5f}"
                    "</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            if matching_known_sites:
                st.info(
                    f"We already remember {len(matching_known_sites)} saved site"
                    f"{'' if len(matching_known_sites) == 1 else 's'} at {normalized_postcode}. "
                    "Pick one below or name a new building if this is a different compound."
                )
            else:
                st.info(
                    "This postcode is new to the command centre. Name the site or building once and "
                    "we will remember it for future site moves."
                )

            resolution_options = ["🏢 New Site / Building"]
            resolution_lookup: Dict[str, Optional[SavedSiteProfile]] = {
                "🏢 New Site / Building": None
            }
            for profile in matching_known_sites:
                option_label = f"🗂️ {profile.label}"
                resolution_options.append(option_label)
                resolution_lookup[option_label] = profile

            resolution_choice_key = "project_setup_postcode_resolution_choice"
            if (
                resolution_choice_key not in st.session_state
                or st.session_state[resolution_choice_key] not in resolution_lookup
            ):
                st.session_state[resolution_choice_key] = "🏢 New Site / Building"

            selected_resolution_label = st.selectbox(
                "Select exact site",
                options=resolution_options,
                key=resolution_choice_key,
                help=(
                    "If you have already used this postcode before, pick the saved site. "
                    "Otherwise choose New Site / Building and name it once."
                ),
            )
            selected_profile = resolution_lookup[selected_resolution_label]

            clear_postcode_search = False
            if selected_profile is None:
                site_name_input = st.text_input(
                    "Site / Building Name",
                    key="project_setup_postcode_site_name",
                    placeholder="e.g. Unit 4, Main Office, Substation Compound",
                    help=(
                        "This becomes the site name and the first line of the saved address."
                    ),
                )
                preview_address = _build_site_address_from_postcode_result(
                    postcode_result,
                    site_name_input,
                )
                st.caption(
                    "Address preview: "
                    f"{preview_address or str(postcode_result.get('formatted_address') or normalized_postcode)}"
                )
                resolution_action_columns = st.columns(2, gap="small")
                with resolution_action_columns[0]:
                    apply_resolved_site = st.button(
                        "📍 Use This Site Address",
                        width="stretch",
                        disabled=not site_name_input.strip(),
                    )
                with resolution_action_columns[1]:
                    clear_postcode_search = st.button(
                        "✖ Clear Search",
                        key="project_setup_clear_postcode_search_new",
                        width="stretch",
                        type="secondary",
                    )
                if apply_resolved_site:
                    resolved_site_name = site_name_input.strip()
                    updated_setup = replace(
                        project_setup,
                        current_site_name=resolved_site_name,
                        site_address=_build_site_address_from_postcode_result(
                            postcode_result,
                            resolved_site_name,
                        ),
                        site_latitude=float(postcode_result["latitude"]),
                        site_longitude=float(postcode_result["longitude"]),
                    )
                    updated_setup = _save_project_setup(updated_setup)
                    st.session_state["project_setup"] = updated_setup
                    _clear_project_setup_postcode_state()
                    st.session_state["project_setup_flash"] = (
                        f"Saved site: {resolved_site_name} ({normalized_postcode})"
                    )
                    st.session_state["project_setup_flash_level"] = "success"
                    st.rerun()
            else:
                st.caption(
                    f"Saved address: {selected_profile.site_address or 'No address saved'}"
                )
                st.caption(
                    f"Last used: {selected_profile.last_used_at or 'Unknown'} | "
                    f"Radius: {selected_profile.geofence_radius_meters}m"
                )
                resolution_action_columns = st.columns(2, gap="small")
                with resolution_action_columns[0]:
                    load_matching_site = st.button(
                        "🗂️ Load This Known Site",
                        key="project_setup_load_postcode_known_site",
                        width="stretch",
                        type="primary",
                    )
                with resolution_action_columns[1]:
                    clear_postcode_search = st.button(
                        "✖ Clear Search",
                        key="project_setup_clear_postcode_search_known",
                        width="stretch",
                        type="secondary",
                    )
                if load_matching_site:
                    updated_setup = replace(
                        project_setup,
                        current_site_name=selected_profile.site_name,
                        site_address=selected_profile.site_address,
                        client_name=selected_profile.client_name or project_setup.client_name,
                        job_number=selected_profile.job_number,
                        site_latitude=selected_profile.site_latitude,
                        site_longitude=selected_profile.site_longitude,
                        geofence_radius_meters=selected_profile.geofence_radius_meters,
                    )
                    updated_setup = _save_project_setup(updated_setup)
                    st.session_state["project_setup"] = updated_setup
                    _clear_project_setup_postcode_state()
                    st.session_state["project_setup_flash"] = (
                        f"Loaded saved site: {selected_profile.site_name}"
                    )
                    st.session_state["project_setup_flash_level"] = "success"
                    st.rerun()

            if clear_postcode_search:
                _clear_project_setup_postcode_state()
                st.session_state["project_setup_flash"] = "Cleared postcode search."
                st.session_state["project_setup_flash_level"] = "info"
                st.rerun()

        if project_setup.known_sites:
            st.markdown("<div class='form-section-kicker'>Known Sites</div>", unsafe_allow_html=True)
            st.markdown(
                (
                    "<div class='form-section-copy'>"
                    "Previously used site profiles are remembered here so you can switch the command centre back to a known project quickly."
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            known_site_options = {
                profile.label: profile for profile in project_setup.known_sites
            }
            selected_known_site_label = st.selectbox(
                "Saved Site Profiles",
                options=list(known_site_options),
                key="project_setup_known_site",
            )
            selected_known_site = known_site_options[selected_known_site_label]
            st.caption(
                f"Last used: {selected_known_site.last_used_at or 'Unknown'} | "
                f"Radius: {selected_known_site.geofence_radius_meters}m"
            )
            if st.button(
                "🗂️ Load Selected Site",
                key="project_setup_load_known_site",
                width="stretch",
                type="secondary",
            ):
                updated_setup = replace(
                    project_setup,
                    current_site_name=selected_known_site.site_name,
                    site_address=selected_known_site.site_address,
                    client_name=selected_known_site.client_name or project_setup.client_name,
                    job_number=selected_known_site.job_number,
                    site_latitude=selected_known_site.site_latitude,
                    site_longitude=selected_known_site.site_longitude,
                    geofence_radius_meters=selected_known_site.geofence_radius_meters,
                )
                updated_setup = _save_project_setup(updated_setup)
                st.session_state["project_setup"] = updated_setup
                st.session_state["project_setup_flash"] = (
                    f"Loaded saved site: {selected_known_site.site_name}"
                )
                st.session_state["project_setup_flash_level"] = "success"
                st.rerun()

        st.markdown(
            (
                "<div class='sidebar-tool-card'>"
                "<div class='sidebar-tool-title'>Tunnel Note</div>"
                "<div class='sidebar-tool-copy'>"
                "One-time setup: run <code>cloudflared tunnel login</code> in Terminal before using the named tunnel launcher."
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )


def _render_sidebar_tunnel_status(project_setup: ProjectSetup) -> None:
    """Show the manager warning when the site tunnel is offline."""

    if _is_tunnel_running():
        return

    st.warning("⚠️ Site Gate Offline - Check Tunnel.")
    if project_setup.public_tunnel_url:
        st.caption(project_setup.public_tunnel_url)


def _render_workspace_doctor() -> None:
    """Render the workspace doctor status block in the sidebar."""

    with st.expander("🏥 Workspace Doctor", expanded=False):
        if st.button(
            "🔄 Re-scan Workspace",
            width="stretch",
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
            submitted = st.form_submit_button("💾 Update Status", width="stretch")

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
        if st.session_state.pop("ladder_permit_reset_pending", False):
            _reset_ladder_permit_form_state()

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
            "🔄 Refresh & Sync",
            key="file_4_refresh_sync",
            width="stretch",
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
            company_context_key = "ladder_permit_company_context_worker"
            company_selection_key = "ladder_permit_company_selection"
            company_override_key = "ladder_permit_worker_company_override"
            company_options = _build_file_4_company_options(
                repository,
                site_name=project_setup.current_site_name,
                worker_name=selected_worker.worker_name,
                default_company=selected_worker.company,
            )
            selected_worker_company = selected_worker.company.strip()
            if st.session_state.get(company_context_key) != selected_worker_label:
                st.session_state[company_selection_key] = (
                    selected_worker_company
                    if selected_worker_company in company_options
                    else (
                        company_options[0]
                        if company_options
                        else "🏢 Other Company (Type Below)"
                    )
                )
                st.session_state[company_override_key] = ""
                st.session_state[company_context_key] = selected_worker_label
            selected_company_option = st.selectbox(
                "Company",
                options=company_options,
                key=company_selection_key,
            )
            if selected_company_option == "🏢 Other Company (Type Below)":
                resolved_worker_company = st.text_input(
                    "Enter Company Name",
                    key=company_override_key,
                    placeholder="Enter contractor name",
                )
            else:
                resolved_worker_company = selected_company_option
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
                "🪜 Generate Ladder Permit",
                width="stretch",
                disabled=not project_setup.job_number.strip(),
            )

        if st.button(
            "↺ Reset Permit",
            key="ladder_permit_clear_form",
            width="stretch",
        ):
            _queue_ladder_permit_form_reset()
            st.session_state["ladder_permit_flash"] = {
                "level": "success",
                "message": "File 4 permit form reset.",
            }
            st.rerun()

        if not submitted:
            return

        _, selected_record = worker_options[selected_worker_label]
        try:
            if not str(resolved_worker_company or "").strip():
                raise ValidationError(
                    "Enter the correct contractor name before issuing the permit."
                )
            generated_permit = create_ladder_permit_draft(
                repository,
                attendance_record=selected_record,
                site_worker=selected_worker,
                worker_company_override=resolved_worker_company,
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
        "Inspection refs outstanding: "
        f"{sum(1 for asset in plant_assets if _plant_asset_requires_inspection_reference(asset))}"
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
    """Render the attendance station sidebar hints."""

    with st.expander("UHSF16.09", expanded=False):
        st.success("Daily sign-in / sign-out register is ready.")
        st.caption(f"Project: {project_setup.current_site_name}")
        st.caption(f"Attendance signatures: {FILE_2_ATTENDANCE_SIGNATURES_DIR.name}")
        st.caption(ATTENDANCE_FORM_METADATA)


def _render_sidebar_site_broadcast_quick_actions(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render the broadcast hub quick summary in the sidebar."""

    live_contacts = build_live_site_broadcast_contacts(
        repository,
        site_name=project_setup.current_site_name,
    )
    with st.expander("Broadcast Hub", expanded=False):
        st.success("Live fire roll audience is ready for Messages delivery.")
        st.caption(f"Project: {project_setup.current_site_name}")
        st.caption(f"Active phones on site: {len(live_contacts)}")
        if live_contacts:
            st.caption(
                "Latest contact: "
                f"{live_contacts[0].individual_name} | {live_contacts[0].mobile_number}"
            )
        else:
            st.caption("No valid mobile numbers are currently available on site.")


def _render_station_button_group(
    title: str,
    caption: str,
    stations: List[FileStation],
    *,
    active_station_label: str,
) -> Optional[str]:
    """Render one grouped station button row and return any newly selected station."""

    st.markdown(
        f"<div class='station-nav-group-title'>{html.escape(title)}</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<div class='station-nav-group-copy'>{html.escape(caption)}</div>",
        unsafe_allow_html=True,
    )
    selection: Optional[str] = None
    station_columns = st.columns(len(stations), gap="small")
    for station, station_column in zip(stations, station_columns):
        with station_column:
            if st.button(
                station.label,
                key=f"station_nav_{station.number}",
                width="stretch",
                type="primary" if active_station_label == station.label else "secondary",
            ):
                selection = station.label
    return selection


def _render_file_station_navigation() -> str:
    """Render the grouped station navigator and return the active station."""

    active_station_label = str(st.session_state["active_file_station"])
    core_stations = [
        station
        for station in FILE_STATIONS
        if station.number in {"FILE 1", "FILE 2", "FILE 3", "FILE 4"}
    ]
    live_stations = [
        station
        for station in FILE_STATIONS
        if station.number in {"INDUCTION", "BROADCAST"}
    ]

    core_selection = _render_station_button_group(
        "Core Site Files",
        "The formal project file spine. Keep these in order and use them as the compliance backbone.",
        core_stations,
        active_station_label=active_station_label,
    )
    live_selection = _render_station_button_group(
        "Live Operations",
        "Daily sign-in, alerts, and operational actions that sit around the core file structure.",
        live_stations,
        active_station_label=active_station_label,
    )

    selected_station = core_selection or live_selection
    if selected_station is not None:
        st.session_state["active_file_station"] = selected_station
        _sync_manager_station_query_params(selected_station)
        return selected_station
    return active_station_label


def _render_active_station(
    repository: DocumentRepository,
    station_label: str,
    project_setup: ProjectSetup,
    *,
    is_kiosk: bool = False,
) -> None:
    """Render the active file-station page."""

    station = _get_file_station(station_label)
    if not is_kiosk:
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
    if station.number == "BROADCAST":
        _render_site_broadcast_station(repository, project_setup)
        return
    if station.number == "INDUCTION":
        _render_site_induction_station(repository, project_setup, is_kiosk=is_kiosk)
        return
    _render_file_4_station(repository, project_setup)


def _render_station_header(station: FileStation, project_setup: ProjectSetup) -> None:
    """Render the file header card."""

    station_icon = {
        "FILE 1": "♻️",
        "FILE 2": "📋",
        "FILE 3": "🛡️",
        "FILE 4": "⚡",
        "INDUCTION": "🦺",
        "BROADCAST": "📢",
    }.get(station.number, "📁")
    st.markdown(
        (
            "<div class='station-header'>"
            f"<div class='station-hero-icon'>{station_icon}</div>"
            "<div class='station-hero-copy'>"
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
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_workspace_hero(
    *,
    icon: str,
    kicker: str,
    title: str,
    caption: str,
) -> None:
    """Render a premium hero block for one manager workspace tab."""

    st.markdown(
        (
            "<div class='workspace-hero'>"
            "<div class='workspace-hero-top'>"
            f"<div class='workspace-hero-icon'>{icon}</div>"
            "<div>"
            f"<div class='workspace-hero-kicker'>{html.escape(kicker)}</div>"
            f"<div class='workspace-hero-title'>{html.escape(title)}</div>"
            "</div>"
            "</div>"
            f"<div class='workspace-hero-caption'>{html.escape(caption)}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_file_1_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render File 1: Environment & Waste."""

    _render_workspace_hero(
        icon="♻️",
        kicker="File 1",
        title="Environment & Waste",
        caption="Monitor live waste movement, keep carrier compliance tight, and turn the filed evidence into clean environmental records.",
    )
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
    waste_source_conflicts = _get_cached_file_1_waste_source_conflicts(
        site_name=project_setup.current_site_name,
        repository=repository,
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

    flash_message = st.session_state.pop("waste_log_flash", None)
    if flash_message is not None:
        if flash_message["level"] == "error":
            st.error(flash_message["message"])
        else:
            st.success(flash_message["message"])

    file_1_view_options = {
        "Waste Register": "register",
        "Needs Review Queue": "review",
        "Smart Scan & Updates": "scan",
        "Carrier Compliance": "compliance",
    }
    selected_file_1_view = str(
        st.session_state.get("file_1_active_view", "register")
    )
    if selected_file_1_view not in file_1_view_options.values():
        selected_file_1_view = "register"
    st.session_state["file_1_active_view"] = selected_file_1_view

    st.divider()
    _render_workspace_zone_heading(
        "Workspace View",
        "Choose one File 1 workspace at a time. This keeps the page much snappier than rendering every heavy panel on each rerun.",
    )
    selected_file_1_view = str(
        st.radio(
            "File 1 Workspace View",
            options=list(file_1_view_options.values()),
            format_func=lambda option: next(
                label for label, value in file_1_view_options.items() if value == option
            ),
            horizontal=True,
            key="file_1_active_view",
            label_visibility="collapsed",
        )
    )

    if selected_file_1_view == "register":
        _render_file_1_waste_register_panel(
            repository,
            project_setup=project_setup,
            waste_notes=active_waste_notes,
            waste_kpi_metadata=waste_kpi_metadata,
            waste_source_conflicts=waste_source_conflicts,
        )
    elif selected_file_1_view == "review":
        _render_file_1_waste_review_queue_panel(
            repository,
            project_setup=project_setup,
            waste_kpi_metadata=waste_kpi_metadata,
            waste_notes=active_waste_notes,
            waste_source_conflicts=waste_source_conflicts,
        )
    elif selected_file_1_view == "scan":
        _render_file_1_waste_log_panel(
            repository,
            project_setup=project_setup,
            waste_kpi_metadata=waste_kpi_metadata,
            waste_notes=active_waste_notes,
            waste_source_conflicts=waste_source_conflicts,
        )
    else:
        _render_carrier_compliance_panel(abucs_rows)


def _format_waste_kpi_source_label(workbook_path: Optional[Path]) -> str:
    """Return a cleaner user-facing label for the linked waste KPI workbook."""

    if workbook_path is None:
        return "Fallback"

    source_label = workbook_path.stem.strip()
    if not source_label:
        source_label = workbook_path.name
    source_label = re.sub(r"[_]+", " ", source_label)
    source_label = re.sub(r"\s*-\s*", " · ", source_label)
    source_label = re.sub(r"\s{2,}", " ", source_label).strip()
    return source_label or workbook_path.name


def _build_file_1_waste_source_conflict_cache_key(
    repository: DocumentRepository,
    *,
    site_name: str,
) -> tuple[tuple[tuple[Any, ...], ...], tuple[tuple[str, int, int], ...]]:
    """Return a cache key that changes when waste files or overrides change."""

    override_signature = tuple(
        sorted(
            (
                waste_note.doc_id,
                waste_note.wtn_number,
                waste_note.source_file_override_path,
                waste_note.canonical_source_path,
                json.dumps(
                    [
                        source_candidate.to_storage_dict()
                        if hasattr(source_candidate, "to_storage_dict")
                        else {
                            "source_path": source_candidate.source_path,
                            "source_file_name": source_candidate.source_file_name,
                            "ticket_date": source_candidate.ticket_date.isoformat(),
                            "collection_type": source_candidate.collection_type,
                            "quantity_tonnes": source_candidate.quantity_tonnes,
                        }
                        for source_candidate in waste_note.source_conflict_candidates
                    ],
                    sort_keys=True,
                    default=str,
                ),
                waste_note.status.value,
            )
            for waste_note in repository.list_documents(
                document_type=WasteTransferNoteDocument.document_type
            )
            if isinstance(waste_note, WasteTransferNoteDocument)
            and waste_note.site_name.casefold() == site_name.casefold()
        )
    )
    file_signature = tuple(
        sorted(
            (
                source_path.name,
                source_path.stat().st_mtime_ns,
                source_path.stat().st_size,
            )
            for source_path in WASTE_DESTINATION.glob("*.pdf")
            if source_path.is_file()
        )
    )
    return override_signature, file_signature


@st.cache_data(show_spinner=False)
def _load_cached_file_1_waste_source_conflicts(
    *,
    site_name: str,
    override_signature: tuple[tuple[Any, ...], ...],
    file_signature: tuple[tuple[str, int, int], ...],
) -> List[Any]:
    """Return cached File 1 source conflicts for the current waste folder state."""

    del override_signature
    del file_signature
    repository = DocumentRepository(DATABASE_PATH)
    return list_waste_transfer_note_source_conflicts(
        repository,
        site_name=site_name,
    )


def _get_cached_file_1_waste_source_conflicts(
    *,
    site_name: str,
    repository: DocumentRepository,
) -> List[Any]:
    """Return cached duplicate-source conflicts for File 1."""

    override_signature, file_signature = _build_file_1_waste_source_conflict_cache_key(
        repository,
        site_name=site_name,
    )
    return _load_cached_file_1_waste_source_conflicts(
        site_name=site_name,
        override_signature=override_signature,
        file_signature=file_signature,
    )


def _render_file_1_waste_register_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
    waste_notes: List[WasteTransferNoteDocument],
    waste_kpi_metadata: Any,
    waste_source_conflicts: List[Any],
) -> None:
    """Render the live File 1 waste register and print action."""

    waste_source_conflict_lookup = _build_waste_source_conflict_lookup(
        waste_notes,
        waste_source_conflicts,
    )
    register_rows = _build_live_waste_register_rows(
        waste_notes,
        waste_source_conflict_lookup=waste_source_conflict_lookup,
    )
    needs_review_count = sum(
        1
        for waste_note in waste_notes
        if _waste_note_requires_queue_review(
            waste_note,
            waste_source_conflict_lookup=waste_source_conflict_lookup,
        )
    )
    file_1_indexed_files = repository.list_indexed_files(file_group=FileGroup.FILE_1)
    filed_ticket_pdfs = [
        indexed_file
        for indexed_file in file_1_indexed_files
        if indexed_file.file_category in {"abucs_pdf", "waste_ticket_pdf"}
        and indexed_file.site_name == project_setup.current_site_name
    ]
    generated_register_files = [
        indexed_file
        for indexed_file in file_1_indexed_files
        if indexed_file.file_category == "waste_register_docx"
        and indexed_file.site_name == project_setup.current_site_name
    ]
    waste_report_workbooks = [
        indexed_file
        for indexed_file in file_1_indexed_files
        if indexed_file.file_category in {"waste_report_excel", "waste_report_word"}
    ]
    summary_columns = st.columns(5)
    with summary_columns[0]:
        _render_inline_metric("Register Rows", str(len(register_rows)), icon="🧾")
    with summary_columns[1]:
        _render_inline_metric(
            "Logged Tonnage",
            f"{sum(note.quantity_tonnes for note in waste_notes):.2f} t",
            icon="♻️",
        )
    with summary_columns[2]:
        _render_inline_metric(
            "KPI Workbook",
            "Linked" if waste_kpi_metadata.workbook_path is not None else "Fallback",
            icon="📊",
        )
    with summary_columns[3]:
        _render_inline_metric(
            "Needs Review",
            str(needs_review_count),
            icon="🟠",
        )
    with summary_columns[4]:
        _render_inline_metric(
            "Source Conflicts",
            str(len(waste_source_conflicts)),
            icon="⚠️",
        )

    _render_workspace_hero(
        icon="♻️",
        kicker="Waste Register",
        title="Live Waste Removal History",
        caption="This view holds the active File 1 waste history and feeds the official UHSF50.0 register output.",
    )
    if waste_kpi_metadata.workbook_path is not None:
        st.caption(
            "KPI source: "
            f"{_format_waste_kpi_source_label(waste_kpi_metadata.workbook_path)}"
        )
    else:
        st.caption("KPI source: not found in FILE_1_Environment/Waste_Reports.")

    st.divider()
    _render_workspace_zone_heading(
        "Evidence Snapshot",
        "File 1 waste is grounded in filed ticket PDFs, KPI workbooks, and generated register outputs.",
    )
    evidence_columns = st.columns(3)
    with evidence_columns[0]:
        _render_inline_metric("Filed Ticket PDFs", str(len(filed_ticket_pdfs)), icon="📄")
    with evidence_columns[1]:
        _render_inline_metric("Waste Reports", str(len(waste_report_workbooks)), icon="📊")
    with evidence_columns[2]:
        _render_inline_metric("Generated Registers", str(len(generated_register_files)), icon="🖨️")

    if waste_source_conflicts:
        st.divider()
        _render_workspace_zone_heading(
            "Source Conflicts",
            "These WTNs have more than one filed PDF. The register now uses a single canonical source for each ticket number.",
        )
        st.warning(
            "Duplicate filed waste tickets were found. Review the canonical source below before printing the File 1 register."
        )
        conflict_rows = [
            {
                "WTN": source_conflict.wtn_number,
                "Ticket Date": source_conflict.canonical_source.scanned_note.ticket_date.strftime("%d/%m/%Y"),
                "Canonical Source": source_conflict.canonical_source.source_path.name,
                "Chosen Date": source_conflict.canonical_source.scanned_note.ticket_date.strftime("%d/%m/%Y"),
                "Chosen Tonnes": (
                    f"{source_conflict.canonical_source.scanned_note.quantity_tonnes:.2f}"
                    if source_conflict.canonical_source.scanned_note.quantity_tonnes is not None
                    else "Needs review"
                ),
                "Alt Sources": ", ".join(
                    source_candidate.source_path.name
                    for source_candidate in source_conflict.source_candidates
                    if source_candidate.source_path != source_conflict.canonical_source.source_path
                ),
            }
            for source_conflict in waste_source_conflicts
        ]
        st.dataframe(
            pd.DataFrame(conflict_rows),
            hide_index=True,
            width="stretch",
        )

    st.divider()
    _render_workspace_zone_heading(
        "Live Register",
        "This table is the active waste history that drives the physical File 1 register.",
    )
    if register_rows:
        st.dataframe(
            pd.DataFrame(register_rows),
            hide_index=True,
            width="stretch",
        )
    else:
        st.info("No waste register rows have been logged yet.")

    st.divider()
    _render_workspace_zone_heading(
        "Export / Print",
        "Generate the official UHSF50.0 register from the live File 1 waste history.",
    )
    if st.button(
        "🖨️ Print Waste Register",
        key="print_waste_register",
        width="stretch",
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


def _build_file_1_waste_note_options(
    waste_notes: List[WasteTransferNoteDocument],
) -> Dict[str, WasteTransferNoteDocument]:
    """Return selectbox options for File 1 waste-note review."""

    return {
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


def _build_file_1_waste_review_queue_rows(
    waste_notes: List[WasteTransferNoteDocument],
    *,
    waste_source_conflict_lookup: Dict[tuple[str, str], Any],
) -> List[Dict[str, str]]:
    """Return the queue rows for WTNs that still need File 1 review."""

    queue_rows: List[Dict[str, str]] = []
    for waste_note in sorted(
        waste_notes,
        key=lambda note: (note.date, note.created_at, note.wtn_number),
        reverse=True,
    ):
        if not _waste_note_requires_queue_review(
            waste_note,
            waste_source_conflict_lookup=waste_source_conflict_lookup,
        ):
            continue
        source_conflict = _get_waste_source_conflict_for_note(
            waste_note,
            waste_source_conflict_lookup,
        )
        issue_label = (
            "Source Conflict"
            if source_conflict is not None
            else "Missing Tonnage"
        )
        canonical_source_name = (
            source_conflict.canonical_source.source_path.name
            if source_conflict is not None
            else ""
        )
        queue_rows.append(
            {
                "WTN": waste_note.wtn_number,
                "Issue": issue_label,
                "Date": waste_note.date.strftime("%d/%m/%Y"),
                "Carrier": waste_note.carrier_name,
                "Tonnes": _format_waste_note_tonnage_label(waste_note),
                "Source": canonical_source_name or "Filed source linked",
            }
        )
    return queue_rows


def _render_file_1_waste_note_review_workspace(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
    waste_kpi_metadata: Any,
    selected_waste_note: WasteTransferNoteDocument,
    source_conflict_lookup: Dict[tuple[str, str], Any],
    key_prefix: str,
    queue_state_key: Optional[str] = None,
    queue_pending_state_key: Optional[str] = None,
    next_queue_label: Optional[str] = None,
) -> None:
    """Render the review/editor workspace for one File 1 waste note."""

    selected_source_path = _get_file_1_waste_note_source_path(
        repository,
        selected_waste_note,
        source_conflict_lookup=source_conflict_lookup,
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

    if scanned_waste_note is not None and scanned_waste_note.collection_type:
        st.info(
            "Supplier collection type detected: "
            f"{scanned_waste_note.collection_type}"
        )

    selected_source_conflict = _get_waste_source_conflict_for_note(
        selected_waste_note,
        source_conflict_lookup,
    )

    if waste_kpi_metadata.workbook_path is None:
        st.warning(
            "No File 1 KPI workbook was found. The form is using the current Project Setup values."
        )

    if selected_source_conflict is not None:
        st.warning(
            "This WTN has more than one filed PDF. Choose the correct source first, then save the waste details below."
        )
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "Source File": source_candidate.source_path.name,
                        "Chosen": (
                            "Yes"
                            if selected_source_path is not None
                            and source_candidate.source_path.resolve() == selected_source_path.resolve()
                            else ""
                        ),
                        "Date": source_candidate.scanned_note.ticket_date.strftime("%d/%m/%Y"),
                        "Collection": source_candidate.scanned_note.collection_type or "Not shown",
                        "Tonnes": (
                            f"{source_candidate.scanned_note.quantity_tonnes:.2f}"
                            if source_candidate.scanned_note.quantity_tonnes is not None
                            else "Needs review"
                        ),
                    }
                    for source_candidate in selected_source_conflict.source_candidates
                ]
            ),
            hide_index=True,
            width="stretch",
        )
        st.caption("Pick the filed PDF you trust. The register will keep using that source on future syncs.")
        for source_candidate in selected_source_conflict.source_candidates:
            is_current_source = (
                selected_source_path is not None
                and source_candidate.source_path.resolve() == selected_source_path.resolve()
            )
            action_columns = st.columns([4, 1])
            with action_columns[0]:
                detail_parts = [
                    source_candidate.source_path.name,
                    source_candidate.scanned_note.ticket_date.strftime("%d/%m/%Y"),
                ]
                if source_candidate.scanned_note.collection_type:
                    detail_parts.append(source_candidate.scanned_note.collection_type)
                if source_candidate.scanned_note.quantity_tonnes is not None:
                    detail_parts.append(f"{source_candidate.scanned_note.quantity_tonnes:.2f} t")
                st.markdown("**Source Review**  \n" + " | ".join(detail_parts))
            with action_columns[1]:
                if is_current_source:
                    st.button(
                        "✅ In Use",
                        key=f"{key_prefix}-use-waste-source-{selected_waste_note.doc_id}-{source_candidate.source_path.name}",
                        disabled=True,
                        width="stretch",
                    )
                elif st.button(
                    "Use This Source",
                    key=f"{key_prefix}-use-waste-source-{selected_waste_note.doc_id}-{source_candidate.source_path.name}",
                    width="stretch",
                ):
                    try:
                        set_waste_transfer_note_source_override(
                            repository,
                            source_document=selected_waste_note,
                            source_path=source_candidate.source_path,
                        )
                    except ValidationError as exc:
                        st.session_state["waste_log_flash"] = {
                            "level": "error",
                            "message": str(exc),
                        }
                    except Exception as exc:
                        st.session_state["waste_log_flash"] = {
                            "level": "error",
                            "message": f"Unable to switch the waste source file: {exc}",
                        }
                    else:
                        st.session_state["waste_log_flash"] = {
                            "level": "success",
                            "message": (
                                "Canonical waste source updated to "
                                f"{source_candidate.source_path.name}."
                            ),
                        }
                    st.rerun()

    st.divider()
    _render_workspace_zone_heading(
        "Primary Action",
        "Review the current ticket, correct the live File 1 record, and save it back into the register.",
    )
    workbook_client_name = waste_kpi_metadata.client_name or project_setup.client_name
    workbook_site_address = (
        waste_kpi_metadata.site_address or project_setup.site_address
    )
    workbook_project_number = (
        waste_kpi_metadata.project_number or project_setup.job_number
    )
    scan_columns = st.columns(3)
    with scan_columns[0]:
        st.text_input(
            "Client Name",
            value=workbook_client_name,
            disabled=True,
            key=f"{key_prefix}-{selected_waste_note.doc_id}-client-name",
        )
    with scan_columns[1]:
        st.text_input(
            "Site Address",
            value=workbook_site_address,
            disabled=True,
            key=f"{key_prefix}-{selected_waste_note.doc_id}-site-address",
        )
    with scan_columns[2]:
        st.text_input(
            "Project Number",
            value=workbook_project_number,
            disabled=True,
            key=f"{key_prefix}-{selected_waste_note.doc_id}-project-number",
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
    default_tonnage_review_status = (
        selected_waste_note.tonnage_review_status
        if selected_waste_note.quantity_tonnes <= 0
        else ""
    )

    with st.form(
        f"{key_prefix}-{selected_waste_note.doc_id}-waste-log-form",
        clear_on_submit=False,
    ):
        detail_columns = st.columns(3)
        with detail_columns[0]:
            carrier_name = st.text_input(
                "Carrier Name",
                value=default_carrier_name,
                key=f"{key_prefix}-{selected_waste_note.doc_id}-carrier-name",
            )
        with detail_columns[1]:
            vehicle_registration = st.text_input(
                "Vehicle Reg",
                value=default_vehicle_registration,
                key=f"{key_prefix}-{selected_waste_note.doc_id}-vehicle-registration",
            )
        with detail_columns[2]:
            st.text_input(
                "WTN Reference",
                value=selected_waste_note.wtn_number,
                disabled=True,
                key=f"{key_prefix}-{selected_waste_note.doc_id}-wtn-number",
            )

        waste_description = st.text_input(
            "Description of Waste",
            value=default_waste_description,
            key=f"{key_prefix}-{selected_waste_note.doc_id}-waste-description",
        )

        detail_columns = st.columns(4)
        with detail_columns[0]:
            ticket_date = st.date_input(
                "Date",
                value=default_ticket_date,
                key=f"{key_prefix}-{selected_waste_note.doc_id}-ticket-date",
            )
        with detail_columns[1]:
            quantity_tonnes = st.number_input(
                "Quantity (tonnes)",
                min_value=0.0,
                step=0.01,
                value=float(default_quantity_tonnes or 0.0),
                key=f"{key_prefix}-{selected_waste_note.doc_id}-quantity-tonnes",
            )
        with detail_columns[2]:
            ewc_code = st.text_input(
                "EWC Code",
                value=default_ewc_code,
                key=f"{key_prefix}-{selected_waste_note.doc_id}-ewc-code",
            )
        with detail_columns[3]:
            destination_facility = st.text_input(
                "Destination Facility",
                value=default_destination_facility,
                key=f"{key_prefix}-{selected_waste_note.doc_id}-destination-facility",
            )

        tonnage_review_status = ""
        if quantity_tonnes <= 0:
            tonnage_review_status = str(
                st.selectbox(
                    "Missing Tonnage Handling",
                    options=["-- Select resolution --", *WASTE_MISSING_TONNAGE_REVIEW_OPTIONS],
                    index=(
                        0
                        if not default_tonnage_review_status
                        else (
                            list(WASTE_MISSING_TONNAGE_REVIEW_OPTIONS).index(
                                default_tonnage_review_status
                            )
                            + 1
                        )
                    ),
                    key=f"{key_prefix}-{selected_waste_note.doc_id}-tonnage-review-status",
                    help=(
                        "Use this when the supplier ticket genuinely does not show a weight, "
                        "or when the final tonnage is being resolved outside the ticket."
                    ),
                )
            )
            if tonnage_review_status == "-- Select resolution --":
                tonnage_review_status = ""
            if _is_tanker_waste_note(selected_waste_note):
                st.caption(
                    "Tanker runs can be closed honestly here without inventing a tonne value."
                )

        if queue_state_key is not None:
            submit_columns = st.columns(2)
            with submit_columns[0]:
                submitted = st.form_submit_button("💾 Save Waste Record", width="stretch")
            with submit_columns[1]:
                resolve_and_next = st.form_submit_button("✅ Resolve & Next", width="stretch")
        else:
            submitted = st.form_submit_button("💾 Save Waste Record", width="stretch")
            resolve_and_next = False

    if not submitted and not resolve_and_next:
        if scanned_waste_note is not None and scanned_waste_note.extracted_text.strip():
            st.divider()
            _render_workspace_zone_heading(
                "Scan Preview",
                "Use this OCR preview when the scanned values need checking before you save.",
            )
            with st.expander("Scanned text preview", expanded=False):
                st.text(scanned_waste_note.extracted_text[:3000])
        return

    if quantity_tonnes <= 0 and not tonnage_review_status:
        st.session_state["waste_log_flash"] = {
            "level": "error",
            "message": (
                "Enter Quantity (tonnes) or choose a Missing Tonnage Handling option "
                "before logging waste."
            ),
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
            tonnage_review_status=tonnage_review_status,
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
    if resolve_and_next and queue_pending_state_key is not None:
        if next_queue_label is not None:
            st.session_state[queue_pending_state_key] = next_queue_label
        else:
            st.session_state[queue_pending_state_key] = ""
    st.rerun()


def _render_file_1_waste_review_queue_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
    waste_kpi_metadata: Any,
    waste_notes: List[WasteTransferNoteDocument],
    waste_source_conflicts: List[Any],
) -> None:
    """Render the dedicated File 1 review queue for unresolved waste tickets."""

    source_conflict_lookup = _build_waste_source_conflict_lookup(
        waste_notes,
        waste_source_conflicts,
    )
    queue_note_options = _build_file_1_waste_note_options(
        [
            waste_note
            for waste_note in waste_notes
            if _waste_note_requires_queue_review(
                waste_note,
                waste_source_conflict_lookup=source_conflict_lookup,
            )
        ]
    )
    queue_rows = _build_file_1_waste_review_queue_rows(
        list(queue_note_options.values()),
        waste_source_conflict_lookup=source_conflict_lookup,
    )
    weightless_count = sum(
        1 for waste_note in queue_note_options.values() if waste_note.quantity_tonnes <= 0
    )
    conflict_count = sum(
        1
        for waste_note in queue_note_options.values()
        if _get_waste_source_conflict_for_note(waste_note, source_conflict_lookup)
        is not None
    )

    summary_columns = st.columns(3)
    with summary_columns[0]:
        _render_inline_metric("Tickets in Queue", str(len(queue_rows)), icon="🧭")
    with summary_columns[1]:
        _render_inline_metric("Missing Tonnage", str(weightless_count), icon="⚖️")
    with summary_columns[2]:
        _render_inline_metric("Source Conflicts", str(conflict_count), icon="⚠️")

    _render_workspace_hero(
        icon="🧭",
        kicker="Needs Review",
        title="Waste Review Queue",
        caption="Work through the tickets that still block confidence in the File 1 register: missing tonnage first, duplicate source files second.",
    )

    if not queue_rows:
        st.success("File 1 waste is looking clean. No live tickets currently need review.")
        return

    st.divider()
    _render_workspace_zone_heading(
        "Queue Snapshot",
        "This is the short list of waste tickets that still need attention before the printed register is fully comfortable to trust.",
    )
    st.dataframe(
        pd.DataFrame(queue_rows),
        hide_index=True,
        width="stretch",
    )

    queue_option_labels = list(queue_note_options)
    queue_state_key = "file_1_review_queue_selected_note"
    queue_pending_state_key = f"{queue_state_key}_pending"
    queued_queue_label = st.session_state.pop(queue_pending_state_key, None)
    if queued_queue_label in queue_option_labels:
        st.session_state[queue_state_key] = queued_queue_label
    elif queued_queue_label == "":
        st.session_state.pop(queue_state_key, None)
    selected_queue_label = st.session_state.get(queue_state_key)
    if selected_queue_label not in queue_option_labels:
        st.session_state[queue_state_key] = queue_option_labels[0]
        selected_queue_label = queue_option_labels[0]
    current_index = queue_option_labels.index(selected_queue_label)
    next_queue_label = (
        queue_option_labels[current_index + 1]
        if current_index + 1 < len(queue_option_labels)
        else (
            queue_option_labels[current_index - 1]
            if current_index - 1 >= 0 and len(queue_option_labels) > 1
            else None
        )
    )

    navigation_columns = st.columns([1, 1, 3])
    with navigation_columns[0]:
        if st.button(
            "← Older",
            key="file_1_review_queue_previous",
            disabled=current_index == len(queue_option_labels) - 1,
            width="stretch",
        ):
            st.session_state[queue_pending_state_key] = queue_option_labels[current_index + 1]
            st.rerun()
    with navigation_columns[1]:
        if st.button(
            "Newer →",
            key="file_1_review_queue_next",
            disabled=current_index == 0,
            width="stretch",
        ):
            st.session_state[queue_pending_state_key] = queue_option_labels[current_index - 1]
            st.rerun()
    with navigation_columns[2]:
        st.caption(
            f"Reviewing {current_index + 1} of {len(queue_option_labels)} unresolved tickets. The queue is ordered newest first."
        )

    selected_queue_label = st.selectbox(
        "Choose the next ticket to review",
        options=queue_option_labels,
        key=queue_state_key,
    )
    selected_waste_note = queue_note_options[selected_queue_label]
    _render_file_1_waste_note_review_workspace(
        repository,
        project_setup=project_setup,
        waste_kpi_metadata=waste_kpi_metadata,
        selected_waste_note=selected_waste_note,
        source_conflict_lookup=source_conflict_lookup,
        key_prefix="file_1_review_queue",
        queue_state_key=queue_state_key,
        queue_pending_state_key=queue_pending_state_key,
        next_queue_label=next_queue_label,
    )


def _render_file_1_waste_log_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
    waste_kpi_metadata: Any,
    waste_notes: List[WasteTransferNoteDocument],
    waste_source_conflicts: List[Any],
) -> None:
    """Render the File 1 WTN smart-scan form for already-filed notes."""

    summary_columns = st.columns(3)
    with summary_columns[0]:
        _render_inline_metric("Filed WTNs", str(len(waste_notes)), icon="📥")
    with summary_columns[1]:
        _render_inline_metric(
            "KPI Workbook",
            "Connected" if waste_kpi_metadata.workbook_path is not None else "Fallback",
            icon="📊",
        )
    with summary_columns[2]:
        _render_inline_metric(
            "Project",
            project_setup.job_number or "Not set",
            icon="🏷️",
        )

    _render_workspace_hero(
        icon="🔎",
        kicker="Smart Scan",
        title="Filed Waste Transfer Notes",
        caption="Review the filed WTNs, trust the smart scan where it is strong, and correct the live register before printing.",
    )
    st.caption("Use `Needs Review Queue` for the short list of problem tickets. This tab remains the full review space for any filed WTN.")

    if not waste_notes:
        st.info("No filed WTNs found in File 1 yet. Run SYNC WORKSPACE to ingest the waste PDFs first.")
        return

    note_options = _build_file_1_waste_note_options(waste_notes)
    note_option_labels = list(note_options)
    note_state_key = "file_1_selected_waste_note"
    selected_note_label = st.session_state.get(note_state_key)
    if selected_note_label not in note_option_labels:
        st.session_state[note_state_key] = note_option_labels[0]

    selected_note_label = st.selectbox(
        "Select Waste Transfer Note",
        options=note_option_labels,
        key=note_state_key,
    )
    selected_waste_note = note_options[selected_note_label]
    source_conflict_lookup = _build_waste_source_conflict_lookup(
        waste_notes,
        waste_source_conflicts,
    )
    _render_file_1_waste_note_review_workspace(
        repository,
        project_setup=project_setup,
        waste_kpi_metadata=waste_kpi_metadata,
        selected_waste_note=selected_waste_note,
        source_conflict_lookup=source_conflict_lookup,
        key_prefix="file_1_smart_scan",
    )


def _render_file_2_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render File 2: Registers & Diary."""

    _render_workspace_hero(
        icon="📋",
        kicker="File 2",
        title="Registers & Diary",
        caption="Keep the live site record moving: checks, attendance, plant, and daily operating registers in one controlled workspace.",
    )
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

    selected_file_2_view = str(
        st.session_state.get("file2_active_view", _get_file_2_view_from_query_params() or "checks")
    ).strip().casefold()
    if selected_file_2_view not in FILE_2_VIEW_KEYS:
        selected_file_2_view = "checks"
    st.session_state["file2_active_view"] = selected_file_2_view

    st.divider()
    _render_workspace_zone_heading(
        "Workspace View",
        "Move between the main File 2 workspaces without losing your place. The Daily Site Diary view is now sticky for dictation returns.",
    )
    selected_file_2_view = str(
        st.radio(
            "File 2 Workspace View",
            options=[view_key for view_key, _ in FILE_2_VIEW_OPTIONS],
            format_func=lambda view_key: FILE_2_VIEW_LABELS.get(view_key, view_key),
            key="file2_active_view",
            horizontal=True,
            label_visibility="collapsed",
        )
    ).strip().casefold()
    _sync_file_2_view_query_param(selected_file_2_view)

    if selected_file_2_view == "checks":
        _render_file_2_site_checks_panel(
            repository,
            site_name=project_setup.current_site_name,
            latest_site_check=latest_site_check,
        )
    elif selected_file_2_view == "attendance":
        st.markdown(
            (
                "<div class='panel-card'>"
                "<div class='panel-heading'>Attendance Snapshot</div>"
                "<div class='panel-title'>Today at a Glance</div>"
                "<div class='panel-caption'>"
                "This is the quick File 2 snapshot. Use the live attendance station for sign-in, sign-out, fire roll, and print actions."
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        st.divider()
        _render_workspace_zone_heading(
            "Live Register / History",
            "This is the quick on-page attendance snapshot. The dedicated attendance station handles live sign-in, sign-out, fire roll, and printing.",
        )
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
    elif selected_file_2_view == "diary":
        _render_file_2_site_diary_panel(
            repository,
            project_setup=project_setup,
        )
    elif selected_file_2_view == "plant":
        _render_file_2_plant_register_panel(
            repository,
            project_setup=project_setup,
        )
    else:
        _render_workspace_zone_heading(
            "Primary Action",
            "Toolbox Talk creation, remote signing, and register export now live in the Broadcast station so operational messaging stays in one place.",
        )
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

    _render_workspace_hero(
        icon="🛡️",
        kicker="File 3",
        title="Contractor Master",
        caption="Track contractor competence, safety paperwork, and roster coverage so site assurance is always visible and current.",
    )
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

    safety_tab, roster_tab = st.tabs(["Safety Inventory", "Contractor Roster"])
    with safety_tab:
        _render_file_3_safety_panel(
            repository,
            project_setup=project_setup,
        )
    with roster_tab:
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
    """Render File 3 as a document-first safety vault."""

    st.session_state.pop("file3_safety_wipe_count", None)
    st.session_state.pop("file3_manual_review_saved", None)
    st.session_state.pop("file3_manual_review_parked", None)
    st.session_state.pop("file3_safety_rebuild_result", None)

    rams_files = _list_workspace_files(RAMS_DESTINATION)
    coshh_files = _list_workspace_files(COSHH_DESTINATION)
    review_files = _list_workspace_files(FILE_3_REVIEW_DIR)
    induction_files = _list_workspace_files(FILE_3_COMPLETED_INDUCTIONS_DIR)
    competency_files = _list_workspace_files(FILE_3_COMPETENCY_CARDS_DIR)
    register_archive_files = _list_workspace_files(FILE_3_OUTPUT_DIR)

    _render_workspace_hero(
        icon="🗃️",
        kicker="Safety Vault",
        title="Filed Safety Documents",
        caption="File 3 is now document-first: keep approved RAMS, COSHH, review packs, inductions, and supporting evidence visible without asking the app to guess safety metadata for you.",
    )

    summary_columns = st.columns(5)
    with summary_columns[0]:
        _render_inline_metric("RAMS Files", str(len(rams_files)), icon="📘")
    with summary_columns[1]:
        _render_inline_metric("COSHH Files", str(len(coshh_files)), icon="🧪")
    with summary_columns[2]:
        _render_inline_metric("Review Packs", str(len(review_files)), icon="🧾")
    with summary_columns[3]:
        _render_inline_metric("Inductions", str(len(induction_files)), icon="🦺")
    with summary_columns[4]:
        _render_inline_metric("Competency Cards", str(len(competency_files)), icon="🎫")

    st.divider()
    _render_workspace_zone_heading(
        "Primary Action",
        "Use File 3 as the controlled document vault. Open the live folders, check what has been filed, and rely on the approved paperwork rather than automatic register generation.",
    )
    action_columns = st.columns(4)
    with action_columns[0]:
        if st.button("📂 Open RAMS Folder", key="file3-open-rams-folder", width="stretch"):
            _open_workspace_path(RAMS_DESTINATION)
    with action_columns[1]:
        if st.button("📂 Open COSHH Folder", key="file3-open-coshh-folder", width="stretch"):
            _open_workspace_path(COSHH_DESTINATION)
    with action_columns[2]:
        if st.button("📂 Open Review Packs", key="file3-open-review-folder", width="stretch"):
            _open_workspace_path(FILE_3_REVIEW_DIR)
    with action_columns[3]:
        if st.button("📂 Open Induction Output", key="file3-open-inductions-folder", width="stretch"):
            _open_workspace_path(FILE_3_COMPLETED_INDUCTIONS_DIR)

    st.caption(
        "The old RAMS/COSHH auto-register workflow has been retired from the manager view because it was creating more checking work than value on live jobs."
    )

    st.divider()
    rams_tab, coshh_tab, review_tab, inductions_tab, archive_tab = st.tabs(
        ["RAMS Files", "COSHH Files", "Review Packs", "Inductions", "Registers Archive"]
    )
    with rams_tab:
        _render_file_3_vault_tab(
            heading="Live Register / History",
            caption="Approved RAMS documents currently filed in File 3.",
            directory=RAMS_DESTINATION,
            files=rams_files,
            selection_key="file3-rams-vault-select",
            open_folder_label="📂 Open RAMS Folder",
            empty_message="No RAMS files are currently filed in the File 3 RAMS folder.",
        )
    with coshh_tab:
        _render_file_3_vault_tab(
            heading="Live Register / History",
            caption="Approved COSHH documents currently filed in File 3.",
            directory=COSHH_DESTINATION,
            files=coshh_files,
            selection_key="file3-coshh-vault-select",
            open_folder_label="📂 Open COSHH Folder",
            empty_message="No COSHH files are currently filed in the File 3 COSHH folder.",
        )
    with review_tab:
        _render_file_3_vault_tab(
            heading="Live Register / History",
            caption="RAMS review forms, construction plans, and held safety files that should stay visible but out of any auto-generated live register.",
            directory=FILE_3_REVIEW_DIR,
            files=review_files,
            selection_key="file3-review-vault-select",
            open_folder_label="📂 Open Review Packs",
            empty_message="No review forms or held safety files are currently parked in File 3.",
        )
    with inductions_tab:
        induction_summary_columns = st.columns(2)
        with induction_summary_columns[0]:
            _render_file_3_vault_tab(
                heading="Live Register / History",
                caption="Completed induction packs filed to File 3.",
                directory=FILE_3_COMPLETED_INDUCTIONS_DIR,
                files=induction_files,
                selection_key="file3-induction-vault-select",
                open_folder_label="📂 Open Completed Inductions",
                empty_message="No completed induction DOCX files are currently filed in File 3.",
            )
        with induction_summary_columns[1]:
            _render_file_3_vault_tab(
                heading="Supporting Evidence",
                caption="Competency card uploads captured during induction.",
                directory=FILE_3_COMPETENCY_CARDS_DIR,
                files=competency_files,
                selection_key="file3-competency-vault-select",
                open_folder_label="📂 Open Competency Cards",
                empty_message="No competency card files are currently filed in File 3.",
            )
    with archive_tab:
        _render_file_3_vault_tab(
            heading="Export / Print",
            caption="Legacy File 3 register outputs already generated earlier in the project. Kept visible here as archive only, not as a live automation workflow.",
            directory=FILE_3_OUTPUT_DIR,
            files=register_archive_files,
            selection_key="file3-archive-vault-select",
            open_folder_label="📂 Open Register Archive",
            empty_message="No legacy File 3 register exports are currently filed in the archive folder.",
        )


def _build_induction_picker_records(
    induction_documents: List[InductionDocument],
) -> List[InductionDocument]:
    """Return one latest induction record per operative/company pairing."""

    latest_records: Dict[tuple[str, str], InductionDocument] = {}
    for induction_document in induction_documents:
        record_key = (
            induction_document.individual_name.casefold(),
            induction_document.contractor_name.casefold(),
        )
        existing_record = latest_records.get(record_key)
        if (
            existing_record is None
            or induction_document.created_at > existing_record.created_at
        ):
            latest_records[record_key] = induction_document
    return sorted(
        latest_records.values(),
        key=lambda induction_document: (
            induction_document.individual_name.casefold(),
            induction_document.contractor_name.casefold(),
        ),
    )


def _build_induction_company_options(
    repository: DocumentRepository,
    *,
    site_name: str,
    induction_documents: List[InductionDocument],
) -> List[str]:
    """Return the smart company dropdown options for the induction form."""

    company_names_by_key: Dict[str, str] = {}

    def _remember_company(raw_company_name: str) -> None:
        cleaned_company_name = raw_company_name.strip()
        if not cleaned_company_name:
            return
        company_names_by_key.setdefault(
            cleaned_company_name.casefold(),
            cleaned_company_name,
        )

    for induction_document in induction_documents:
        _remember_company(induction_document.contractor_name)

    for attendance_document in repository.list_documents(
        document_type=DailyAttendanceEntryDocument.document_type,
        site_name=site_name,
    ):
        if isinstance(attendance_document, DailyAttendanceEntryDocument):
            _remember_company(attendance_document.contractor_name)

    current_site_roster = build_site_worker_roster(site_name=site_name)
    for worker in current_site_roster:
        _remember_company(worker.company)

    # If the active site is brand new or just being used for testing, fall back to the
    # wider KPI roster so the induction company picker is still useful immediately.
    if not company_names_by_key:
        for worker in build_site_worker_roster():
            _remember_company(worker.company)

    unique_contractors = sorted(company_names_by_key.values(), key=str.casefold)
    return [
        "-- Select Company --",
        *unique_contractors,
        "🏢 New Company (Type Below)",
    ]


def _attendance_picker_label(induction_document: InductionDocument) -> str:
    """Return the operative label shown in the UHSF16.09 picker."""

    return (
        f"{induction_document.individual_name} "
        f"({induction_document.contractor_name})"
    )


def _attendance_sign_out_label(attendance_entry: DailyAttendanceEntryDocument) -> str:
    """Return the sign-out label for one live operative."""

    return (
        f"{attendance_entry.individual_name} "
        f"({attendance_entry.contractor_name}) · "
        f"In {attendance_entry.time_in.strftime('%H:%M')}"
    )


def _attendance_manager_correction_label(
    attendance_entry: DailyAttendanceEntryDocument,
) -> str:
    """Return the manager correction label for one saved attendance entry."""

    if attendance_entry.time_out is None:
        status_text = "On Site"
    else:
        status_text = f"Out {attendance_entry.time_out.strftime('%H:%M')}"
    return (
        f"{attendance_entry.individual_name} "
        f"({attendance_entry.contractor_name}) · "
        f"In {attendance_entry.time_in.strftime('%H:%M')} · {status_text}"
    )


def _resolve_attendance_sign_in_selection(
    *,
    filtered_records: List[InductionDocument],
    current_doc_id: str,
    pending_doc_id: str = "",
) -> str:
    """Return the best induction doc id to preselect in the live sign-in picker."""

    available_doc_ids = {record.doc_id for record in filtered_records}
    if pending_doc_id and pending_doc_id in available_doc_ids:
        return pending_doc_id
    if current_doc_id and current_doc_id in available_doc_ids:
        return current_doc_id
    if len(filtered_records) == 1:
        return filtered_records[0].doc_id
    return ""


def _resolve_attendance_sign_out_selection(
    *,
    filtered_entries: List[DailyAttendanceEntryDocument],
    current_doc_id: str,
) -> str:
    """Return the best live attendance doc id to preselect for sign-out."""

    available_doc_ids = {entry.doc_id for entry in filtered_entries}
    if current_doc_id and current_doc_id in available_doc_ids:
        return current_doc_id
    if len(filtered_entries) == 1:
        return filtered_entries[0].doc_id
    return ""


def _resolve_attendance_correction_selection(
    *,
    filtered_entries: List[DailyAttendanceEntryDocument],
    current_doc_id: str,
) -> str:
    """Return the best attendance record to preselect in manager corrections."""

    available_doc_ids = {entry.doc_id for entry in filtered_entries}
    if current_doc_id and current_doc_id in available_doc_ids:
        return current_doc_id
    if len(filtered_entries) == 1:
        return filtered_entries[0].doc_id
    return ""


def _is_uplands_company(company_name: str) -> bool:
    """Return True when the company should count as an Uplands employee."""

    lowered_company_name = company_name.casefold()
    return any(
        alias in lowered_company_name
        for alias in ("uplands", "url", "uplands retail")
    )


def _build_live_fire_roll_rows(
    attendance_entries: List[DailyAttendanceEntryDocument],
) -> List[Dict[str, str]]:
    """Return manager-facing live fire-roll rows."""

    return [
        {
            "Name": attendance_entry.individual_name,
            "Company": attendance_entry.contractor_name,
            "Time In": attendance_entry.time_in.strftime("%H:%M"),
            "Vehicle Reg": attendance_entry.vehicle_registration or "—",
            "Distance Travelled": attendance_entry.distance_travelled or "—",
            "Gate Check": _format_gate_verification_display(attendance_entry),
            "Category": (
                "Uplands Employee"
                if attendance_entry.is_uplands_employee
                else "Subcontractor / Visitor"
            ),
        }
        for attendance_entry in attendance_entries
    ]


def _build_live_vehicle_rows(
    attendance_entries: List[DailyAttendanceEntryDocument],
) -> List[Dict[str, str]]:
    """Return the active vehicle-clearing rows for emergency use."""

    vehicle_rows: List[Dict[str, str]] = []
    for attendance_entry in attendance_entries:
        if not attendance_entry.vehicle_registration:
            continue
        vehicle_rows.append(
            {
                "Vehicle Reg": attendance_entry.vehicle_registration,
                "Name": attendance_entry.individual_name,
                "Company": attendance_entry.contractor_name,
            }
        )
    return vehicle_rows


def _build_competency_compliance_rows(
    repository: DocumentRepository,
    active_attendance_entries: List[DailyAttendanceEntryDocument],
) -> tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Return expired and expiring competency rows for operatives currently on site."""

    expired_rows: List[Dict[str, str]] = []
    expiring_rows: List[Dict[str, str]] = []
    today = date.today()

    for attendance_entry in active_attendance_entries:
        linked_doc_id = attendance_entry.linked_induction_doc_id
        if not linked_doc_id:
            continue
        try:
            linked_document = repository.get(linked_doc_id)
        except Exception:
            continue
        if not isinstance(linked_document, InductionDocument):
            continue
        if linked_document.status == DocumentStatus.ARCHIVED:
            continue
        if linked_document.competency_expiry_date is None:
            continue

        days_remaining = (linked_document.competency_expiry_date - today).days
        warning_row = {
            "Name": attendance_entry.individual_name,
            "Company": attendance_entry.contractor_name,
            "Expiry Date": linked_document.competency_expiry_date.strftime("%d/%m/%Y"),
            "Days Remaining": str(days_remaining),
        }
        if days_remaining < 0:
            expired_rows.append(warning_row)
        elif days_remaining <= 30:
            expiring_rows.append(warning_row)

    expired_rows.sort(key=lambda row: int(row["Days Remaining"]))
    expiring_rows.sort(key=lambda row: int(row["Days Remaining"]))
    return expired_rows, expiring_rows


def _render_competency_compliance_radar(
    repository: DocumentRepository,
    active_attendance_entries: List[DailyAttendanceEntryDocument],
) -> None:
    """Render live competency expiry warnings for personnel currently on site."""

    expired_rows, expiring_rows = _build_competency_compliance_rows(
        repository,
        active_attendance_entries,
    )
    with st.expander(
        "⚠️ Compliance Warnings",
        expanded=bool(expired_rows or expiring_rows),
    ):
        if not expired_rows and not expiring_rows:
            st.success(
                "No active on-site operatives currently have competency cards expired or expiring within 30 days."
            )
            return

        for warning_row in expired_rows:
            st.error(
                "🚨 ACTION REQUIRED: "
                f"{warning_row['Name']} has an expired card "
                f"({warning_row['Expiry Date']})."
            )
        for warning_row in expiring_rows:
            st.warning(
                f"⚠️ {warning_row['Name']} has a card expiring in "
                f"{warning_row['Days Remaining']} days "
                f"({warning_row['Expiry Date']})."
            )

        st.dataframe(
            pd.DataFrame(expired_rows + expiring_rows),
            width="stretch",
            hide_index=True,
        )


def _build_todays_attendance_log_rows(
    attendance_entries: List[DailyAttendanceEntryDocument],
) -> List[Dict[str, str]]:
    """Return today's UHSF16.09 activity rows for manager visibility."""

    return [
        {
            "Name": attendance_entry.individual_name,
            "Company": attendance_entry.contractor_name,
            "Time In": attendance_entry.time_in.strftime("%H:%M"),
            "Time Out": (
                attendance_entry.time_out.strftime("%H:%M")
                if attendance_entry.time_out is not None
                else "—"
            ),
            "Hours Worked": (
                f"{attendance_entry.hours_worked:.2f}"
                if attendance_entry.hours_worked is not None
                else "—"
            ),
            "Status": "On Site" if attendance_entry.is_on_site else "Signed Out",
            "Vehicle Reg": attendance_entry.vehicle_registration or "—",
            "Gate Check": _format_gate_verification_display(attendance_entry),
        }
        for attendance_entry in sorted(
            attendance_entries,
            key=lambda attendance_entry: attendance_entry.time_in,
            reverse=True,
        )
    ]


def _format_gate_verification_display(
    attendance_entry: DailyAttendanceEntryDocument,
) -> str:
    """Return a compact gate verification label for manager-facing tables."""

    raw_method = str(attendance_entry.gate_verification_method or "").strip().casefold()
    if raw_method == "trusted_device":
        method_label = "Trusted Device"
    elif raw_method == "gate_code":
        method_label = "Gate Code"
    elif raw_method == "gps":
        method_label = "GPS"
    elif raw_method:
        method_label = raw_method.replace("_", " ").title()
    else:
        method_label = "—"

    if attendance_entry.geofence_distance_meters is None or method_label == "—":
        return method_label
    return f"{method_label} • {attendance_entry.geofence_distance_meters:.0f}m"


def _extract_browser_geolocation_coordinates(
    geolocation_payload: Any,
) -> Optional[tuple[float, float, Optional[float]]]:
    """Return latitude, longitude, and accuracy from the browser geolocation payload."""

    if not isinstance(geolocation_payload, dict):
        return None

    coordinates = geolocation_payload.get("coords")
    source_payload = coordinates if isinstance(coordinates, dict) else geolocation_payload
    latitude = source_payload.get("latitude")
    longitude = source_payload.get("longitude")
    accuracy = source_payload.get("accuracy")
    if latitude is None or longitude is None:
        return None
    try:
        resolved_latitude = float(latitude)
        resolved_longitude = float(longitude)
        resolved_accuracy = float(accuracy) if accuracy is not None else None
    except (TypeError, ValueError):
        return None
    return resolved_latitude, resolved_longitude, resolved_accuracy


def _get_kiosk_geolocation_query_payload(
) -> tuple[Optional[tuple[float, float, Optional[float]]], str, str]:
    """Return GPS coordinates or error text captured in the kiosk URL."""

    raw_geo_error = st.query_params.get("geo_error")
    geo_error = str(raw_geo_error).strip() if raw_geo_error else ""
    raw_geo_source = st.query_params.get("geo_source")
    geo_source = str(raw_geo_source).strip().casefold() if raw_geo_source else ""
    raw_latitude = st.query_params.get("geo_lat")
    raw_longitude = st.query_params.get("geo_lng")
    raw_accuracy = st.query_params.get("geo_acc")
    if not raw_latitude or not raw_longitude:
        return None, geo_error, geo_source
    try:
        resolved_latitude = float(raw_latitude)
        resolved_longitude = float(raw_longitude)
        resolved_accuracy = float(raw_accuracy) if raw_accuracy else None
    except (TypeError, ValueError):
        return None, geo_error, geo_source
    return (resolved_latitude, resolved_longitude, resolved_accuracy), geo_error, geo_source


def _apply_project_setup_geolocation_query_payload(
    project_setup: ProjectSetup,
) -> ProjectSetup:
    """Persist site coordinates returned from the manager GPS capture page."""

    raw_latitude = st.query_params.get("setup_geo_lat")
    raw_longitude = st.query_params.get("setup_geo_lng")
    raw_error = st.query_params.get("setup_geo_error")
    if not raw_latitude or not raw_longitude:
        if raw_error:
            st.session_state["project_setup_flash"] = (
                f"GPS update failed: {str(raw_error).strip()}"
            )
            st.session_state["project_setup_flash_level"] = "warning"
            _clear_project_setup_geolocation_query_params()
        return project_setup

    try:
        resolved_latitude = float(raw_latitude)
        resolved_longitude = float(raw_longitude)
    except (TypeError, ValueError):
        _clear_project_setup_geolocation_query_params()
        return project_setup

    updated_setup = replace(
        project_setup,
        site_latitude=resolved_latitude,
        site_longitude=resolved_longitude,
    )
    updated_setup = _save_project_setup(updated_setup)
    st.session_state["project_setup"] = updated_setup
    st.session_state["project_setup_flash"] = (
        f"Site coordinates updated from this device: "
        f"{resolved_latitude:.6f}, {resolved_longitude:.6f}"
    )
    st.session_state["project_setup_flash_level"] = "success"
    _clear_project_setup_geolocation_query_params()
    return updated_setup


def _render_kiosk_geofence_gate(
    *,
    public_url: str,
    project_setup: ProjectSetup,
) -> tuple[bool, Optional[float]]:
    """Return whether kiosk attendance actions should be enabled by GPS verification."""

    st.markdown(
        "<div class='file-2-section-heading'>Allow Location Access</div>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Before signing in or out, allow GPS access so the register can confirm you are at "
        f"{project_setup.current_site_name}."
    )
    geolocation_payload, geo_error, geo_source = _get_kiosk_geolocation_query_payload()
    capture_url = _build_kiosk_geolocation_capture_url(
        public_url=public_url,
        kiosk_view="attendance",
        project_setup=project_setup,
    )
    existing_verification = _get_kiosk_geofence_session_verification(project_setup)
    if existing_verification is not None:
        st.success(_format_kiosk_verification_message(existing_verification))
        refresh_columns = st.columns([0.62, 0.38], gap="medium")
        with refresh_columns[0]:
            st.caption(
                "This session is already unlocked. Re-check GPS only if you need to refresh the location."
            )
        with refresh_columns[1]:
            st.link_button(
                "🔄 Recheck GPS",
                capture_url,
                type="secondary",
                width="stretch",
            )
        return True, existing_verification.get("distance_meters")
    geofence_link_columns = st.columns(2, gap="medium")
    with geofence_link_columns[0]:
        st.link_button(
            "📍 Allow GPS Access",
            capture_url,
            type="primary",
            width="stretch",
        )
    with geofence_link_columns[1]:
        st.link_button(
            "🔄 Retry GPS Check",
            capture_url,
            type="secondary",
            width="stretch",
        )

    if geolocation_payload is not None:
        latitude, longitude, accuracy = geolocation_payload
        st.session_state["site_attendance_geofence_requested"] = True
        distance_meters = calculate_haversine_distance_meters(
            latitude,
            longitude,
            project_setup.site_latitude,
            project_setup.site_longitude,
        )
        if distance_meters <= project_setup.geofence_radius_meters:
            verification_method = "trusted_device" if geo_source == "trusted_device" else "gps"
            verification_note = (
                "Trusted browser session"
                if verification_method == "trusted_device"
                else "Direct GPS verification"
            )
            _set_kiosk_geofence_session_verification(
                project_setup,
                method=verification_method,
                note=verification_note,
                distance_meters=distance_meters,
                accuracy_meters=accuracy,
            )
            _clear_kiosk_geolocation_query_params()
            verification_state = _get_kiosk_geofence_session_verification(project_setup)
            if verification_state is not None:
                st.success(_format_kiosk_verification_message(verification_state))
            return True, distance_meters

    if geolocation_payload is None and not geo_error:
        st.info(
            "Tap “Allow GPS Access” to open the secure GPS check page. If Safari or Chrome already has a recent on-site fix for this browser session, it should come straight back. Otherwise, tap “Use Current Location” once on the next page."
        )
        st.caption(
            "This uses a full-page browser location request for better support on iPhone, Android, tablets, and laptops."
        )
    elif geolocation_payload is None:
        st.warning(
            "⚠️ Geo-Fence Active: "
            + (geo_error or "Location access has not been granted yet.")
        )
        st.caption(
            "If this phone still refuses location, use the short-lived site gate code from the manager dashboard below."
        )
    else:
        accuracy_suffix = (
            f" GPS accuracy ±{accuracy:.0f}m." if accuracy is not None else ""
        )
        st.error(
            "⚠️ Geo-Fence Active: You are currently "
            f"{distance_meters:.0f} meters away. You must be on-site to sign the register."
            f"{accuracy_suffix}"
        )
        st.caption(
            f"Current site fence: {project_setup.current_site_name} "
            f"({project_setup.geofence_radius_meters}m radius)."
        )

    with st.expander("🔐 Can't use location on this phone?", expanded=bool(geo_error)):
        st.caption(
            "Ask the site manager for the live six-digit gate code. It refreshes automatically and is only for operatives physically at the gate."
        )
        gate_code_columns = st.columns([0.65, 0.35], gap="medium")
        with gate_code_columns[0]:
            submitted_gate_code = st.text_input(
                "Site Gate Code",
                key="site_attendance_gate_code",
                placeholder="Enter 6-digit code",
                max_chars=6,
            )
        with gate_code_columns[1]:
            st.caption("Short-lived code")
            if st.button(
                "🔓 Unlock with Gate Code",
                key="site_attendance_unlock_gate_code",
                width="stretch",
                type="primary",
            ):
                if _validate_site_gate_code(
                    project_setup.current_site_name,
                    submitted_gate_code,
                ):
                    st.session_state["site_attendance_gate_code"] = ""
                    _set_kiosk_geofence_session_verification(
                        project_setup,
                        method="gate_code",
                        note="Manager-issued gate code",
                        distance_meters=None,
                        accuracy_meters=None,
                    )
                    _clear_kiosk_geolocation_query_params()
                    st.rerun()
                st.error("That gate code is not valid or has expired.")

    return False, None


def _render_kiosk_new_starter_call_to_action() -> None:
    """Render the top-of-page route for new starters entering kiosk mode."""

    st.markdown(
        "<div class='file-2-section-heading'>New Starter?</div>",
        unsafe_allow_html=True,
    )
    st.caption(
        "If this is your first day on site and your name is not in the attendance list yet, complete your induction first."
    )
    if st.button(
        "🆕 First time on site? Click here to complete your Induction",
        key="route_kiosk_to_induction",
        width="stretch",
        type="secondary",
    ):
        _route_kiosk_to_induction_station(kiosk_view="induction")
        st.session_state["site_induction_reset_pending"] = True
        st.session_state["site_induction_canvas_revision"] = (
            int(st.session_state.get("site_induction_canvas_revision", 0)) + 1
        )
        components.html(
            """
            <script>
            const url = new URL(window.parent.location.href);
            url.searchParams.set("station", "induction");
            url.searchParams.set("mode", "kiosk");
            url.searchParams.set("kiosk_view", "induction");
            window.parent.location.href = url.toString();
            </script>
            """,
            height=0,
        )
        st.stop()
    st.divider()


def _render_site_attendance_console(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
    site_name: str,
    public_url: str,
    induction_picker_records: List[InductionDocument],
    active_attendance_entries: List[DailyAttendanceEntryDocument],
    is_kiosk: bool,
) -> None:
    """Render the UHSF16.09 daily sign-in/sign-out controls."""

    try:
        from streamlit_drawable_canvas import st_canvas
    except ImportError:
        st.error(
            "streamlit-drawable-canvas is not installed. Install dependencies and restart the app."
        )
        return

    kiosk_gps_verified = True
    if is_kiosk:
        kiosk_gps_verified, _ = _render_kiosk_geofence_gate(
            public_url=public_url,
            project_setup=project_setup,
        )
        if not kiosk_gps_verified:
            st.info("The sign-in and sign-out controls will unlock once GPS is verified on site.")

    action_mode = str(
        st.session_state.get("site_attendance_action_mode", "sign_in")
    ).strip().lower()
    if action_mode not in {"sign_in", "sign_out"}:
        action_mode = "sign_in"
        st.session_state["site_attendance_action_mode"] = action_mode

    mode_columns = st.columns(2, gap="medium")
    with mode_columns[0]:
        if st.button(
            "📥 Daily Sign-In",
            key="attendance_mode_sign_in",
            width="stretch",
            type="primary" if action_mode == "sign_in" else "secondary",
            disabled=is_kiosk and not kiosk_gps_verified,
        ):
            st.session_state["site_attendance_action_mode"] = "sign_in"
            st.rerun()
    with mode_columns[1]:
        if st.button(
            "📤 Sign Out",
            key="attendance_mode_sign_out",
            width="stretch",
            type="primary" if action_mode == "sign_out" else "secondary",
            disabled=is_kiosk and not kiosk_gps_verified,
        ):
            st.session_state["site_attendance_action_mode"] = "sign_out"
            st.rerun()

    if action_mode == "sign_in":
        search_term = st.text_input(
            "Search Existing Induction",
            key="site_attendance_worker_search",
            placeholder="Type a name or company",
            disabled=is_kiosk and not kiosk_gps_verified,
        )
        filtered_records = induction_picker_records
        if search_term.strip():
            lowered_search_term = search_term.strip().casefold()
            filtered_records = [
                induction_document
                for induction_document in induction_picker_records
                if lowered_search_term
                in _attendance_picker_label(induction_document).casefold()
            ]

        pending_prefill_doc_id = str(
            st.session_state.get("site_attendance_prefill_induction_doc_id", "") or ""
        ).strip()
        picker_options = [""] + [record.doc_id for record in filtered_records]
        resolved_selected_induction_doc_id = _resolve_attendance_sign_in_selection(
            filtered_records=filtered_records,
            current_doc_id=str(
                st.session_state.get("site_attendance_selected_induction_doc_id", "") or ""
            ).strip(),
            pending_doc_id=pending_prefill_doc_id,
        )
        if (
            st.session_state.get("site_attendance_selected_induction_doc_id")
            != resolved_selected_induction_doc_id
        ):
            st.session_state["site_attendance_selected_induction_doc_id"] = (
                resolved_selected_induction_doc_id
            )
        if pending_prefill_doc_id and resolved_selected_induction_doc_id == pending_prefill_doc_id:
            st.session_state["site_attendance_prefill_induction_doc_id"] = ""
        selected_induction_doc_id = st.selectbox(
            "Name & Company",
            options=picker_options,
            key="site_attendance_selected_induction_doc_id",
            disabled=is_kiosk and not kiosk_gps_verified,
            format_func=lambda doc_id: (
                "Select operative"
                if not doc_id
                else _attendance_picker_label(
                    next(
                        record
                        for record in induction_picker_records
                        if record.doc_id == doc_id
                    )
                )
            ),
        )
        selected_induction = next(
            (
                record
                for record in induction_picker_records
                if record.doc_id == selected_induction_doc_id
            ),
            None,
        )
        if selected_induction is not None:
            st.caption(
                f"Selected operative: {selected_induction.individual_name} | "
                f"{selected_induction.contractor_name}"
            )
        elif not filtered_records:
            st.info("No inducted operatives matched that search.")

        remembered_vehicle_registration = ""
        remembered_distance_travelled = ""
        if selected_induction is not None:
            latest_attendance_entry = _get_latest_daily_attendance_entry_for_induction(
                repository,
                selected_induction,
                site_name=site_name,
            )
            if latest_attendance_entry is not None:
                remembered_vehicle_registration = latest_attendance_entry.vehicle_registration
                remembered_distance_travelled = latest_attendance_entry.distance_travelled
        vehicle_registration_context_key = (
            "site_attendance_vehicle_registration_context_doc_id"
        )
        distance_travelled_context_key = (
            "site_attendance_distance_travelled_context_doc_id"
        )
        if (
            st.session_state.get(vehicle_registration_context_key, "")
            != selected_induction_doc_id
        ):
            st.session_state["site_attendance_vehicle_registration"] = (
                remembered_vehicle_registration
            )
            st.session_state[vehicle_registration_context_key] = (
                selected_induction_doc_id
            )
        if (
            st.session_state.get(distance_travelled_context_key, "")
            != selected_induction_doc_id
        ):
            st.session_state["site_attendance_distance_travelled"] = (
                remembered_distance_travelled
            )
            st.session_state[distance_travelled_context_key] = (
                selected_induction_doc_id
            )

        detail_columns = st.columns(2, gap="large")
        with detail_columns[0]:
            vehicle_registration = st.text_input(
                "Vehicle Details",
                key="site_attendance_vehicle_registration",
                placeholder="e.g. AB12 CDE",
                disabled=is_kiosk and not kiosk_gps_verified,
            )
            if remembered_vehicle_registration:
                st.caption(
                    f"Last known vehicle: {remembered_vehicle_registration}. Change it only if today's vehicle is different."
                )
        with detail_columns[1]:
            distance_travelled = st.text_input(
                "Distance Travelled",
                key="site_attendance_distance_travelled",
                placeholder="e.g. 14 miles",
                disabled=is_kiosk and not kiosk_gps_verified,
            )
            if remembered_distance_travelled:
                st.caption(
                    f"Last known travel: {remembered_distance_travelled}. Change it only if today's journey is different."
                )
        st.caption(
            f"Time In will be stamped automatically at {datetime.now().strftime('%H:%M')}."
        )
    else:
        if active_attendance_entries:
            sorted_active_entries = sorted(
                active_attendance_entries,
                key=lambda entry: (
                    entry.contractor_name.casefold(),
                    entry.individual_name.casefold(),
                    entry.time_in,
                ),
            )
            sign_out_search = st.text_input(
                "Search Currently On Site",
                key="site_attendance_sign_out_search",
                placeholder="Type a name, company, or vehicle",
                disabled=is_kiosk and not kiosk_gps_verified,
            )
            filtered_active_entries = sorted_active_entries
            if sign_out_search.strip():
                lowered_sign_out_search = sign_out_search.strip().casefold()
                filtered_active_entries = [
                    entry
                    for entry in sorted_active_entries
                    if lowered_sign_out_search
                    in " ".join(
                        [
                            entry.individual_name,
                            entry.contractor_name,
                            entry.vehicle_registration or "",
                            entry.time_in.strftime("%H:%M"),
                        ]
                    ).casefold()
                ]
            sign_out_options = [""] + [entry.doc_id for entry in filtered_active_entries]
            resolved_sign_out_doc_id = _resolve_attendance_sign_out_selection(
                filtered_entries=filtered_active_entries,
                current_doc_id=str(
                    st.session_state.get("site_attendance_selected_sign_out_doc_id", "") or ""
                ).strip(),
            )
            if (
                st.session_state.get("site_attendance_selected_sign_out_doc_id")
                != resolved_sign_out_doc_id
            ):
                st.session_state["site_attendance_selected_sign_out_doc_id"] = (
                    resolved_sign_out_doc_id
                )
            selected_sign_out_doc_id = st.selectbox(
                "Currently On Site",
                options=sign_out_options,
                key="site_attendance_selected_sign_out_doc_id",
                disabled=is_kiosk and not kiosk_gps_verified,
                format_func=lambda doc_id: (
                    "Select operative"
                    if not doc_id
                    else _attendance_sign_out_label(
                        next(
                            entry
                            for entry in filtered_active_entries
                            if entry.doc_id == doc_id
                        )
                    )
                ),
            )
            selected_sign_out_entry = next(
                (
                    entry
                    for entry in filtered_active_entries
                    if entry.doc_id == selected_sign_out_doc_id
                ),
                None,
            )
            if selected_sign_out_entry is not None:
                vehicle_suffix = (
                    f" | Vehicle {selected_sign_out_entry.vehicle_registration}"
                    if selected_sign_out_entry.vehicle_registration
                    else ""
                )
                st.caption(
                    f"Selected operative: {selected_sign_out_entry.individual_name} | "
                    f"{selected_sign_out_entry.contractor_name} | "
                    f"Signed in at {selected_sign_out_entry.time_in.strftime('%H:%M')}"
                    f"{vehicle_suffix}"
                )
            elif sign_out_search.strip() and not filtered_active_entries:
                st.info("No on-site operatives matched that search.")
            st.caption(
                f"Currently on site: {len(active_attendance_entries)} | Matches shown: {len(filtered_active_entries)}"
            )
        else:
            selected_sign_out_doc_id = ""
            st.info("Nobody is currently signed in.")
        st.caption(
            f"Time Out will be stamped automatically at {datetime.now().strftime('%H:%M')}."
        )

    st.markdown(
        "<div class='file-2-section-heading'>Safety Signature</div>",
        unsafe_allow_html=True,
    )
    canvas_revision = int(st.session_state.get("site_attendance_canvas_revision", 0))
    canvas_result = st_canvas(
        update_streamlit=True,
        key=f"site_attendance_canvas_{canvas_revision}",
        height=200,
        width=420,
        stroke_width=3,
        stroke_color="#000000",
        background_color="#ffffff",
        drawing_mode="freedraw",
        display_toolbar=False,
    )

    action_columns = (
        st.columns([0.28, 0.28, 0.44], gap="medium")
        if is_kiosk
        else st.columns([0.22, 0.22, 0.56], gap="large")
    )
    with action_columns[0]:
        if st.button(
            "🧽 Clear Signature",
            key="clear_site_attendance_signature",
            width="stretch",
            type="secondary",
            disabled=is_kiosk and not kiosk_gps_verified,
        ):
            st.session_state["site_attendance_canvas_revision"] = canvas_revision + 1
            st.rerun()
    with action_columns[1]:
        if st.button(
            "↺ Reset Form",
            key="reset_site_attendance_form",
            width="stretch",
            type="secondary",
            disabled=is_kiosk and not kiosk_gps_verified,
        ):
            st.session_state["site_attendance_reset_pending"] = True
            st.session_state["site_attendance_canvas_revision"] = canvas_revision + 1
            st.rerun()

    submit_label = (
        "✅ Complete Sign-In"
        if action_mode == "sign_in"
        else "✅ Complete Sign-Out"
    )
    with action_columns[2]:
        if st.button(
            submit_label,
            key="submit_site_attendance",
            width="stretch",
            disabled=is_kiosk and not kiosk_gps_verified,
        ):
            try:
                if action_mode == "sign_in":
                    if selected_induction is None:
                        raise ValidationError("Select an inducted operative before signing in.")
                    current_gate_verification = _get_kiosk_geofence_session_verification(
                        project_setup
                    )
                    logged_entry = create_daily_attendance_sign_in(
                        repository,
                        site_name=site_name,
                        induction_document=selected_induction,
                        vehicle_registration=vehicle_registration,
                        distance_travelled=distance_travelled,
                        signature_image_data=canvas_result.image_data,
                        gate_verification_method=(
                            str(
                                (current_gate_verification or {}).get("method", "")
                                or ""
                            ).strip()
                        ),
                        gate_verification_note=(
                            str(
                                (current_gate_verification or {}).get("note", "")
                                or ""
                            ).strip()
                        ),
                        geofence_distance_meters=(
                            (current_gate_verification or {}).get("distance_meters")
                        ),
                    )
                    flash_message = (
                        f"{logged_entry.attendance_entry.individual_name} signed in at "
                        f"{logged_entry.attendance_entry.time_in.strftime('%H:%M')}."
                    )
                    kiosk_message = (
                        f"Welcome to Site, {logged_entry.attendance_entry.individual_name}!"
                    )
                else:
                    if not selected_sign_out_doc_id:
                        raise ValidationError("Select an operative before signing out.")
                    logged_entry = complete_daily_attendance_sign_out(
                        repository,
                        attendance_doc_id=selected_sign_out_doc_id,
                        signature_image_data=canvas_result.image_data,
                    )
                    flash_message = (
                        f"{logged_entry.attendance_entry.individual_name} signed out at "
                        f"{logged_entry.attendance_entry.time_out.strftime('%H:%M')} "
                        f"({logged_entry.attendance_entry.hours_worked:.2f} hrs)."
                    )
                    kiosk_message = (
                        f"Signed Out. Safe journey, {logged_entry.attendance_entry.individual_name}."
                    )
            except ValidationError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"Unable to update the attendance register: {exc}")
            else:
                st.session_state["site_attendance_reset_pending"] = True
                st.session_state["site_attendance_canvas_revision"] = canvas_revision + 1
                if is_kiosk:
                    st.session_state["site_attendance_kiosk_complete_message"] = kiosk_message
                    st.session_state["site_attendance_kiosk_complete_at"] = time.time()
                else:
                    st.session_state["site_attendance_flash"] = flash_message
                st.rerun()

    st.caption(ATTENDANCE_FORM_METADATA)


def _render_live_fire_roll_panel(
    active_attendance_entries: List[DailyAttendanceEntryDocument],
) -> None:
    """Render the manager-facing live fire roll metrics and tables."""

    subcontractor_count = sum(
        1
        for attendance_entry in active_attendance_entries
        if not attendance_entry.is_uplands_employee
    )
    uplands_employee_count = sum(
        1 for attendance_entry in active_attendance_entries if attendance_entry.is_uplands_employee
    )
    active_vehicle_rows = _build_live_vehicle_rows(active_attendance_entries)
    fire_roll_rows = _build_live_fire_roll_rows(active_attendance_entries)

    fire_roll_metric_columns = st.columns(3, gap="large")
    with fire_roll_metric_columns[0]:
        _render_metric_card(
            title="On Site",
            icon="🔥",
            value=str(len(active_attendance_entries)),
            caption="Operatives currently signed in and available on the fire roll.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Vehicle regs live: <strong>{len(active_vehicle_rows)}</strong>"
                "</div>"
            ),
        )
    with fire_roll_metric_columns[1]:
        _render_metric_card(
            title="Subcontractors & Visitors",
            icon="👷",
            value=str(subcontractor_count),
            caption="Non-Uplands personnel currently on site.",
            body_html="<div class='data-card-subtext'>Live fire roll</div>",
        )
    with fire_roll_metric_columns[2]:
        _render_metric_card(
            title="Uplands Employees",
            icon="🏗️",
            value=str(uplands_employee_count),
            caption="Uplands personnel currently on site.",
            body_html="<div class='data-card-subtext'>Live fire roll</div>",
        )

    table_columns = st.columns([1.65, 1], gap="large")
    with table_columns[0]:
        st.markdown(
            "<div class='file-2-section-heading'>Live Fire Roll</div>",
            unsafe_allow_html=True,
        )
        if fire_roll_rows:
            st.dataframe(pd.DataFrame(fire_roll_rows), width="stretch", hide_index=True)
        else:
            st.info("Nobody is currently signed in on today's live fire roll.")
    with table_columns[1]:
        st.markdown(
            "<div class='file-2-section-heading'>Active Vehicle Regs</div>",
            unsafe_allow_html=True,
        )
        if active_vehicle_rows:
            st.dataframe(
                pd.DataFrame(active_vehicle_rows),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info("No vehicle registrations are currently active on site.")


def _render_site_gate_fallback_panel(project_setup: ProjectSetup) -> None:
    """Render the manager-only fallback code for phones that refuse geolocation."""

    current_gate_code, minutes_remaining = _get_site_gate_code(
        project_setup.current_site_name
    )
    with st.expander("🔐 Site Gate Fallback Code", expanded=False):
        st.caption(
            "Use this only when an operative is physically at the gate and their phone browser refuses location access."
        )
        code_columns = st.columns([0.7, 0.3], gap="large")
        with code_columns[0]:
            _render_metric_card(
                title="Current Gate Code",
                icon="🔐",
                value=current_gate_code,
                caption="Short-lived fallback for location failures on phone browsers.",
                body_html=(
                    "<div class='data-card-subtext'>"
                    f"Refreshes in <strong>{minutes_remaining} min</strong>"
                    "</div>"
                ),
            )
        with code_columns[1]:
            st.markdown(
                (
                    "<div class='panel-card' style='height:100%; display:flex; "
                    "flex-direction:column; justify-content:center;'>"
                    "<div class='panel-heading'>Use</div>"
                    "<div class='panel-title'>Tell the operative this code</div>"
                    "<div class='panel-caption'>"
                    "The kiosk fallback accepts the live code and the previous slot."
                    "</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            st.code(current_gate_code)


def _render_todays_attendance_activity_panel(
    attendance_entries: List[DailyAttendanceEntryDocument],
) -> None:
    """Render today's full attendance activity including signed-out operatives."""

    st.markdown(
        "<div class='file-2-section-heading'>Today's Attendance Activity</div>",
        unsafe_allow_html=True,
    )
    if attendance_entries:
        st.dataframe(
            pd.DataFrame(_build_todays_attendance_log_rows(attendance_entries)),
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("No UHSF16.09 attendance activity has been logged for today yet.")


def _format_gate_verification_method_option(method_value: str) -> str:
    """Return the manager-facing label for one gate verification method value."""

    normalized_value = str(method_value or "").strip().casefold()
    if normalized_value == "gps":
        return "GPS"
    if normalized_value == "trusted_device":
        return "Trusted Device"
    if normalized_value == "gate_code":
        return "Gate Code"
    if normalized_value == "manager_correction":
        return "Manager Correction"
    if not normalized_value:
        return "Not Recorded"
    return normalized_value.replace("_", " ").title()


def _build_gate_verification_method_options(current_method: str) -> List[str]:
    """Return gate verification method options while preserving unknown legacy values."""

    options = ["", "gps", "trusted_device", "gate_code", "manager_correction"]
    normalized_current = str(current_method or "").strip()
    if normalized_current and normalized_current not in options:
        options.append(normalized_current)
    return options


def _render_manager_attendance_correction_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
) -> None:
    """Render manager-side attendance recovery controls for fixing gate mistakes."""

    st.markdown(
        "<div class='file-2-section-heading'>Manager Corrections</div>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Fix a mistaken sign-in or sign-out without leaving the app. You can update company, vehicle, travel, gate method, times, or remove one bad record entirely."
    )

    correction_date = st.date_input(
        "Correction Date",
        value=st.session_state.get("site_attendance_correction_date", date.today()),
        key="site_attendance_correction_date",
    )
    correction_search = st.text_input(
        "Search Attendance Record",
        key="site_attendance_correction_search",
        placeholder="Search by name, company, vehicle, or time",
    ).strip()

    correction_entries = list_daily_attendance_entries(
        repository,
        site_name=project_setup.current_site_name,
        on_date=correction_date,
        active_only=False,
    )
    filtered_entries = [
        entry
        for entry in correction_entries
        if not correction_search
        or correction_search.casefold() in entry.individual_name.casefold()
        or correction_search.casefold() in entry.contractor_name.casefold()
        or correction_search.casefold() in entry.vehicle_registration.casefold()
        or correction_search.casefold() in entry.time_in.strftime("%H:%M").casefold()
        or (
            entry.time_out is not None
            and correction_search.casefold()
            in entry.time_out.strftime("%H:%M").casefold()
        )
    ]

    correction_metrics = st.columns(3, gap="large")
    with correction_metrics[0]:
        _render_inline_metric(
            "Matching Entries",
            str(len(filtered_entries)),
            icon="🧰",
        )
    with correction_metrics[1]:
        _render_inline_metric(
            "On Site",
            str(sum(1 for entry in filtered_entries if entry.is_on_site)),
            icon="🔥",
        )
    with correction_metrics[2]:
        _render_inline_metric(
            "Signed Out",
            str(sum(1 for entry in filtered_entries if not entry.is_on_site)),
            icon="📤",
        )

    if not filtered_entries:
        st.info("No attendance records match this date and search filter.")
        return

    selection_key = "site_attendance_correction_doc_id"
    current_doc_id = str(st.session_state.get(selection_key, "")).strip()
    resolved_doc_id = _resolve_attendance_correction_selection(
        filtered_entries=filtered_entries,
        current_doc_id=current_doc_id,
    )
    if st.session_state.get(selection_key) != resolved_doc_id:
        st.session_state[selection_key] = resolved_doc_id

    selected_doc_id = st.selectbox(
        "Select Attendance Record",
        options=[""] + [entry.doc_id for entry in filtered_entries],
        format_func=lambda doc_id: (
            "Choose a saved attendance entry"
            if not doc_id
            else _attendance_manager_correction_label(
                next(entry for entry in filtered_entries if entry.doc_id == doc_id)
            )
        ),
        key=selection_key,
    )
    if not selected_doc_id:
        return

    selected_entry = next(
        entry for entry in filtered_entries if entry.doc_id == selected_doc_id
    )
    field_key_prefix = f"attendance-correction-{selected_entry.doc_id}"

    summary_columns = st.columns(4, gap="medium")
    with summary_columns[0]:
        _render_inline_metric("Operative", selected_entry.individual_name, icon="👤")
    with summary_columns[1]:
        _render_inline_metric(
            "Time In",
            selected_entry.time_in.strftime("%d/%m %H:%M"),
            icon="📥",
        )
    with summary_columns[2]:
        _render_inline_metric(
            "Status",
            "On Site" if selected_entry.is_on_site else "Signed Out",
            icon="📍",
        )
    with summary_columns[3]:
        _render_inline_metric(
            "Hours",
            (
                f"{selected_entry.hours_worked:.2f}"
                if selected_entry.hours_worked is not None
                else "—"
            ),
            icon="⏱️",
        )

    with st.form(f"{field_key_prefix}-form"):
        st.markdown(
            "<div class='file-2-section-heading'>Edit Selected Record</div>",
            unsafe_allow_html=True,
        )
        identity_columns = st.columns(2, gap="medium")
        with identity_columns[0]:
            company_name = st.text_input(
                "Company / Contractor",
                value=selected_entry.contractor_name,
                key=f"{field_key_prefix}-company",
            )
        with identity_columns[1]:
            vehicle_registration = st.text_input(
                "Vehicle Registration",
                value=selected_entry.vehicle_registration,
                key=f"{field_key_prefix}-vehicle",
            )

        detail_columns = st.columns(2, gap="medium")
        with detail_columns[0]:
            distance_travelled = st.text_input(
                "Distance Travelled",
                value=selected_entry.distance_travelled,
                key=f"{field_key_prefix}-distance",
            )
        with detail_columns[1]:
            gate_method_options = _build_gate_verification_method_options(
                selected_entry.gate_verification_method
            )
            current_gate_method = str(selected_entry.gate_verification_method or "").strip()
            gate_method_index = (
                gate_method_options.index(current_gate_method)
                if current_gate_method in gate_method_options
                else 0
            )
            gate_verification_method = st.selectbox(
                "Gate Verification",
                options=gate_method_options,
                index=gate_method_index,
                format_func=_format_gate_verification_method_option,
                key=f"{field_key_prefix}-gate-method",
            )

        gate_note = st.text_input(
            "Gate Verification Note",
            value=selected_entry.gate_verification_note,
            key=f"{field_key_prefix}-gate-note",
        )

        timing_columns = st.columns(3, gap="medium")
        with timing_columns[0]:
            corrected_date = st.date_input(
                "Attendance Date",
                value=selected_entry.time_in.date(),
                key=f"{field_key_prefix}-date",
            )
        with timing_columns[1]:
            corrected_time_in = st.time_input(
                "Time In",
                value=selected_entry.time_in.time().replace(second=0, microsecond=0),
                key=f"{field_key_prefix}-time-in",
            )
        with timing_columns[2]:
            status_label = st.radio(
                "Entry Status",
                options=["On Site", "Signed Out"],
                index=0 if selected_entry.is_on_site else 1,
                horizontal=True,
                key=f"{field_key_prefix}-status",
            )

        corrected_time_out: Optional[datetime] = None
        if status_label == "Signed Out":
            sign_out_columns = st.columns(2, gap="medium")
            existing_time_out = selected_entry.time_out or selected_entry.time_in
            with sign_out_columns[0]:
                corrected_time_out_date = st.date_input(
                    "Time Out Date",
                    value=existing_time_out.date(),
                    key=f"{field_key_prefix}-time-out-date",
                )
            with sign_out_columns[1]:
                corrected_time_out_time = st.time_input(
                    "Time Out",
                    value=existing_time_out.time().replace(second=0, microsecond=0),
                    key=f"{field_key_prefix}-time-out-time",
                )
            corrected_time_out = datetime.combine(
                corrected_time_out_date,
                corrected_time_out_time,
            )
        corrected_time_in_datetime = datetime.combine(corrected_date, corrected_time_in)

        save_correction = st.form_submit_button(
            "💾 Save Attendance Correction",
            width="stretch",
        )

    if save_correction:
        try:
            updated_entry = update_daily_attendance_entry(
                repository,
                attendance_doc_id=selected_entry.doc_id,
                contractor_name=company_name,
                vehicle_registration=vehicle_registration,
                distance_travelled=distance_travelled,
                gate_verification_method=gate_verification_method,
                gate_verification_note=gate_note,
                time_in=corrected_time_in_datetime,
                time_out=corrected_time_out,
            )
        except ValidationError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.error(f"Unable to save the attendance correction: {exc}")
        else:
            st.session_state["site_attendance_correction_date"] = (
                updated_entry.attendance_date
            )
            st.session_state["site_attendance_edit_flash"] = (
                f"Updated attendance record for {updated_entry.individual_name}."
            )
            st.rerun()

    signature_summary: List[str] = []
    if selected_entry.sign_in_signature_path:
        signature_summary.append("Sign-in signature saved")
    if selected_entry.sign_out_signature_path:
        signature_summary.append("Sign-out signature saved")
    if not signature_summary:
        signature_summary.append("No signature files linked")
    st.caption(" • ".join(signature_summary))

    delete_pending_doc_id = str(
        st.session_state.get("site_attendance_delete_pending_doc_id", "")
    ).strip()
    delete_columns = st.columns([1.2, 4.8], gap="medium")
    with delete_columns[0]:
        if st.button(
            "🗑️ Remove This Record",
            key=f"{field_key_prefix}-delete",
            width="stretch",
            type="secondary",
        ):
            st.session_state["site_attendance_delete_pending_doc_id"] = selected_entry.doc_id
            st.rerun()
    with delete_columns[1]:
        st.caption(
            "Use remove only when the operative signed in or out against the wrong record and the full attendance entry needs to be deleted."
        )

    if delete_pending_doc_id == selected_entry.doc_id:
        st.warning(
            "Remove this attendance record and any linked signature files? This cannot be undone from inside the app."
        )
        confirm_columns = st.columns([1.2, 1.0, 4.0], gap="small")
        if confirm_columns[0].button(
            "Confirm Remove",
            key=f"{field_key_prefix}-confirm-delete",
            width="stretch",
        ):
            deleted_paths = repository.delete_document_and_files(selected_entry.doc_id)
            st.session_state.pop("site_attendance_delete_pending_doc_id", None)
            st.session_state["site_attendance_edit_flash"] = (
                f"Removed attendance record for {selected_entry.individual_name}."
                + (
                    f" Removed {len(deleted_paths)} linked file(s)."
                    if deleted_paths
                    else " No linked files were present on disk."
                )
            )
            st.rerun()
        if confirm_columns[1].button(
            "Cancel",
            key=f"{field_key_prefix}-cancel-delete",
            width="stretch",
        ):
            st.session_state.pop("site_attendance_delete_pending_doc_id", None)
            st.rerun()


def _coerce_site_diary_table_rows(
    raw_rows: Any,
    *,
    columns: List[str],
) -> List[Dict[str, Any]]:
    """Return clean row dictionaries from one Streamlit data editor value."""

    if isinstance(raw_rows, pd.DataFrame):
        records = raw_rows.to_dict("records")
    elif isinstance(raw_rows, list):
        records = raw_rows
    else:
        records = []

    cleaned_rows: List[Dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        cleaned_rows.append({column: record.get(column, "") for column in columns})
    return cleaned_rows


def _render_browser_dictation_button(
    *,
    target_state_key: str,
    component_key: str,
    field_label: str,
) -> None:
    """Render one browser-native speech-to-text control for a diary text field."""

    component_id = f"dictation-{component_key}"
    target_diary_date = _extract_site_diary_date_from_state_key(target_state_key)
    return_path = "/?station=FILE%202&file2_view=diary"
    if target_diary_date is not None:
        return_path += (
            f"&site_diary_date={quote(target_diary_date.isoformat(), safe='')}"
        )
    popup_url = (
        "/gps/voice-capture.html"
        f"?v=20260315c&target={quote(target_state_key, safe='')}"
        f"&label={quote(field_label, safe='')}"
        f"&return={quote(return_path, safe='/?=&')}"
    )
    js_expression = f"""
    (() => {{
        setFrameHeight(88);
        const targetKey = {json.dumps(target_state_key)};
        const rootId = {json.dumps(component_id)};
        const popupBaseUrl = {json.dumps(popup_url)};
        document.body.style.margin = "0";
        document.body.style.background = "transparent";

        if (!document.getElementById(rootId)) {{
            document.body.innerHTML = `
                <style>
                    .dictation-shell {{
                        display: flex;
                        flex-direction: column;
                        gap: 0.35rem;
                        align-items: stretch;
                        font-family: "Avenir Next", "Segoe UI", sans-serif;
                    }}
                    .dictation-button {{
                        appearance: none;
                        border: 1px solid rgba(209, 34, 142, 0.18);
                        border-radius: 999px;
                        background: linear-gradient(135deg, rgba(209,34,142,0.12), rgba(91,141,239,0.14));
                        color: {TEXT_DARK};
                        cursor: pointer;
                        font: 800 0.9rem/1.1 "Avenir Next", "Segoe UI", sans-serif;
                        padding: 0.72rem 0.95rem;
                        transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease;
                        width: 100%;
                    }}
                    .dictation-button:hover {{
                        border-color: rgba(209, 34, 142, 0.28);
                        box-shadow: 0 10px 22px rgba(18, 24, 38, 0.10);
                        transform: translateY(-1px);
                    }}
                    .dictation-button:disabled {{
                        cursor: not-allowed;
                        opacity: 0.68;
                        transform: none;
                        box-shadow: none;
                    }}
                    .dictation-status {{
                        color: {TEXT_MUTED};
                        font-size: 0.74rem;
                        line-height: 1.35;
                        min-height: 1.15rem;
                        text-align: center;
                    }}
                </style>
                <div class="dictation-shell">
                    <button id="${{rootId}}" class="dictation-button" type="button">🎙️ Dictate</button>
                    <div id="${{rootId}}-status" class="dictation-status">Open a secure dictation window to speak into this diary field.</div>
                </div>
            `;
        }}

        const button = document.getElementById(rootId);
        const status = document.getElementById(`${{rootId}}-status`);
        if (!button.dataset.bound) {{
            button.dataset.bound = "true";
            button.addEventListener("click", () => {{
                const hostWindow = window.top || window.parent || window;
                const popupUrl = popupBaseUrl;
                const popup = hostWindow.open(
                    popupUrl,
                    `uplands-dictation-${{targetKey.replace(/[^a-z0-9]+/gi, "-")}}`,
                    "popup=yes,width=620,height=760,resizable=yes,scrollbars=yes"
                );
                if (!popup) {{
                    const message = "Dictation window was blocked. Allow pop-ups for this site and try again.";
                    status.textContent = message;
                    return;
                }}
                status.textContent = "Dictation window opened. Speak there, then it will send the transcript back here.";
                try {{
                    popup.focus();
                }} catch (error) {{
                    console.error(error);
                }}
            }});
        }}
        return null;
    }})()
    """
    streamlit_js_eval(js_expressions=js_expression, key=component_key)


def _apply_site_diary_dictation_result(
    dictation_result: Any,
    *,
    friendly_label: str,
) -> None:
    """Append fresh dictation text into the requested Site Diary field."""

    if not isinstance(dictation_result, dict):
        return

    target_state_key = str(dictation_result.get("target", "")).strip()
    if not target_state_key.startswith("file2_site_diary_"):
        return

    nonce = str(dictation_result.get("nonce", "")).strip()
    if not nonce:
        return

    handled_nonce_key = f"{target_state_key}__dictation_nonce"
    if st.session_state.get(handled_nonce_key) == nonce:
        return
    st.session_state[handled_nonce_key] = nonce

    error_message = str(dictation_result.get("error", "")).strip()
    if error_message:
        st.session_state["file2_site_diary_dictation_warning"] = error_message
        return

    transcript = str(dictation_result.get("transcript", "")).strip()
    if not transcript:
        return

    existing_text = str(st.session_state.get(target_state_key, "")).strip()
    if existing_text:
        st.session_state[target_state_key] = f"{existing_text}\n{transcript}"
    else:
        st.session_state[target_state_key] = transcript
    st.session_state["file2_site_diary_dictation_flash"] = (
        f"Dictation added to {friendly_label}."
    )


def _derive_uplands_site_diary_counts(
    contractor_rows: List[Dict[str, Any]],
) -> tuple[int, int]:
    """Return the Uplands day/night counts derived from the contractor table."""

    uplands_days = 0
    uplands_nights = 0
    for contractor_row in contractor_rows:
        company_name = str(contractor_row.get("company", "")).strip()
        if not company_name or not _is_uplands_company(company_name):
            continue
        try:
            uplands_days += int(contractor_row.get("days", 0) or 0)
        except (TypeError, ValueError):
            pass
        try:
            uplands_nights += int(contractor_row.get("nights", 0) or 0)
        except (TypeError, ValueError):
            pass
    return uplands_days, uplands_nights


def _get_latest_site_diary_document(
    repository: DocumentRepository,
    *,
    site_name: str,
    target_date: date,
) -> Optional[SiteDiaryDocument]:
    """Return the latest saved UHSF15.63 diary for one site/date."""

    matching_diaries = [
        document
        for document in repository.list_documents(
            document_type=SiteDiaryDocument.document_type,
            site_name=site_name,
        )
        if isinstance(document, SiteDiaryDocument) and document.date == target_date
    ]
    if not matching_diaries:
        return None
    return max(
        matching_diaries,
        key=lambda diary_document: diary_document.created_at,
    )


def _build_site_diary_history_rows(
    site_diary_documents: List[SiteDiaryDocument],
) -> List[Dict[str, str]]:
    """Return UI rows for the saved UHSF15.63 diary history."""

    return [
        {
            "Date": site_diary_document.date.strftime("%d/%m/%Y"),
            "Uplands Days": str(site_diary_document.uplands_days),
            "Uplands Nights": str(site_diary_document.uplands_nights),
            "Contractors": str(len(site_diary_document.contractors)),
            "Visitors": str(len(site_diary_document.visitors)),
            "Saved": site_diary_document.created_at.strftime("%d/%m/%Y %H:%M"),
        }
        for site_diary_document in sorted(
            site_diary_documents,
            key=lambda site_diary_document: (
                site_diary_document.date,
                site_diary_document.created_at,
            ),
            reverse=True,
        )
    ]


def _render_file_2_site_diary_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
) -> None:
    """Render the UHSF15.63 Daily Site Diary manager station."""

    site_name = project_setup.current_site_name
    _apply_site_diary_form_reset_if_pending(site_name)
    diary_flash_message = st.session_state.pop("file2_site_diary_flash", "")
    if diary_flash_message:
        st.success(diary_flash_message)
    dictation_flash_message = st.session_state.pop(
        "file2_site_diary_dictation_flash",
        "",
    )
    if dictation_flash_message:
        st.success(dictation_flash_message)
    dictation_warning_message = st.session_state.pop(
        "file2_site_diary_dictation_warning",
        "",
    )
    if dictation_warning_message:
        st.warning(dictation_warning_message)
    target_date = st.date_input(
        "Diary Date",
        value=date.today(),
        key="file2_site_diary_date",
    )
    contractor_snapshot = get_daily_contractor_headcount(
        repository,
        site_name,
        target_date,
    )
    saved_diary = _get_latest_site_diary_document(
        repository,
        site_name=site_name,
        target_date=target_date,
    )
    all_site_diaries = [
        document
        for document in repository.list_documents(
            document_type=SiteDiaryDocument.document_type,
            site_name=site_name,
        )
        if isinstance(document, SiteDiaryDocument)
    ]
    draft_text_fields = _load_site_diary_text_draft(site_name, target_date)
    contractor_editor_default = (
        saved_diary.contractors if saved_diary is not None else contractor_snapshot
    )
    visitor_editor_default = saved_diary.visitors if saved_diary is not None else []
    default_uplands_days, default_uplands_nights = _derive_uplands_site_diary_counts(
        contractor_editor_default
    )
    incidents_state_key = f"file2_site_diary_incidents_{target_date.isoformat()}"
    handovers_state_key = f"file2_site_diary_handovers_{target_date.isoformat()}"
    comments_state_key = f"file2_site_diary_comments_{target_date.isoformat()}"
    if incidents_state_key not in st.session_state:
        st.session_state[incidents_state_key] = (
            draft_text_fields.get("incidents_details")
            or (saved_diary.incidents_details if saved_diary is not None else "")
        )
    if handovers_state_key not in st.session_state:
        st.session_state[handovers_state_key] = (
            draft_text_fields.get("area_handovers")
            or (saved_diary.area_handovers if saved_diary is not None else "")
        )
    if comments_state_key not in st.session_state:
        st.session_state[comments_state_key] = (
            draft_text_fields.get("todays_comments")
            or (saved_diary.todays_comments if saved_diary is not None else "")
        )

    _render_workspace_hero(
        icon="📔",
        kicker="UHSF15.63",
        title="Daily Site Diary",
        caption="Capture the daily site story, pull contractor numbers from the live gate, and save the official diary into File 2.",
    )

    summary_columns = st.columns(4)
    with summary_columns[0]:
        _render_inline_metric(
            "Gate Companies",
            str(len(contractor_snapshot)),
            icon="🏢",
        )
    with summary_columns[1]:
        _render_inline_metric(
            "Gate Headcount",
            str(sum(int(row.get("days", 0) or 0) for row in contractor_snapshot)),
            icon="👷",
        )
    with summary_columns[2]:
        _render_inline_metric(
            "Saved Diaries",
            str(len(all_site_diaries)),
            icon="📚",
        )
    with summary_columns[3]:
        _render_inline_metric(
            "Loaded Record",
            target_date.strftime("%d/%m/%Y") if saved_diary is not None else "New",
            icon="📝",
        )

    if saved_diary is not None:
        st.caption(
            f"Loaded the latest saved diary for {target_date:%d/%m/%Y}. Regenerate it to refresh the file."
        )

    st.divider()
    _render_workspace_zone_heading(
        "Primary Action",
        "The contractor table is pulled straight from the live attendance gate for the selected day. Review it, add visitors, complete the diary notes, then generate the official Word document.",
    )
    st.markdown("<div id='site-diary-editor'></div>", unsafe_allow_html=True)
    st.markdown("**Live Gate Snapshot**")
    if contractor_snapshot:
        st.dataframe(
            pd.DataFrame(contractor_snapshot),
            hide_index=True,
            width="stretch",
        )
    else:
        st.info("No active sign-ins were found for that date. You can still complete the diary manually.")

    contractor_rows = st.data_editor(
        pd.DataFrame(
            contractor_editor_default or [{"company": "", "days": 0, "nights": 0}]
        ),
        key=f"file2_site_diary_contractors_{target_date.isoformat()}",
        hide_index=True,
        width="stretch",
        num_rows="dynamic",
        column_config={
            "company": st.column_config.TextColumn("Company"),
            "days": st.column_config.NumberColumn("Days", min_value=0, step=1),
            "nights": st.column_config.NumberColumn("Nights", min_value=0, step=1),
        },
    )

    personnel_columns = st.columns(3, gap="large")
    with personnel_columns[0]:
        uplands_days = st.number_input(
            "Uplands Days",
            min_value=0,
            step=1,
            value=saved_diary.uplands_days if saved_diary is not None else default_uplands_days,
            key=f"file2_site_diary_uplands_days_{target_date.isoformat()}",
        )
    with personnel_columns[1]:
        uplands_nights = st.number_input(
            "Uplands Nights",
            min_value=0,
            step=1,
            value=saved_diary.uplands_nights if saved_diary is not None else default_uplands_nights,
            key=f"file2_site_diary_uplands_nights_{target_date.isoformat()}",
        )
    with personnel_columns[2]:
        skip_exchange = st.text_input(
            "Skip Exchange",
            value=saved_diary.skip_exchange if saved_diary is not None else "",
            placeholder="Type / reference",
            key=f"file2_site_diary_skip_exchange_{target_date.isoformat()}",
        )

    fire_columns = st.columns(4, gap="medium")
    with fire_columns[0]:
        fire_day_on = st.checkbox(
            "Fire Day On",
            value=saved_diary.fire_day_on if saved_diary is not None else False,
            key=f"file2_site_diary_fire_day_on_{target_date.isoformat()}",
        )
    with fire_columns[1]:
        fire_day_off = st.checkbox(
            "Fire Day Off",
            value=saved_diary.fire_day_off if saved_diary is not None else False,
            key=f"file2_site_diary_fire_day_off_{target_date.isoformat()}",
        )
    with fire_columns[2]:
        fire_night_on = st.checkbox(
            "Fire Night On",
            value=saved_diary.fire_night_on if saved_diary is not None else False,
            key=f"file2_site_diary_fire_night_on_{target_date.isoformat()}",
        )
    with fire_columns[3]:
        fire_night_off = st.checkbox(
            "Fire Night Off",
            value=saved_diary.fire_night_off if saved_diary is not None else False,
            key=f"file2_site_diary_fire_night_off_{target_date.isoformat()}",
        )

    weather_columns = st.columns(3, gap="medium")
    with weather_columns[0]:
        weather_dry = st.checkbox(
            "Weather Dry",
            value=saved_diary.weather_dry if saved_diary is not None else True,
            key=f"file2_site_diary_weather_dry_{target_date.isoformat()}",
        )
    with weather_columns[1]:
        weather_mixed = st.checkbox(
            "Weather Mixed",
            value=saved_diary.weather_mixed if saved_diary is not None else False,
            key=f"file2_site_diary_weather_mixed_{target_date.isoformat()}",
        )
    with weather_columns[2]:
        weather_wet = st.checkbox(
            "Weather Wet",
            value=saved_diary.weather_wet if saved_diary is not None else False,
            key=f"file2_site_diary_weather_wet_{target_date.isoformat()}",
        )

    incidents_header_columns = st.columns([4.3, 1.7], gap="medium")
    with incidents_header_columns[0]:
        st.markdown("**Incidents Details**")
    with incidents_header_columns[1]:
        _render_browser_dictation_button(
            target_state_key=incidents_state_key,
            component_key=f"site_diary_incidents_dictation_{target_date.isoformat()}",
            field_label="Incidents Details",
        )
    incidents_details = st.text_area(
        "Incidents Details",
        key=incidents_state_key,
        height=110,
        label_visibility="collapsed",
        placeholder="Speak or type any incidents, near misses, or notable safety events here.",
        on_change=_persist_site_diary_text_draft,
        args=(
            site_name,
            target_date,
            incidents_state_key,
            handovers_state_key,
            comments_state_key,
        ),
    )
    hs_reported_tick = st.checkbox(
        "Sent to H&S Department",
        value=saved_diary.hs_reported_tick if saved_diary is not None else False,
        key=f"file2_site_diary_hs_reported_tick_{target_date.isoformat()}",
    )

    st.markdown("**Visitors**")
    visitor_rows = st.data_editor(
        pd.DataFrame(visitor_editor_default or [{"name": "", "company": ""}]),
        key=f"file2_site_diary_visitors_{target_date.isoformat()}",
        hide_index=True,
        width="stretch",
        num_rows="dynamic",
        column_config={
            "name": st.column_config.TextColumn("Name"),
            "company": st.column_config.TextColumn("Company"),
        },
    )

    handovers_header_columns = st.columns([4.3, 1.7], gap="medium")
    with handovers_header_columns[0]:
        st.markdown("**Area Handovers**")
    with handovers_header_columns[1]:
        _render_browser_dictation_button(
            target_state_key=handovers_state_key,
            component_key=f"site_diary_handovers_dictation_{target_date.isoformat()}",
            field_label="Area Handovers",
        )
    area_handovers = st.text_area(
        "Area Handovers",
        key=handovers_state_key,
        height=100,
        label_visibility="collapsed",
        placeholder="Speak or type any area handovers, access changes, or outstanding points.",
        on_change=_persist_site_diary_text_draft,
        args=(
            site_name,
            target_date,
            incidents_state_key,
            handovers_state_key,
            comments_state_key,
        ),
    )

    comments_header_columns = st.columns([4.3, 1.7], gap="medium")
    with comments_header_columns[0]:
        st.markdown("**Today's Comments**")
    with comments_header_columns[1]:
        _render_browser_dictation_button(
            target_state_key=comments_state_key,
            component_key=f"site_diary_comments_dictation_{target_date.isoformat()}",
            field_label="Today's Comments",
        )
    todays_comments = st.text_area(
        "Today's Comments",
        key=comments_state_key,
        height=120,
        label_visibility="collapsed",
        placeholder="Use this space for progress, constraints, deliveries, or end-of-day narrative.",
        on_change=_persist_site_diary_text_draft,
        args=(
            site_name,
            target_date,
            incidents_state_key,
            handovers_state_key,
            comments_state_key,
        ),
    )

    if st.session_state.pop("file2_site_diary_scroll_pending", False):
        scroll_expression = """
        (() => {
            const hostWindow = window.top || window.parent || window;
            const attemptScroll = () => {
                const target = hostWindow.document.getElementById("site-diary-editor");
                if (target) {
                    target.scrollIntoView({ behavior: "smooth", block: "start" });
                    const top =
                        target.getBoundingClientRect().top + hostWindow.scrollY - 24;
                    hostWindow.scrollTo({
                        top: Math.max(0, top),
                        behavior: "smooth",
                    });
                }
            };
            [120, 420, 900].forEach((delay) => {
                window.setTimeout(attemptScroll, delay);
            });
            return null;
        })()
        """
        streamlit_js_eval(
            js_expressions=scroll_expression,
            key=f"site_diary_scroll_{target_date.isoformat()}_{int(time.time() * 1000)}",
        )

    st.divider()
    _render_workspace_zone_heading(
        "Export / Print",
        "Generate the tagged UHSF15.63 diary document and save it into the File 2 diary folder.",
    )
    diary_action_columns = st.columns(2, gap="medium")
    with diary_action_columns[0]:
        generate_diary = st.button(
            "🖨️ Generate Daily Site Diary",
            width="stretch",
            key=f"file2_site_diary_generate_{target_date.isoformat()}",
        )
    with diary_action_columns[1]:
        if st.button(
            "↺ Reset Diary Form",
            width="stretch",
            key=f"file2_site_diary_reset_{target_date.isoformat()}",
        ):
            _queue_site_diary_form_reset(target_date)
            st.rerun()

    if generate_diary:
        try:
            cleaned_contractor_rows = _coerce_site_diary_table_rows(
                contractor_rows,
                columns=["company", "days", "nights"],
            )
            cleaned_visitor_rows = _coerce_site_diary_table_rows(
                visitor_rows,
                columns=["name", "company"],
            )
            site_diary_document = SiteDiaryDocument(
                doc_id=(
                    f"SITE-DIARY-{_slugify_identifier(site_name)}-{target_date:%Y%m%d}"
                ),
                site_name=site_name,
                created_at=datetime.now().replace(second=0, microsecond=0),
                status=DocumentStatus.ACTIVE,
                date=target_date,
                uplands_days=int(uplands_days),
                uplands_nights=int(uplands_nights),
                skip_exchange=skip_exchange,
                fire_day_on=fire_day_on,
                fire_day_off=fire_day_off,
                fire_night_on=fire_night_on,
                fire_night_off=fire_night_off,
                weather_dry=weather_dry,
                weather_mixed=weather_mixed,
                weather_wet=weather_wet,
                contractors=cleaned_contractor_rows,
                visitors=cleaned_visitor_rows,
                incidents_details=incidents_details,
                hs_reported_tick=hs_reported_tick,
                area_handovers=area_handovers,
                todays_comments=todays_comments,
            )
            generated_diary = generate_site_diary_document(
                repository,
                site_diary_document=site_diary_document,
            )
        except (TemplateValidationError, ValidationError, ValueError, RuntimeError) as exc:
            st.error(f"Unable to generate the daily site diary: {exc}")
        else:
            _clear_site_diary_text_draft(site_name, target_date)
            st.session_state["file2_site_diary_flash"] = (
                f"Daily site diary saved to {generated_diary.output_path}"
            )
            st.rerun()

    st.divider()
    _render_workspace_zone_heading(
        "Live Register / History",
        "Previously generated UHSF15.63 diaries for the active site.",
    )
    if all_site_diaries:
        sorted_diaries = sorted(
            all_site_diaries,
            key=lambda site_diary_document: (
                site_diary_document.date,
                site_diary_document.created_at,
            ),
            reverse=True,
        )
        st.dataframe(
            pd.DataFrame(_build_site_diary_history_rows(sorted_diaries)),
            hide_index=True,
            width="stretch",
        )
        selected_diary = st.selectbox(
            "Open Saved Diary",
            options=sorted_diaries,
            key="file2_site_diary_history_select",
            format_func=lambda diary_document: (
                f"{diary_document.date:%d/%m/%Y} | saved {diary_document.created_at:%H:%M}"
            ),
        )
        selected_diary_path = Path(selected_diary.generated_document_path)
        history_columns = st.columns(3)
        with history_columns[0]:
            if st.button("📂 Open Diary Output Folder", key="file2-open-diary-folder", width="stretch"):
                _open_workspace_path(FILE_2_DIARY_OUTPUT_DIR)
        with history_columns[1]:
            if selected_diary.generated_document_path and selected_diary_path.exists() and st.button(
                "📂 Open Saved Diary",
                key="file2-open-selected-diary",
                width="stretch",
            ):
                _open_workspace_path(selected_diary_path)
        with history_columns[2]:
            if selected_diary.generated_document_path and selected_diary_path.exists():
                st.download_button(
                    "📥 Download Saved Diary",
                    data=selected_diary_path.read_bytes(),
                    file_name=selected_diary_path.name,
                    mime=_guess_download_mime_type(selected_diary_path),
                    key="file2-download-selected-diary",
                    width="stretch",
                )
        delete_diary_flag_key = "file2_site_diary_delete_confirm"
        if st.button(
            "🗑️ Remove Selected Saved Diary",
            key="file2-remove-selected-diary",
            width="stretch",
        ):
            st.session_state[delete_diary_flag_key] = selected_diary.doc_id
            st.rerun()
        pending_delete_doc_id = str(st.session_state.get(delete_diary_flag_key, "") or "").strip()
        if pending_delete_doc_id == selected_diary.doc_id:
            st.warning(
                "Remove this saved diary and its generated Word file from File 2?"
            )
            confirm_columns = st.columns(2, gap="medium")
            with confirm_columns[0]:
                if st.button(
                    "Confirm Remove Diary",
                    key="file2-confirm-remove-selected-diary",
                    width="stretch",
                    type="primary",
                ):
                    repository.delete_document_and_files(selected_diary.doc_id)
                    st.session_state.pop(delete_diary_flag_key, None)
                    if selected_diary.date == target_date:
                        _queue_site_diary_form_reset(target_date)
                    st.session_state["file2_site_diary_flash"] = (
                        f"Removed saved diary for {selected_diary.date:%d/%m/%Y}."
                    )
                    st.rerun()
            with confirm_columns[1]:
                if st.button(
                    "Cancel Remove",
                    key="file2-cancel-remove-selected-diary",
                    width="stretch",
                ):
                    st.session_state.pop(delete_diary_flag_key, None)
                    st.rerun()
    else:
        st.info("No UHSF15.63 diary documents have been generated for this site yet.")


def _build_site_broadcast_rows(
    live_contacts: List[SiteBroadcastContact],
) -> List[Dict[str, str]]:
    """Return the manager-facing broadcast table rows."""

    return [
        {
            "Name": contact.individual_name,
            "Company": contact.contractor_name,
            "Mobile": contact.mobile_number,
            "Vehicle Reg": contact.vehicle_registration or "—",
        }
        for contact in live_contacts
    ]


def _build_toolbox_talk_completion_rows(
    completions: List[ToolboxTalkCompletionDocument],
) -> List[Dict[str, str]]:
    """Return manager-facing UHSF16.2 completion rows."""

    return [
        {
            "Name": completion.individual_name,
            "Company": completion.contractor_name,
            "Topic": completion.topic,
            "Signed": completion.completed_at.strftime("%d/%m/%Y %H:%M"),
        }
        for completion in completions
    ]


def _build_broadcast_dispatch_rows(
    dispatches: List[BroadcastDispatchDocument],
) -> List[Dict[str, str]]:
    """Return manager-facing broadcast dispatch history rows."""

    return [
        {
            "Sent": dispatch.dispatched_at.strftime("%d/%m/%Y %H:%M"),
            "Type": dispatch.dispatch_kind.replace("_", " ").title(),
            "Subject": dispatch.subject,
            "Audience": dispatch.audience_label,
            "Recipients": str(len(dispatch.recipient_numbers)),
            "Chunks": str(dispatch.chunk_count or 1),
            "Status": "Opened" if dispatch.launched_successfully else "Needs attention",
        }
        for dispatch in dispatches
    ]


def _build_dispatch_recipient_rows(
    dispatch: BroadcastDispatchDocument,
) -> List[Dict[str, str]]:
    """Return one recipient audit table for a saved dispatch."""

    recipient_rows: List[Dict[str, str]] = []
    recipient_names = list(dispatch.recipient_names)
    recipient_numbers = list(dispatch.recipient_numbers)
    total_rows = max(len(recipient_names), len(recipient_numbers))
    for row_index in range(total_rows):
        recipient_rows.append(
            {
                "Name": recipient_names[row_index]
                if row_index < len(recipient_names)
                else "Unknown",
                "Mobile": recipient_numbers[row_index]
                if row_index < len(recipient_numbers)
                else "—",
            }
        )
    return recipient_rows


def _build_toolbox_talk_audience_rows(
    contacts: List[SiteBroadcastContact],
    *,
    status_label: str,
) -> List[Dict[str, str]]:
    """Return one named audience preview for TBT sends."""

    return [
        {
            "Name": contact.individual_name,
            "Company": contact.contractor_name,
            "Mobile": contact.mobile_number,
            "Vehicle Reg": contact.vehicle_registration or "—",
            "Status": status_label,
        }
        for contact in contacts
    ]


def _build_pending_toolbox_talk_attendance_entries(
    attendance_entries: List[DailyAttendanceEntryDocument],
    completions: List[ToolboxTalkCompletionDocument],
) -> List[DailyAttendanceEntryDocument]:
    """Return active on-site operatives who still need to sign the current TBT."""

    signed_induction_ids = {
        completion.linked_induction_doc_id.strip()
        for completion in completions
        if completion.linked_induction_doc_id.strip()
    }
    signed_people = {
        (
            completion.individual_name.casefold(),
            completion.contractor_name.casefold(),
        )
        for completion in completions
    }
    return [
        entry
        for entry in attendance_entries
        if not (
            (
                entry.linked_induction_doc_id.strip()
                and entry.linked_induction_doc_id.strip() in signed_induction_ids
            )
            or (
                entry.individual_name.casefold(),
                entry.contractor_name.casefold(),
            )
            in signed_people
        )
    ]


def _render_dispatch_history_audit(
    dispatches: List[BroadcastDispatchDocument],
    *,
    empty_message: str,
    expander_prefix: str,
) -> None:
    """Render one richer dispatch history view with recipient detail."""

    if not dispatches:
        st.caption(empty_message)
        return

    st.dataframe(
        pd.DataFrame(_build_broadcast_dispatch_rows(dispatches[:12])),
        width="stretch",
        hide_index=True,
    )
    st.caption(
        "Open any batch below to inspect exactly who the app opened in Messages and the wording used."
    )
    for dispatch_index, dispatch in enumerate(dispatches[:8]):
        recipient_count = len(dispatch.recipient_numbers)
        expander_label = (
            f"{dispatch.dispatched_at:%d/%m/%Y %H:%M} · "
            f"{dispatch.dispatch_kind.replace('_', ' ').title()} · "
            f"{recipient_count} recipient{'s' if recipient_count != 1 else ''}"
        )
        with st.expander(expander_label, expanded=dispatch_index == 0):
            audit_columns = st.columns(4, gap="medium")
            with audit_columns[0]:
                _render_dispatch_audit_card(
                    label="Audience",
                    value=dispatch.audience_label,
                    caption=dispatch.dispatch_kind.replace("_", " ").title(),
                )
            with audit_columns[1]:
                _render_dispatch_audit_card(
                    label="Recipients",
                    value=str(recipient_count),
                    caption="People opened in Messages",
                )
            with audit_columns[2]:
                _render_dispatch_audit_card(
                    label="Drafts",
                    value=str(dispatch.chunk_count or 1),
                    caption="Messages draft windows opened",
                )
            with audit_columns[3]:
                _render_dispatch_audit_card(
                    label="Status",
                    value="Opened" if dispatch.launched_successfully else "Needs attention",
                    caption=dispatch.launch_mode.replace("_", " ").title(),
                )
            if dispatch.message_body.strip():
                st.caption("Message body")
                _render_dispatch_message_box(dispatch.message_body.strip())
            recipient_rows = _build_dispatch_recipient_rows(dispatch)
            if recipient_rows:
                st.caption("Recipients opened in Messages")
                st.dataframe(
                    pd.DataFrame(recipient_rows),
                    width="stretch",
                    hide_index=True,
                    key=f"{expander_prefix}_dispatch_recipients_{dispatch.doc_id}",
                )


def _build_broadcast_company_options(
    live_contacts: List[SiteBroadcastContact],
) -> List[str]:
    """Return sorted live company options for audience filtering."""

    return sorted(
        {contact.contractor_name for contact in live_contacts if contact.contractor_name},
        key=str.casefold,
    )


def _filter_site_broadcast_contacts(
    live_contacts: List[SiteBroadcastContact],
    *,
    audience_filter: str,
    selected_companies: List[str],
) -> List[SiteBroadcastContact]:
    """Return the filtered live audience for the broadcast station."""

    if audience_filter == "uplands":
        return [
            contact
            for contact in live_contacts
            if _is_uplands_company(contact.contractor_name)
        ]
    if audience_filter == "subcontractors":
        return [
            contact
            for contact in live_contacts
            if not _is_uplands_company(contact.contractor_name)
        ]
    if audience_filter == "companies":
        selected_company_set = {company for company in selected_companies if company}
        return [
            contact
            for contact in live_contacts
            if contact.contractor_name in selected_company_set
        ]
    return live_contacts


def _filter_active_attendance_entries(
    attendance_entries: List[DailyAttendanceEntryDocument],
    *,
    audience_filter: str,
    selected_companies: List[str],
) -> List[DailyAttendanceEntryDocument]:
    """Return live attendance entries matching the chosen audience filter."""

    if audience_filter == "uplands":
        return [
            entry for entry in attendance_entries if _is_uplands_company(entry.contractor_name)
        ]
    if audience_filter == "subcontractors":
        return [
            entry
            for entry in attendance_entries
            if not _is_uplands_company(entry.contractor_name)
        ]
    if audience_filter == "companies":
        selected_company_set = {company for company in selected_companies if company}
        return [
            entry
            for entry in attendance_entries
            if entry.contractor_name in selected_company_set
        ]
    return attendance_entries


def _count_missing_broadcast_numbers(
    attendance_entries: List[DailyAttendanceEntryDocument],
    live_contacts: List[SiteBroadcastContact],
) -> int:
    """Return how many targeted active operatives still lack a reachable mobile."""

    attendance_identities = {
        (
            entry.linked_induction_doc_id or entry.individual_name.casefold(),
            entry.contractor_name.casefold(),
        )
        for entry in attendance_entries
    }
    reachable_identities = {
        (
            contact.linked_induction_doc_id or contact.individual_name.casefold(),
            contact.contractor_name.casefold(),
        )
        for contact in live_contacts
    }
    return max(len(attendance_identities - reachable_identities), 0)


def _describe_broadcast_audience(
    audience_filter: str,
    selected_companies: List[str],
) -> str:
    """Return one human-readable audience label for dispatch logs."""

    if audience_filter == "uplands":
        return "Uplands Employees"
    if audience_filter == "subcontractors":
        return "Subcontractors / Visitors"
    if audience_filter == "companies":
        if not selected_companies:
            return "Selected Companies"
        return "Companies: " + ", ".join(selected_companies)
    return "Everyone On Site"


def _get_recent_site_values(
    site_name: str,
    *,
    settings_key: str,
) -> List[str]:
    """Return recent site-specific workflow values from root settings."""

    resolved_site_name = site_name.strip() or PROJECT_NAME
    settings_payload = load_app_settings()
    history_by_site = settings_payload.get(settings_key, {})
    if not isinstance(history_by_site, dict):
        return []
    recent_values = history_by_site.get(resolved_site_name, [])
    if not isinstance(recent_values, list):
        return []
    return [str(value).strip() for value in recent_values if str(value).strip()]


def _remember_recent_site_value(
    site_name: str,
    value: str,
    *,
    settings_key: str,
    max_entries: int = 6,
) -> None:
    """Persist one recent workflow value against the current site."""

    resolved_site_name = site_name.strip() or PROJECT_NAME
    cleaned_value = value.strip()
    if not cleaned_value:
        return

    settings_payload = load_app_settings()
    history_by_site = dict(settings_payload.get(settings_key, {}) or {})
    existing_values = [
        str(item).strip()
        for item in history_by_site.get(resolved_site_name, [])
        if str(item).strip()
    ]
    deduped_values = [cleaned_value] + [
        existing_value
        for existing_value in existing_values
        if existing_value.casefold() != cleaned_value.casefold()
    ]
    history_by_site[resolved_site_name] = deduped_values[:max_entries]

    save_kwargs: Dict[str, Any] = {}
    if settings_key == "broadcast_message_history_by_site":
        save_kwargs["broadcast_message_history_by_site"] = history_by_site
    elif settings_key == "tbt_topic_history_by_site":
        save_kwargs["tbt_topic_history_by_site"] = history_by_site
    else:
        return
    save_app_settings(**save_kwargs)


def _build_broadcast_message_presets(site_name: str) -> Dict[str, str]:
    """Return premium quick-start broadcast copy presets."""

    resolved_site_name = site_name.strip() or PROJECT_NAME
    return {
        "Safety Alert": (
            f"⚠️ Safety Alert: [update required]. "
            f"All operatives at {resolved_site_name} must stop, review the instruction, "
            "and report to their supervisor immediately."
        ),
        "Stand Down": (
            f"🛑 Stand Down: Stop work now and report to the canteen / welfare point at "
            f"{resolved_site_name} for a site-wide briefing."
        ),
        "Toolbox Talk": (
            f"🦺 Toolbox Talk: A new site briefing is live for {resolved_site_name}. "
            "Open the link provided by the site team, read the document, and sign the register."
        ),
        "Welfare": (
            f"☕ Welfare Update: Please report to the canteen / welfare area at "
            f"{resolved_site_name} for a briefing in 5 minutes."
        ),
    }


def _build_company_reachability_rows(
    attendance_entries: List[DailyAttendanceEntryDocument],
    live_contacts: List[SiteBroadcastContact],
) -> List[Dict[str, Any]]:
    """Return per-company audience stats for the broadcast reachability heatmap."""

    company_stats: Dict[str, Dict[str, int]] = {}
    for attendance_entry in attendance_entries:
        company_name = attendance_entry.contractor_name or "Unknown"
        stats = company_stats.setdefault(
            company_name,
            {"on_site": 0, "reachable": 0},
        )
        stats["on_site"] += 1
    for contact in live_contacts:
        company_name = contact.contractor_name or "Unknown"
        stats = company_stats.setdefault(
            company_name,
            {"on_site": 0, "reachable": 0},
        )
        stats["reachable"] += 1

    rows: List[Dict[str, Any]] = []
    for company_name, stats in sorted(company_stats.items(), key=lambda item: item[0].casefold()):
        on_site_count = stats["on_site"]
        reachable_count = stats["reachable"]
        missing_count = max(on_site_count - reachable_count, 0)
        reachability_percent = (
            round((reachable_count / on_site_count) * 100, 1)
            if on_site_count
            else 0.0
        )
        rows.append(
            {
                "Company": company_name,
                "On Site": on_site_count,
                "Reachable": reachable_count,
                "Missing": missing_count,
                "Reachability %": reachability_percent,
                "Status": (
                    "Fully covered"
                    if missing_count == 0 and on_site_count > 0
                    else "Patchy"
                    if reachable_count > 0
                    else "Needs mobiles"
                ),
            }
        )
    return rows


def _render_broadcast_status_badges(
    badges: List[tuple[str, str]],
) -> None:
    """Render premium inline resend/status badges for the broadcast station."""

    badge_markup = "".join(
        (
            "<span class='broadcast-status-badge "
            f"broadcast-status-badge-{html.escape(tone, quote=True)}'>"
            f"{html.escape(label)}"
            "</span>"
        )
        for label, tone in badges
    )
    st.markdown(
        f"<div class='broadcast-status-badge-row'>{badge_markup}</div>",
        unsafe_allow_html=True,
    )


def _toolbox_talk_signer_label(attendance_entry: DailyAttendanceEntryDocument) -> str:
    """Return the mobile TBT signer label for one active operative."""

    return (
        f"{attendance_entry.individual_name} "
        f"({attendance_entry.contractor_name}) · "
        f"In {attendance_entry.time_in.strftime('%H:%M')}"
    )


def _render_site_broadcast_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render the manager-facing broadcast and toolbox talk hub."""

    live_contacts = build_live_site_broadcast_contacts(
        repository,
        site_name=project_setup.current_site_name,
    )
    active_attendance_entries = list_daily_attendance_entries(
        repository,
        site_name=project_setup.current_site_name,
        on_date=date.today(),
        active_only=True,
    )
    existing_tbt_documents = list_toolbox_talk_documents(
        repository,
        site_name=project_setup.current_site_name,
    )
    existing_tbt_completions = list_toolbox_talk_completions(
        repository,
        site_name=project_setup.current_site_name,
    )
    known_topics = sorted(
        {
            *(completion.topic for completion in existing_tbt_completions),
            *(document.topic for document in existing_tbt_documents),
        },
        key=str.casefold,
    )
    missing_mobile_count = _count_missing_broadcast_numbers(
        active_attendance_entries,
        live_contacts,
    )

    _render_workspace_hero(
        icon="📢",
        kicker="Live Operations",
        title="Site Alerts & TBTs",
        caption=(
            "Reach the live fire roll through Messages, launch remote toolbox talks, "
            "and chase only the unsigned names without leaving the app."
        ),
    )

    metric_columns = st.columns(4, gap="large")
    with metric_columns[0]:
        _render_metric_card(
            title="Active Phones on Site",
            icon="📱",
            value=str(len(live_contacts)),
            caption="Unique mobile numbers currently available from the live fire roll.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Operatives signed in: <strong>{len(active_attendance_entries)}</strong>"
                "</div>"
            ),
        )
    with metric_columns[1]:
        _render_metric_card(
            title="Missing Mobile Records",
            icon="⚠️",
            value=str(missing_mobile_count),
            caption="Active operatives still missing a reachable mobile number.",
            body_html="<div class='data-card-subtext'>Fix these in UHSF16.01 inductions</div>",
        )
    with metric_columns[2]:
        _render_metric_card(
            title="Subcontractors / Visitors",
            icon="👷",
            value=str(
                sum(
                    1
                    for attendance_entry in active_attendance_entries
                    if not attendance_entry.is_uplands_employee
                )
            ),
            caption="Live non-Uplands count currently signed in.",
            body_html="<div class='data-card-subtext'>Live audience</div>",
        )
    with metric_columns[3]:
        _render_metric_card(
            title="Uplands Employees",
            icon="🏗️",
            value=str(
                sum(
                    1
                    for attendance_entry in active_attendance_entries
                    if attendance_entry.is_uplands_employee
                )
            ),
            caption="Live Uplands personnel currently signed in.",
            body_html="<div class='data-card-subtext'>Live audience</div>",
        )

    broadcast_tab, tbt_tab = st.tabs(["Mass Broadcast", "Toolbox Talks"])

    with broadcast_tab:
        pending_broadcast_preset_key = "site_broadcast_message_pending"
        if pending_broadcast_preset_key in st.session_state:
            st.session_state["site_broadcast_message"] = st.session_state.pop(
                pending_broadcast_preset_key
            )

        st.markdown(
            "<div class='file-2-section-heading'>Mass Broadcast</div>",
            unsafe_allow_html=True,
        )
        audience_labels = {
            "Everyone On Site": "all",
            "Subcontractors / Visitors": "subcontractors",
            "Uplands Employees": "uplands",
            "Specific Companies": "companies",
        }
        selected_audience_label = st.radio(
            "Audience",
            options=list(audience_labels.keys()),
            horizontal=True,
            key="site_broadcast_audience_label",
        )
        selected_company_options = _build_broadcast_company_options(live_contacts)
        selected_companies = (
            st.multiselect(
                "Choose companies to include",
                options=selected_company_options,
                key="site_broadcast_companies",
                placeholder="Select one or more live contractors",
            )
            if audience_labels[selected_audience_label] == "companies"
            else []
        )
        filtered_contacts = _filter_site_broadcast_contacts(
            live_contacts,
            audience_filter=audience_labels[selected_audience_label],
            selected_companies=selected_companies,
        )
        filtered_attendance_entries = _filter_active_attendance_entries(
            active_attendance_entries,
            audience_filter=audience_labels[selected_audience_label],
            selected_companies=selected_companies,
        )
        filtered_missing_mobile_count = _count_missing_broadcast_numbers(
            filtered_attendance_entries,
            filtered_contacts,
        )

        audience_metric_columns = st.columns(3, gap="large")
        with audience_metric_columns[0]:
            st.metric("📱 Reachable Phones", len(filtered_contacts))
        with audience_metric_columns[1]:
            st.metric("⚠️ Missing Mobile Records", filtered_missing_mobile_count)
        with audience_metric_columns[2]:
            st.metric(
                "🏢 Companies in Audience",
                len(
                    {
                        contact.contractor_name
                        for contact in filtered_contacts
                        if contact.contractor_name
                    }
                ),
            )

        st.divider()
        st.caption("Quick-start presets")
        preset_messages = _build_broadcast_message_presets(
            project_setup.current_site_name
        )
        preset_columns = st.columns(4, gap="small")
        for preset_column, preset_name in zip(preset_columns, preset_messages.keys()):
            with preset_column:
                if st.button(
                    preset_name,
                    key=f"broadcast_preset_{preset_name.lower().replace(' ', '_')}",
                    width="stretch",
                ):
                    st.session_state[pending_broadcast_preset_key] = preset_messages[
                        preset_name
                    ]
                    st.rerun()

        recent_broadcast_messages = _get_recent_site_values(
            project_setup.current_site_name,
            settings_key="broadcast_message_history_by_site",
        )
        if recent_broadcast_messages:
            st.caption("Recent site messages")
            recent_message_columns = st.columns(
                min(3, len(recent_broadcast_messages)),
                gap="small",
            )
            for message_index, recent_message in enumerate(recent_broadcast_messages):
                with recent_message_columns[message_index % len(recent_message_columns)]:
                    recent_label = (
                        recent_message
                        if len(recent_message) <= 38
                        else recent_message[:35].rstrip() + "..."
                    )
                    if st.button(
                        f"↺ {recent_label}",
                        key=f"broadcast_recent_message_{message_index}",
                        width="stretch",
                        help=recent_message,
                    ):
                        st.session_state[pending_broadcast_preset_key] = recent_message
                        st.rerun()

        st.caption(
            "Messages on this Mac will open ready-to-send SMS draft(s) automatically. "
            "Start from a preset, then tune the wording if needed."
        )
        st.divider()
        st.caption(
            f"Audience ready now: {len(filtered_contacts)} reachable phone(s), "
            f"{filtered_missing_mobile_count} missing mobile record(s)."
        )
        broadcast_message = st.text_area(
            "Broadcast Message",
            key="site_broadcast_message",
            placeholder="⚠️ High winds. Crane operations are suspended until further notice.",
            help="Write the message once and the app will open it in Messages for the selected live audience.",
            height=150,
        )

        if st.button(
            "📨 Send Broadcast in Messages",
            key="send_site_broadcast_messages",
            width="stretch",
        ):
            resolved_message = broadcast_message.strip()
            if not filtered_contacts:
                st.session_state["site_broadcast_flash"] = {
                    "level": "error",
                    "message": "No reachable mobiles are available for the selected audience.",
                }
            elif not resolved_message:
                st.session_state["site_broadcast_flash"] = {
                    "level": "error",
                    "message": "Type the broadcast message before sending it.",
                }
            else:
                launch_result = launch_messages_sms_broadcast(
                    [contact.mobile_number for contact in filtered_contacts],
                    message=resolved_message,
                )
                log_broadcast_dispatch(
                    repository,
                    site_name=project_setup.current_site_name,
                    dispatch_kind="mass_broadcast",
                    audience_label=_describe_broadcast_audience(
                        audience_labels[selected_audience_label],
                        selected_companies,
                    ),
                    subject=resolved_message[:80],
                    message_body=resolved_message,
                    recipient_numbers=[
                        contact.mobile_number for contact in filtered_contacts
                    ],
                    recipient_names=[
                        contact.individual_name for contact in filtered_contacts
                    ],
                    launch_result=launch_result,
                )
                _remember_recent_site_value(
                    project_setup.current_site_name,
                    resolved_message,
                    settings_key="broadcast_message_history_by_site",
                )
                st.session_state["site_broadcast_flash"] = {
                    "level": "success" if launch_result.launched_successfully else "warning",
                    "message": (
                        f"Opened {launch_result.chunk_count} Messages draft(s) for "
                        f"{launch_result.recipient_count} recipient(s)."
                    ),
                    "detail": launch_result.error_message,
                }
            st.rerun()

        broadcast_flash = st.session_state.pop("site_broadcast_flash", None)
        if broadcast_flash:
            if broadcast_flash["level"] == "success":
                st.success(broadcast_flash["message"])
            elif broadcast_flash["level"] == "warning":
                st.warning(broadcast_flash["message"])
            else:
                st.error(broadcast_flash["message"])
            if broadcast_flash.get("detail"):
                st.caption(broadcast_flash["detail"])

        st.divider()
        st.markdown(
            "<div class='file-2-section-heading'>Live Audience</div>",
            unsafe_allow_html=True,
        )
        if filtered_contacts:
            st.dataframe(
                pd.DataFrame(_build_site_broadcast_rows(filtered_contacts)),
                width="stretch",
                hide_index=True,
            )
        else:
            st.info(
                "Nobody in the selected live audience currently has a valid mobile number attached."
            )

        st.divider()
        st.markdown(
            "<div class='file-2-section-heading'>Per-Company Reachability</div>",
            unsafe_allow_html=True,
        )
        company_heatmap_rows = _build_company_reachability_rows(
            filtered_attendance_entries,
            filtered_contacts,
        )
        if company_heatmap_rows:
            company_heatmap_frame = pd.DataFrame(company_heatmap_rows)
            st.dataframe(
                company_heatmap_frame,
                width="stretch",
                hide_index=True,
                column_config={
                    "Company": st.column_config.TextColumn(
                        "Company",
                        width="medium",
                    ),
                    "On Site": st.column_config.NumberColumn(
                        "On Site",
                        format="%d",
                        width="small",
                    ),
                    "Reachable": st.column_config.NumberColumn(
                        "Reachable",
                        format="%d",
                        width="small",
                    ),
                    "Missing": st.column_config.NumberColumn(
                        "Missing",
                        format="%d",
                        width="small",
                    ),
                    "Reachability %": st.column_config.ProgressColumn(
                        "Reachability %",
                        help="Higher is better. This shows how much of each company audience is reachable right now.",
                        format="%.0f%%",
                        min_value=0,
                        max_value=100,
                        width="medium",
                    ),
                    "Status": st.column_config.TextColumn(
                        "Status",
                        width="small",
                    ),
                },
            )
            st.caption(
                "This native readiness grid stays dependency-free and shows which companies are fully covered before you send."
            )
        else:
            st.caption("No company audience data is available for the current filter yet.")

        st.divider()
        st.markdown(
            "<div class='file-2-section-heading'>Dispatch History</div>",
            unsafe_allow_html=True,
        )
        broadcast_dispatches = list_broadcast_dispatches(
            repository,
            site_name=project_setup.current_site_name,
            dispatch_kind="mass_broadcast",
        )
        _render_dispatch_history_audit(
            broadcast_dispatches,
            empty_message="No broadcast batches have been launched yet.",
            expander_prefix="mass_broadcast",
        )

    with tbt_tab:
        pending_tbt_topic_key = "site_tbt_topic_pending"
        if pending_tbt_topic_key in st.session_state:
            st.session_state["site_broadcast_tbt_topic"] = st.session_state.pop(
                pending_tbt_topic_key
            )

        st.markdown(
            "<div class='file-2-section-heading'>Start New Toolbox Talk</div>",
            unsafe_allow_html=True,
        )
        recent_tbt_topics = _get_recent_site_values(
            project_setup.current_site_name,
            settings_key="tbt_topic_history_by_site",
        )
        if recent_tbt_topics:
            st.caption("Recent toolbox talk topics")
            recent_topic_columns = st.columns(min(3, len(recent_tbt_topics)), gap="small")
            for topic_index, recent_topic in enumerate(recent_tbt_topics):
                with recent_topic_columns[topic_index % len(recent_topic_columns)]:
                    recent_topic_label = (
                        recent_topic
                        if len(recent_topic) <= 34
                        else recent_topic[:31].rstrip() + "..."
                    )
                    if st.button(
                        f"↺ {recent_topic_label}",
                        key=f"recent_tbt_topic_{topic_index}",
                        width="stretch",
                        help=recent_topic,
                    ):
                        st.session_state[pending_tbt_topic_key] = recent_topic
                        st.rerun()
        tbt_topic = st.text_input(
            "TBT Topic / Subject",
            key="site_broadcast_tbt_topic",
            placeholder="e.g. Working in high winds",
        )
        uploaded_tbt_document = st.file_uploader(
            "Upload TBT Document",
            type=["pdf", "doc", "docx"],
            key="site_broadcast_tbt_document",
            help="Upload the toolbox talk file the operatives must read before signing. PDF gives the best phone viewing experience.",
        )
        st.caption("Best mobile experience: upload PDF.")
        if uploaded_tbt_document is not None and not uploaded_tbt_document.name.lower().endswith(".pdf"):
            st.info(
                "This file will still work, but PDF is the smoothest option for phone viewing."
            )
        if st.button(
            "➕ Create Toolbox Talk",
            key="create_remote_tbt_link",
            width="stretch",
        ):
            resolved_topic = tbt_topic.strip()
            if not resolved_topic:
                st.session_state["site_tbt_flash"] = {
                    "level": "error",
                    "message": "Enter a toolbox talk topic before creating the remote link.",
                }
            elif uploaded_tbt_document is None:
                st.session_state["site_tbt_flash"] = {
                    "level": "error",
                    "message": "Upload the toolbox talk document before creating the remote link.",
                }
            else:
                saved_document = save_toolbox_talk_document(
                    repository,
                    site_name=project_setup.current_site_name,
                    topic=resolved_topic,
                    uploaded_file_name=uploaded_tbt_document.name,
                    uploaded_file_bytes=uploaded_tbt_document.getvalue(),
                )
                tbt_link = build_toolbox_talk_url(
                    resolved_topic,
                    public_url=project_setup.public_tunnel_url,
                )
                st.session_state["site_tbt_flash"] = {
                    "level": "success",
                    "message": (
                        "Remote toolbox talk link created and the source document is now live."
                    ),
                }
                st.session_state["site_tbt_active_topic"] = resolved_topic
                st.session_state["site_tbt_active_link"] = tbt_link
                st.session_state["site_tbt_active_document_name"] = (
                    saved_document.toolbox_talk_document.original_file_name
                )
                st.session_state["site_tbt_message_template"] = build_toolbox_talk_sms_message(
                    resolved_topic,
                    tbt_link,
                )
                _remember_recent_site_value(
                    project_setup.current_site_name,
                    resolved_topic,
                    settings_key="tbt_topic_history_by_site",
                )
            st.rerun()

        tbt_flash = st.session_state.pop("site_tbt_flash", None)
        if tbt_flash is not None:
            if tbt_flash["level"] == "success":
                st.success(tbt_flash["message"])
            else:
                st.error(tbt_flash["message"])

        active_tbt_topic = str(st.session_state.get("site_tbt_active_topic", "")).strip()
        if active_tbt_topic and active_tbt_topic not in known_topics:
            topic_options = [active_tbt_topic] + known_topics
        else:
            topic_options = known_topics
        if topic_options:
            default_topic = active_tbt_topic if active_tbt_topic in topic_options else topic_options[0]
            selected_tbt_topic = st.selectbox(
                "Current / Previous TBT Topic",
                options=topic_options,
                index=topic_options.index(default_topic),
                key="site_tbt_selected_topic",
            )
        else:
            selected_tbt_topic = active_tbt_topic

        selected_tbt_document = (
            get_latest_toolbox_talk_document(
                repository,
                site_name=project_setup.current_site_name,
                topic=selected_tbt_topic,
            )
            if selected_tbt_topic
            else None
        )

        selected_tbt_link = (
            build_toolbox_talk_url(
                selected_tbt_topic,
                public_url=project_setup.public_tunnel_url,
            )
            if selected_tbt_topic
            else ""
        )
        selected_tbt_message = (
            build_toolbox_talk_sms_message(selected_tbt_topic, selected_tbt_link)
            if selected_tbt_topic and selected_tbt_link
            else ""
        )
        topic_completions = (
            list_toolbox_talk_completions(
                repository,
                site_name=project_setup.current_site_name,
                topic=selected_tbt_topic,
            )
            if selected_tbt_topic
            else []
        )
        pending_contacts = (
            build_pending_toolbox_talk_contacts(
                repository,
                site_name=project_setup.current_site_name,
                topic=selected_tbt_topic,
                on_date=date.today(),
            )
            if selected_tbt_topic
            else []
        )

        if selected_tbt_document is not None:
            selected_tbt_document_view_url = build_toolbox_talk_document_view_url(
                selected_tbt_document.doc_id,
                public_url=project_setup.public_tunnel_url,
            )
            try:
                document_bytes, mime_type = read_toolbox_talk_document_bytes(
                    selected_tbt_document
                )
            except FileNotFoundError:
                st.warning("The uploaded toolbox talk document is missing from disk.")
            else:
                document_columns = st.columns([1.3, 0.7], gap="large")
                with document_columns[0]:
                    st.caption(
                        f"Attached TBT document: {selected_tbt_document.original_file_name}"
                    )
                    st.caption(
                        f"Storage folder: {FILE_2_TBT_ACTIVE_DOCS_DIR.name}"
                    )
                with document_columns[1]:
                    if selected_tbt_document_view_url:
                        st.link_button(
                            "📖 Open TBT Document",
                            url=selected_tbt_document_view_url,
                            width="stretch",
                        )
                    st.download_button(
                        "📥 Download TBT Document",
                        data=document_bytes,
                        file_name=selected_tbt_document.original_file_name,
                        mime=mime_type,
                        width="stretch",
                        key=f"download_tbt_doc_{selected_tbt_document.doc_id}",
                    )

        tbt_dispatches = (
            list_broadcast_dispatches(
                repository,
                site_name=project_setup.current_site_name,
                topic=selected_tbt_topic,
            )
            if selected_tbt_topic
            else []
        )
        if selected_tbt_topic:
            signed_live_count = max(len(live_contacts) - len(pending_contacts), 0)
            st.divider()
            tbt_metric_columns = st.columns(4, gap="large")
            with tbt_metric_columns[0]:
                st.metric("📱 Reachable Live Audience", len(live_contacts))
            with tbt_metric_columns[1]:
                st.metric("✍️ Signed", len(topic_completions))
            with tbt_metric_columns[2]:
                st.metric("🔁 Pending Live Signers", len(pending_contacts))
            with tbt_metric_columns[3]:
                st.metric("⚠️ Missing Mobiles", missing_mobile_count)

            _render_broadcast_status_badges(
                [
                    (f"Live {len(live_contacts)}", "neutral"),
                    (f"Signed {len(topic_completions)}", "success"),
                    (
                        f"Pending {len(pending_contacts)}",
                        "warning" if pending_contacts else "success",
                    ),
                    (
                        f"Missing mobiles {missing_mobile_count}",
                        "danger" if missing_mobile_count else "neutral",
                    ),
                ]
            )
            st.caption(
                f"Current toolbox talk: {selected_tbt_topic} | Live link stays pinned to the active site audience."
            )

            st.markdown(
                "<div class='file-2-section-heading'>Pre-Send Audience Preview</div>",
                unsafe_allow_html=True,
            )
            _render_broadcast_status_badges(
                [
                    (
                        f"Signed {signed_live_count}",
                        "success" if signed_live_count else "neutral",
                    ),
                    (
                        f"Pending {len(pending_contacts)}",
                        "warning" if pending_contacts else "success",
                    ),
                    (
                        f"Not Reachable {missing_mobile_count}",
                        "danger" if missing_mobile_count else "neutral",
                    ),
                ]
            )
            preview_columns = st.columns(2, gap="large")
            with preview_columns[0]:
                st.caption("Everyone on the live fire roll who will get the first send")
                if live_contacts:
                    st.dataframe(
                        pd.DataFrame(
                            _build_toolbox_talk_audience_rows(
                                live_contacts,
                                status_label="Ready now",
                            )
                        ),
                        width="stretch",
                        hide_index=True,
                    )
                else:
                    st.info("No live operatives with mobile numbers are currently reachable.")
            with preview_columns[1]:
                st.caption("Reminder queue if you resend to pending signers later")
                if pending_contacts:
                    st.dataframe(
                        pd.DataFrame(
                            _build_toolbox_talk_audience_rows(
                                pending_contacts,
                                status_label="Pending signature",
                            )
                        ),
                        width="stretch",
                        hide_index=True,
                    )
                else:
                    st.success("No live reminder queue right now. Everyone on site is signed or unreachable.")

            st.caption("Message preview")
            st.code(selected_tbt_message, language="text")

            if selected_tbt_link:
                action_columns = st.columns([1, 1, 1], gap="large")
                with action_columns[0]:
                    if st.button(
                        "📨 Send TBT in Messages",
                        key="send_toolbox_talk_messages",
                        width="stretch",
                    ):
                        if not live_contacts:
                            st.session_state["site_tbt_delivery_flash"] = {
                                "level": "error",
                                "message": "No reachable live audience is currently signed in.",
                            }
                        else:
                            launch_result = launch_messages_sms_broadcast(
                                [contact.mobile_number for contact in live_contacts],
                                message=selected_tbt_message,
                            )
                            log_broadcast_dispatch(
                                repository,
                                site_name=project_setup.current_site_name,
                                dispatch_kind="toolbox_talk",
                                audience_label="Everyone On Site",
                                subject=selected_tbt_topic,
                                message_body=selected_tbt_message,
                                recipient_numbers=[
                                    contact.mobile_number for contact in live_contacts
                                ],
                                recipient_names=[
                                    contact.individual_name for contact in live_contacts
                                ],
                                launch_result=launch_result,
                                topic=selected_tbt_topic,
                            )
                            st.session_state["site_tbt_delivery_flash"] = {
                                "level": "success"
                                if launch_result.launched_successfully
                                else "warning",
                                "message": (
                                    f"Opened {launch_result.chunk_count} Messages draft(s) "
                                    f"for {launch_result.recipient_count} live recipient(s)."
                                ),
                                "detail": launch_result.error_message,
                            }
                        st.rerun()
                with action_columns[1]:
                    if st.button(
                        "🔁 Remind Pending Only",
                        key="remind_pending_tbt_messages",
                        width="stretch",
                    ):
                        if not pending_contacts:
                            st.session_state["site_tbt_delivery_flash"] = {
                                "level": "error",
                                "message": "Nobody on the live fire roll is currently pending this toolbox talk.",
                            }
                        else:
                            launch_result = launch_messages_sms_broadcast(
                                [contact.mobile_number for contact in pending_contacts],
                                message=selected_tbt_message,
                            )
                            log_broadcast_dispatch(
                                repository,
                                site_name=project_setup.current_site_name,
                                dispatch_kind="toolbox_talk_reminder",
                                audience_label="Pending Live Signers",
                                subject=selected_tbt_topic,
                                message_body=selected_tbt_message,
                                recipient_numbers=[
                                    contact.mobile_number for contact in pending_contacts
                                ],
                                recipient_names=[
                                    contact.individual_name for contact in pending_contacts
                                ],
                                launch_result=launch_result,
                                topic=selected_tbt_topic,
                            )
                            st.session_state["site_tbt_delivery_flash"] = {
                                "level": "success"
                                if launch_result.launched_successfully
                                else "warning",
                                "message": (
                                    f"Opened {launch_result.chunk_count} reminder draft(s) "
                                    f"for {launch_result.recipient_count} pending signer(s)."
                                ),
                                "detail": launch_result.error_message,
                            }
                        st.rerun()
                with action_columns[2]:
                    st.link_button(
                        "🔗 Open Live Signing Link",
                        url=selected_tbt_link,
                        width="stretch",
                    )

            tbt_delivery_flash = st.session_state.pop("site_tbt_delivery_flash", None)
            if tbt_delivery_flash is not None:
                if tbt_delivery_flash["level"] == "success":
                    st.success(tbt_delivery_flash["message"])
                elif tbt_delivery_flash["level"] == "warning":
                    st.warning(tbt_delivery_flash["message"])
                else:
                    st.error(tbt_delivery_flash["message"])
                if tbt_delivery_flash.get("detail"):
                    st.caption(tbt_delivery_flash["detail"])

            st.divider()
            st.markdown(
                "<div class='file-2-section-heading'>Remote Signature Register</div>",
                unsafe_allow_html=True,
            )
            if topic_completions:
                st.dataframe(
                    pd.DataFrame(_build_toolbox_talk_completion_rows(topic_completions)),
                    width="stretch",
                    hide_index=True,
                )
            else:
                st.info("No remote signatures have been logged for this toolbox talk yet.")

            st.divider()
            st.markdown(
                "<div class='file-2-section-heading'>Delivery History</div>",
                unsafe_allow_html=True,
            )
            _render_dispatch_history_audit(
                tbt_dispatches,
                empty_message="No TBT delivery batches have been launched for this topic yet.",
                expander_prefix="toolbox_talk",
            )

            st.divider()
            if st.button(
                "🖨️ Export TBT Register",
                key="export_toolbox_talk_register",
                width="stretch",
            ):
                try:
                    generated_register = generate_toolbox_talk_register_document(
                        repository,
                        site_name=project_setup.current_site_name,
                        topic=selected_tbt_topic,
                    )
                except TemplateValidationError as exc:
                    st.error(f"Official UHSF16.2 template failed validation: {exc}")
                except Exception as exc:
                    st.error(f"Unable to export toolbox talk register: {exc}")
                else:
                    _open_file_for_printing(generated_register.output_path)
                    st.success(f"✅ Register saved to {generated_register.output_path}")
                    st.caption(
                        f"Rows printed: {generated_register.row_count} | Output folder: {FILE_2_TBT_OUTPUT_DIR.name}"
                    )
        else:
            st.info("Create a toolbox talk link or wait for completions to appear here.")


def _render_toolbox_talk_kiosk(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
    *,
    topic: str,
) -> None:
    """Render the mobile remote UHSF16.2 signer kiosk."""

    try:
        from streamlit_drawable_canvas import st_canvas
    except ImportError:
        st.error(
            "streamlit-drawable-canvas is not installed. Install dependencies and restart the app."
        )
        return

    resolved_topic = topic.strip()
    if not resolved_topic:
        st.error("Toolbox talk topic missing from the link. Ask the manager to generate a fresh TBT link.")
        return

    source_document = get_latest_toolbox_talk_document(
        repository,
        site_name=project_setup.current_site_name,
        topic=resolved_topic,
    )
    if source_document is None:
        st.error(
            "No toolbox talk document is attached to this link yet. Ask the manager to generate a fresh TBT link."
        )
        return

    if st.session_state.pop("toolbox_talk_reset_pending", False):
        st.session_state["toolbox_talk_selected_attendance_doc_id"] = ""
        st.session_state["toolbox_talk_read_confirmed"] = False
    completed_message = str(st.session_state.get("toolbox_talk_kiosk_complete_message", "")).strip()
    completed_at = float(st.session_state.get("toolbox_talk_kiosk_complete_at", 0.0))
    if completed_message and completed_at and (time.time() - completed_at) >= 8:
        st.session_state.pop("toolbox_talk_kiosk_complete_message", None)
        st.session_state.pop("toolbox_talk_kiosk_complete_at", None)
        st.session_state["toolbox_talk_reset_pending"] = True
        st.session_state["toolbox_talk_canvas_revision"] = (
            int(st.session_state.get("toolbox_talk_canvas_revision", 0)) + 1
        )
        st.rerun()

    active_attendance_entries = list_daily_attendance_entries(
        repository,
        site_name=project_setup.current_site_name,
        on_date=date.today(),
        active_only=True,
    )
    pending_attendance_entries = sorted(
        _build_pending_toolbox_talk_attendance_entries(
            active_attendance_entries,
            list_toolbox_talk_completions(
                repository,
                site_name=project_setup.current_site_name,
                topic=resolved_topic,
                on_date=date.today(),
            ),
        ),
        key=lambda entry: (
            entry.individual_name.casefold(),
            entry.contractor_name.casefold(),
            entry.time_in,
        ),
    )

    if UPLANDS_LOGO.exists():
        logo_columns = st.columns([1, 1.2, 1])
        with logo_columns[1]:
            st.image(str(UPLANDS_LOGO), width="stretch")

    st.markdown(
        (
            "<div class='panel-card'>"
            "<div class='panel-heading'>UHSF16.2 TOOLBOX TALK</div>"
            f"<div class='panel-title'>{html.escape(resolved_topic)}</div>"
            "<div class='panel-caption'>"
            "Read the document, pick your name, confirm you understand it, then sign once on screen."
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    _render_broadcast_status_badges(
        [
            ("1 Read document", "neutral"),
            ("2 Choose your name", "neutral"),
            ("3 Confirm", "neutral"),
            ("4 Sign", "success"),
        ]
    )

    try:
        source_document_bytes, source_document_mime_type = read_toolbox_talk_document_bytes(
            source_document
        )
    except FileNotFoundError:
        st.error("The attached toolbox talk document is missing. Ask the manager to upload it again.")
        return

    source_document_view_url = build_toolbox_talk_document_view_url(
        source_document.doc_id,
        public_url=project_setup.public_tunnel_url,
    )
    document_action_columns = st.columns(2, gap="medium")
    with document_action_columns[0]:
        if source_document_view_url:
            st.link_button(
                "📖 Open TBT Document",
                url=source_document_view_url,
                width="stretch",
            )
    with document_action_columns[1]:
        st.download_button(
            "📥 Download Copy",
            data=source_document_bytes,
            file_name=source_document.original_file_name,
            mime=source_document_mime_type,
            width="stretch",
            key=f"toolbox_talk_source_doc_{source_document.doc_id}",
        )
    st.info(
        "Open the toolbox talk in your browser first. Download is still there if your phone cannot preview the file type."
    )

    if completed_message:
        st.success(completed_message)
        st.info("This form will reset automatically for the next operative.")
        components.html(
            """
            <script>
            setTimeout(function () {
                window.parent.location.reload();
            }, 8000);
            </script>
            """,
            height=0,
        )
        return

    if not active_attendance_entries:
        st.info("Nobody is currently on the live fire roll, so the toolbox talk signer is paused.")
        return

    if not pending_attendance_entries:
        st.success("Everyone currently on site has already signed this toolbox talk.")
        return

    if len(pending_attendance_entries) == 1 and not str(
        st.session_state.get("toolbox_talk_selected_attendance_doc_id", "")
    ).strip():
        st.session_state["toolbox_talk_selected_attendance_doc_id"] = (
            pending_attendance_entries[0].doc_id
        )

    signer_search = st.text_input(
        "Find Your Name",
        key="toolbox_talk_signer_search",
        placeholder="Start typing your name or company",
    )
    search_value = signer_search.strip().casefold()
    filtered_attendance_entries = [
        entry
        for entry in pending_attendance_entries
        if not search_value
        or search_value in entry.individual_name.casefold()
        or search_value in entry.contractor_name.casefold()
        or search_value in (entry.vehicle_registration or "").casefold()
    ]
    if not filtered_attendance_entries:
        st.warning("No pending signer matches that search. Clear it and try again.")
        filtered_attendance_entries = pending_attendance_entries

    signer_options = [""] + [entry.doc_id for entry in filtered_attendance_entries]
    if (
        st.session_state.get("toolbox_talk_selected_attendance_doc_id")
        not in signer_options
    ):
        st.session_state["toolbox_talk_selected_attendance_doc_id"] = ""
    selected_attendance_doc_id = st.selectbox(
        "Select Your Name",
        options=signer_options,
        key="toolbox_talk_selected_attendance_doc_id",
        format_func=lambda doc_id: (
            "Choose operative"
            if not doc_id
            else _toolbox_talk_signer_label(
                next(
                    entry
                    for entry in filtered_attendance_entries
                    if entry.doc_id == doc_id
                )
            )
        ),
    )
    selected_attendance_entry = next(
        (
            entry
            for entry in filtered_attendance_entries
            if entry.doc_id == selected_attendance_doc_id
        ),
        None,
    )
    st.caption(
        f"Pending live signers: {len(pending_attendance_entries)} | Showing: {len(filtered_attendance_entries)}"
    )
    if selected_attendance_entry is not None:
        st.markdown(
            (
                "<div class='panel-card'>"
                "<div class='panel-heading'>Signing As</div>"
                f"<div class='panel-title'>{html.escape(selected_attendance_entry.individual_name)}</div>"
                "<div class='panel-caption'>"
                f"{html.escape(selected_attendance_entry.contractor_name)}"
                f" | On site since {selected_attendance_entry.time_in:%H:%M}"
                f"{' | Vehicle ' + html.escape(selected_attendance_entry.vehicle_registration) if selected_attendance_entry.vehicle_registration else ''}"
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    read_confirmed = st.checkbox(
        "I confirm I have read and understood the attached Toolbox Talk document.",
        key="toolbox_talk_read_confirmed",
    )
    if not read_confirmed:
        st.warning("Read the attached toolbox talk and tick the confirmation box before signing.")

    st.markdown(
        "<div class='file-2-section-heading'>Signature</div>",
        unsafe_allow_html=True,
    )
    canvas_revision = int(st.session_state.get("toolbox_talk_canvas_revision", 0))
    canvas_result = st_canvas(
        update_streamlit=True,
        key=f"toolbox_talk_canvas_{canvas_revision}",
        height=200,
        width=340,
        stroke_width=3,
        stroke_color="#000000",
        background_color="#ffffff",
        drawing_mode="freedraw",
        display_toolbar=False,
    )

    action_columns = st.columns([0.35, 0.65], gap="medium")
    with action_columns[0]:
        if st.button(
            "🧽 Clear Signature",
            key="clear_toolbox_talk_signature",
            width="stretch",
            type="secondary",
        ):
            st.session_state["toolbox_talk_canvas_revision"] = canvas_revision + 1
            st.rerun()
    with action_columns[1]:
        if st.button(
            "✅ Submit",
            key="submit_toolbox_talk_completion",
            width="stretch",
            disabled=(not read_confirmed or selected_attendance_entry is None),
        ):
            try:
                if selected_attendance_entry is None:
                    raise ValidationError("Select your name before submitting the toolbox talk.")
                logged_completion = log_toolbox_talk_completion(
                    repository,
                    site_name=project_setup.current_site_name,
                    topic=resolved_topic,
                    attendance_entry=selected_attendance_entry,
                    signature_image_data=canvas_result.image_data,
                    document_read_confirmed=read_confirmed,
                )
            except ValidationError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"Unable to log the toolbox talk signature: {exc}")
            else:
                st.session_state["toolbox_talk_kiosk_complete_message"] = (
                    f"Toolbox Talk signed. Thank you, {logged_completion.toolbox_talk_completion.individual_name}."
                )
                st.session_state["toolbox_talk_kiosk_complete_at"] = time.time()
                st.session_state["toolbox_talk_reset_pending"] = True
                st.session_state["toolbox_talk_canvas_revision"] = canvas_revision + 1
                st.rerun()


def _render_site_induction_capture_form(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
    *,
    induction_company_options: List[str],
    is_kiosk: bool,
    st_canvas: Any,
) -> None:
    """Render the UHSF16.01 induction capture form."""

    reset_caption = (
        "Reset clears the current induction form, signature canvas, and any staged competency uploads."
    )
    top_action_columns = st.columns([0.34, 0.66], gap="medium")
    with top_action_columns[0]:
        if st.button(
            "↺ Reset Induction Form",
            key="top_reset_site_induction_form",
            width="stretch",
            type="secondary",
        ):
            st.session_state["site_induction_reset_pending"] = True
            st.session_state["site_induction_canvas_revision"] = (
                int(st.session_state.get("site_induction_canvas_revision", 0)) + 1
            )
            st.session_state.pop("site_induction_kiosk_complete_name", None)
            st.session_state.pop("site_induction_kiosk_complete_at", None)
            st.session_state["site_induction_flash"] = "Induction form reset."
            st.rerun()
    with top_action_columns[1]:
        st.caption(reset_caption)
    st.markdown("")

    if (
        "site_induction_company_selection" not in st.session_state
        or st.session_state["site_induction_company_selection"]
        not in induction_company_options
    ):
        st.session_state["site_induction_company_selection"] = (
            "-- Select Company --"
        )
    selected_company_option = st.selectbox(
        "Company / Contractor Name",
        induction_company_options,
        key="site_induction_company_selection",
    )

    _render_workspace_zone_heading(
        "Step 1 · Operative & Employer",
        "Start with the operative, employer, and the contact details that must appear on the filed induction.",
    )
    if selected_company_option == "🏢 New Company (Type Below)":
        company = st.text_input(
            "Enter New Company Name",
            key="site_induction_new_company_name",
        )
    else:
        company = selected_company_option

    identity_columns = st.columns(2, gap="large")
    with identity_columns[0]:
        full_name = st.text_input("Full Name", key="site_induction_full_name")
        occupation = st.text_input("Occupation", key="site_induction_occupation")
        contact_number = st.text_input(
            "Contact Number",
            key="site_induction_contact_number",
            placeholder="Enter your mobile number",
        )
    with identity_columns[1]:
        home_address = st.text_area(
            "Home Address",
            key="site_induction_home_address",
            height=110,
            placeholder="Enter your full home address",
        )

    _render_workspace_zone_heading(
        "Step 2 · Emergency & Welfare",
        "Capture the key welfare and emergency details before moving into competence evidence.",
    )
    emergency_columns = st.columns(2, gap="large")
    with emergency_columns[0]:
        emergency_contact = st.text_input(
            "Emergency Contact",
            key="site_induction_emergency_contact",
        )
        emergency_tel = st.text_input(
            "Emergency Tel",
            key="site_induction_emergency_tel",
        )
    with emergency_columns[1]:
        medical = st.text_area(
            "Medical / Welfare Notes",
            key="site_induction_medical",
            height=110,
            placeholder="Enter any declared conditions, medication, or relevant site welfare notes",
        )

    _render_workspace_zone_heading(
        "Step 3 · Core Cards & Mandatory Evidence",
        "The digital induction now requires the operative's core card details and a manual handling certificate upload.",
    )
    core_competence_columns = st.columns(2, gap="large")
    with core_competence_columns[0]:
        cscs_number = st.text_input("CSCS No.", key="site_induction_cscs_number")
        cscs_expiry = st.date_input(
            "CSCS Expiry Date",
            value=None,
            format="DD/MM/YYYY",
            key="site_induction_cscs_expiry",
        )
        cscs_card_upload = st.file_uploader(
            "📸 Upload CSCS Card",
            type=["png", "jpg", "jpeg", "pdf"],
            accept_multiple_files=False,
            key="site_induction_cscs_card_upload",
        )
        competency_expiry_date = st.date_input(
            "📅 Primary Competency Card Expiry Date",
            format="DD/MM/YYYY",
            key="site_induction_competency_expiry_date",
        )
    with core_competence_columns[1]:
        asbestos_cert = _render_site_induction_yes_no_field(
            "Asbestos Awareness Certificate",
            key="site_induction_asbestos_cert",
        )
        asbestos_card_upload = None
        if asbestos_cert:
            asbestos_card_upload = st.file_uploader(
                "📄 Upload Asbestos Certificate",
                type=["png", "jpg", "jpeg", "pdf"],
                accept_multiple_files=False,
                key="site_induction_asbestos_card_upload",
            )
        manual_handling_card_upload = st.file_uploader(
            f"📄 Upload {MANDATORY_MANUAL_HANDLING_LABEL}",
            type=["png", "jpg", "jpeg", "pdf"],
            accept_multiple_files=False,
            key="site_induction_manual_handling_upload",
            help="This certificate is mandatory for the app induction workflow.",
        )
        st.caption(
            "Manual handling evidence is mandatory for this digital induction workflow."
        )

    if is_kiosk:
        scaffold_section = st.container()
        plant_section = st.container()
    else:
        competence_columns = st.columns(2, gap="large")
        scaffold_section, plant_section = competence_columns

    _render_workspace_zone_heading(
        "Step 4 · Specialist Activities & Role Evidence",
        "Only capture the extra cards and certificates that apply to this operative's work on site.",
    )
    with scaffold_section:
        erect_scaffold = _render_site_induction_yes_no_field(
            "Are you erecting scaffold?",
            key="site_induction_erect_scaffold",
        )
        cisrs_no = ""
        cisrs_expiry = None
        cisrs_card_upload = None
        if erect_scaffold:
            cisrs_no = st.text_input(
                "CISRS No.",
                key="site_induction_cisrs_no",
            )
            cisrs_expiry = st.date_input(
                "CISRS Expiry",
                value=None,
                format="DD/MM/YYYY",
                key="site_induction_cisrs_expiry",
            )
            cisrs_card_upload = st.file_uploader(
                "📸 Upload CISRS Card",
                type=["png", "jpg", "jpeg", "pdf"],
                accept_multiple_files=False,
                key="site_induction_cisrs_card_upload",
            )
        st.markdown("**Role Certificates**")
        first_aider = _render_site_induction_yes_no_field(
            "First Aider",
            key="site_induction_first_aider",
        )
        first_aider_upload = None
        if first_aider:
            first_aider_upload = st.file_uploader(
                "📄 Upload First Aid Certificate",
                type=["png", "jpg", "jpeg", "pdf"],
                accept_multiple_files=False,
                key="site_induction_first_aider_upload",
            )
        fire_warden = _render_site_induction_yes_no_field(
            "Fire Warden",
            key="site_induction_fire_warden",
        )
        fire_warden_upload = None
        if fire_warden:
            fire_warden_upload = st.file_uploader(
                "📄 Upload Fire Warden Certificate",
                type=["png", "jpg", "jpeg", "pdf"],
                accept_multiple_files=False,
                key="site_induction_fire_warden_upload",
            )
        supervisor = _render_site_induction_yes_no_field(
            "Supervisor",
            key="site_induction_supervisor",
        )
        supervisor_upload = None
        if supervisor:
            supervisor_upload = st.file_uploader(
                "📄 Upload Supervisor Certificate",
                type=["png", "jpg", "jpeg", "pdf"],
                accept_multiple_files=False,
                key="site_induction_supervisor_upload",
            )
        smsts = _render_site_induction_yes_no_field(
            "SMSTS",
            key="site_induction_smsts",
        )
        smsts_upload = None
        if smsts:
            smsts_upload = st.file_uploader(
                "📄 Upload SMSTS Certificate",
                type=["png", "jpg", "jpeg", "pdf"],
                accept_multiple_files=False,
                key="site_induction_smsts_upload",
            )
    with plant_section:
        operate_plant = _render_site_induction_yes_no_field(
            "Are you operating plant?",
            key="site_induction_operate_plant",
        )
        cpcs_no = ""
        cpcs_expiry = None
        cpcs_card_upload = None
        if operate_plant:
            cpcs_no = st.text_input(
                "CPCS No.",
                key="site_induction_cpcs_no",
            )
            cpcs_expiry = st.date_input(
                "CPCS Expiry",
                value=None,
                format="DD/MM/YYYY",
                key="site_induction_cpcs_expiry",
            )
            cpcs_card_upload = st.file_uploader(
                "📸 Upload CPCS Card",
                type=["png", "jpg", "jpeg", "pdf"],
                accept_multiple_files=False,
                key="site_induction_cpcs_card_upload",
            )
        st.markdown("**Site-Specific Training**")
        st.caption(
            "Record any client-specific training and attach the supporting evidence if the operative has it."
        )
        client_training_desc = st.text_area(
            "Client Specific Training",
            key="site_induction_client_training_desc",
            height=110,
            placeholder="Describe any client-specific training completed",
        )
        training_date_columns = st.columns(2)
        with training_date_columns[0]:
            client_training_date = st.date_input(
                "Training Date",
                value=None,
                format="DD/MM/YYYY",
                key="site_induction_client_training_date",
            )
        with training_date_columns[1]:
            client_training_expiry = st.date_input(
                "Training Expiry",
                value=None,
                format="DD/MM/YYYY",
                key="site_induction_client_training_expiry",
            )
        client_training_upload = st.file_uploader(
            "📄 Upload Client Training Evidence",
            type=["png", "jpg", "jpeg", "pdf"],
            accept_multiple_files=False,
            key="site_induction_client_training_upload",
        )

    uploaded_evidence_count = len(
        [
            uploaded_file
            for uploaded_file in (
                cscs_card_upload,
                asbestos_card_upload if asbestos_cert else None,
                manual_handling_card_upload,
                cisrs_card_upload if erect_scaffold else None,
                first_aider_upload if first_aider else None,
                fire_warden_upload if fire_warden else None,
                supervisor_upload if supervisor else None,
                smsts_upload if smsts else None,
                cpcs_card_upload if operate_plant else None,
                client_training_upload,
            )
            if uploaded_file is not None
        ]
    )
    company_readiness_value = (
        company.strip()
        if selected_company_option != "-- Select Company --" and company.strip()
        else "Needed"
    )
    if is_kiosk:
        readiness_rows = [
            st.columns(2, gap="medium"),
            st.columns(2, gap="medium"),
        ]
        readiness_targets = [
            (readiness_rows[0][0], "Company", company_readiness_value, "🏢"),
            (readiness_rows[0][1], "Phone", "Ready" if contact_number.strip() else "Missing", "📱"),
            (readiness_rows[1][0], "Evidence Files", str(uploaded_evidence_count), "🎫"),
            (
                readiness_rows[1][1],
                "Manual Handling",
                "Ready" if manual_handling_card_upload is not None else "Required",
                "🧰",
            ),
        ]
        for column, label, value, icon in readiness_targets:
            with column:
                _render_inline_metric(label, value, icon=icon)
    else:
        readiness_columns = st.columns(4, gap="medium")
        readiness_targets = [
            (readiness_columns[0], "Company", company_readiness_value, "🏢"),
            (readiness_columns[1], "Phone", "Ready" if contact_number.strip() else "Missing", "📱"),
            (readiness_columns[2], "Evidence Files", str(uploaded_evidence_count), "🎫"),
            (
                readiness_columns[3],
                "Manual Handling",
                "Ready" if manual_handling_card_upload is not None else "Required",
                "🧰",
            ),
        ]
        for column, label, value, icon in readiness_targets:
            with column:
                _render_inline_metric(label, value, icon=icon)

    st.caption(
        "On mobile, tap each upload field to take a live photo of the relevant card or certificate. Manual handling evidence is mandatory."
    )

    _render_workspace_zone_heading(
        "Step 6 · Operative Signature",
        "Once the details and evidence are complete, capture the operative signature to lock the induction into the site file.",
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

    action_columns = (
        st.columns([0.28, 0.32, 0.40], gap="medium")
        if is_kiosk
        else st.columns([0.24, 0.24, 0.52], gap="large")
    )
    with action_columns[0]:
        if st.button(
            "🧽 Clear Signature",
            key="clear_site_induction_signature",
            width="stretch",
            type="secondary",
        ):
            st.session_state["site_induction_canvas_revision"] = canvas_revision + 1
            st.rerun()
    with action_columns[1]:
        if st.button(
            "↺ Clear Induction Form",
            key="clear_site_induction_form",
            width="stretch",
            type="secondary",
        ):
            st.session_state["site_induction_reset_pending"] = True
            st.session_state["site_induction_canvas_revision"] = canvas_revision + 1
            st.session_state.pop("site_induction_kiosk_complete_name", None)
            st.session_state.pop("site_induction_kiosk_complete_at", None)
            st.rerun()
    with action_columns[2]:
        if st.button("✅ Submit Induction", width="stretch"):
            try:
                if selected_company_option == "-- Select Company --":
                    raise ValidationError(
                        "Please select or enter your company name."
                    )
                resolved_company_name = (
                    company.strip()
                    if selected_company_option == "🏢 New Company (Type Below)"
                    else selected_company_option.strip()
                )
                if not resolved_company_name:
                    raise ValidationError(
                        "Please select or enter your company name."
                    )
                resolved_home_address = str(
                    st.session_state.get("site_induction_home_address", home_address)
                ).strip()
                resolved_contact_number = str(
                    st.session_state.get("site_induction_contact_number", contact_number)
                ).strip()
                generated_document = create_site_induction_document(
                    repository,
                    site_name=project_setup.current_site_name,
                    full_name=full_name,
                    home_address=resolved_home_address,
                    contact_number=resolved_contact_number,
                    company=resolved_company_name,
                    occupation=occupation,
                    emergency_contact=emergency_contact,
                    emergency_tel=emergency_tel,
                    medical=medical,
                    cscs_number=cscs_number,
                    cscs_expiry=cscs_expiry,
                    asbestos_cert=asbestos_cert,
                    erect_scaffold=erect_scaffold,
                    cisrs_no=cisrs_no,
                    cisrs_expiry=cisrs_expiry,
                    operate_plant=operate_plant,
                    cpcs_no=cpcs_no,
                    cpcs_expiry=cpcs_expiry,
                    client_training_desc=client_training_desc,
                    client_training_date=client_training_date,
                    client_training_expiry=client_training_expiry,
                    first_aider=first_aider,
                    fire_warden=fire_warden,
                    supervisor=supervisor,
                    smsts=smsts,
                    competency_expiry_date=competency_expiry_date,
                    competency_files=_build_site_induction_competency_file_payloads(
                        [
                            ("CSCS Card", cscs_card_upload),
                            (MANDATORY_MANUAL_HANDLING_LABEL, manual_handling_card_upload),
                            ("Asbestos Certificate", asbestos_card_upload if asbestos_cert else None),
                            ("CISRS Card", cisrs_card_upload if erect_scaffold else None),
                            ("First Aid Certificate", first_aider_upload if first_aider else None),
                            ("Fire Warden Certificate", fire_warden_upload if fire_warden else None),
                            ("Supervisor Certificate", supervisor_upload if supervisor else None),
                            ("SMSTS Certificate", smsts_upload if smsts else None),
                            ("CPCS Card", cpcs_card_upload if operate_plant else None),
                            ("Client Training Evidence", client_training_upload),
                        ]
                    ),
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
                if is_kiosk:
                    _route_kiosk_to_induction_station(kiosk_view="induction")
                    st.session_state["site_induction_kiosk_complete_name"] = (
                        generated_document.induction_document.individual_name
                    )
                    st.session_state["site_induction_kiosk_complete_doc_id"] = (
                        generated_document.induction_document.doc_id
                    )
                    st.session_state["site_induction_kiosk_complete_at"] = time.time()
                else:
                    st.session_state["site_induction_flash"] = (
                        "Induction Complete. Welcome to site, "
                        f"{generated_document.induction_document.individual_name}!"
                    )
                st.rerun()
    if not is_kiosk:
        with action_columns[2]:
            st.caption(f"Site: {project_setup.current_site_name}")
            st.caption("Template: templates/UHSF16.01_Template.docx")
            st.caption(f"Signatures: {FILE_3_SIGNATURES_DIR.name}")
            st.caption(f"Completed docs: {FILE_3_COMPLETED_INDUCTIONS_DIR.name}")


def _render_manager_attendance_register_tab(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
    *,
    induction_picker_records: List[InductionDocument],
    active_attendance_entries: List[DailyAttendanceEntryDocument],
    todays_attendance_entries: List[DailyAttendanceEntryDocument],
) -> None:
    """Render the live attendance register manager workspace."""

    attendance_flash_message = st.session_state.pop(
        "site_attendance_delete_flash",
        None,
    )
    if attendance_flash_message:
        st.success(attendance_flash_message)
    attendance_edit_flash_message = st.session_state.pop(
        "site_attendance_edit_flash",
        None,
    )
    if attendance_edit_flash_message:
        st.success(attendance_edit_flash_message)

    st.caption(
        "Run the live sign-in console, monitor who is on site, and keep an eye on competency risk from one operational view."
    )
    _render_competency_compliance_radar(repository, active_attendance_entries)
    _render_site_gate_fallback_panel(project_setup)
    _render_live_fire_roll_panel(active_attendance_entries)
    _render_todays_attendance_activity_panel(todays_attendance_entries)
    _render_manager_attendance_correction_panel(
        repository,
        project_setup=project_setup,
    )

    site_attendance_history = [
        document
        for document in repository.list_documents(
            document_type=DailyAttendanceEntryDocument.document_type,
            site_name=project_setup.current_site_name,
        )
        if isinstance(document, DailyAttendanceEntryDocument)
    ]
    pending_clear_today = bool(
        st.session_state.get("site_attendance_clear_today_pending")
    )
    pending_clear_all = bool(
        st.session_state.get("site_attendance_clear_all_pending")
    )

    with st.expander("Reset Attendance Data", expanded=False):
        st.caption(
            "Use these controls when you need to wipe test sign-ins or start the attendance station from a clean slate."
        )
        reset_metrics = st.columns(3, gap="large")
        with reset_metrics[0]:
            _render_inline_metric(
                "Today Entries",
                str(len(todays_attendance_entries)),
                icon="📅",
            )
        with reset_metrics[1]:
            _render_inline_metric(
                "On Site Now",
                str(len(active_attendance_entries)),
                icon="🔥",
            )
        with reset_metrics[2]:
            _render_inline_metric(
                "Site History",
                str(len(site_attendance_history)),
                icon="🗃️",
            )

        reset_action_columns = st.columns(2, gap="medium")
        with reset_action_columns[0]:
            if st.button(
                "🗑️ Clear Today's Attendance",
                key="clear-todays-attendance",
                width="stretch",
                type="secondary",
                disabled=not todays_attendance_entries,
            ):
                st.session_state["site_attendance_clear_today_pending"] = True
                st.session_state.pop("site_attendance_clear_all_pending", None)
                st.rerun()
        with reset_action_columns[1]:
            if st.button(
                "🧨 Clear All Site Attendance History",
                key="clear-all-site-attendance",
                width="stretch",
                type="secondary",
                disabled=not site_attendance_history,
            ):
                st.session_state["site_attendance_clear_all_pending"] = True
                st.session_state.pop("site_attendance_clear_today_pending", None)
                st.rerun()

        if pending_clear_today:
            st.warning(
                "Clear today's attendance activity for this site? This will remove the "
                "saved sign-in/sign-out records and attempt to delete the linked "
                "attendance signature files."
            )
            clear_today_columns = st.columns([1.2, 1.0, 4.0], gap="small")
            if clear_today_columns[0].button(
                "Confirm Clear Today",
                key="confirm-clear-todays-attendance",
                width="stretch",
            ):
                deleted_paths = repository.delete_documents_and_files(
                    entry.doc_id for entry in todays_attendance_entries
                )
                st.session_state.pop("site_attendance_clear_today_pending", None)
                _reset_site_attendance_form_state()
                st.session_state["site_attendance_delete_flash"] = (
                    f"Cleared {len(todays_attendance_entries)} attendance record(s) for today."
                    + (
                        f" Removed {len(deleted_paths)} linked file(s)."
                        if deleted_paths
                        else " No linked files were present on disk."
                    )
                )
                st.rerun()
            if clear_today_columns[1].button(
                "Cancel",
                key="cancel-clear-todays-attendance",
                width="stretch",
            ):
                st.session_state.pop("site_attendance_clear_today_pending", None)
                st.rerun()

        if pending_clear_all:
            st.warning(
                "Clear all saved attendance history for this site? This wipes the site's "
                "full UHSF16.09 attendance history and linked signature files."
            )
            clear_all_columns = st.columns([1.2, 1.0, 4.0], gap="small")
            if clear_all_columns[0].button(
                "Confirm Clear All",
                key="confirm-clear-all-site-attendance",
                width="stretch",
            ):
                deleted_paths = repository.delete_documents_and_files(
                    entry.doc_id for entry in site_attendance_history
                )
                st.session_state.pop("site_attendance_clear_all_pending", None)
                _reset_site_attendance_form_state()
                st.session_state["site_attendance_delete_flash"] = (
                    f"Cleared {len(site_attendance_history)} attendance record(s) for "
                    f"{project_setup.current_site_name}."
                    + (
                        f" Removed {len(deleted_paths)} linked file(s)."
                        if deleted_paths
                        else " No linked files were present on disk."
                    )
                )
                st.rerun()
            if clear_all_columns[1].button(
                "Cancel",
                key="cancel-clear-all-site-attendance",
                width="stretch",
            ):
                st.session_state.pop("site_attendance_clear_all_pending", None)
                st.rerun()

    st.divider()
    st.markdown(
        "<div class='file-2-section-heading'>Daily Attendance Console</div>",
        unsafe_allow_html=True,
    )
    _render_site_attendance_console(
        repository,
        project_setup=project_setup,
        site_name=project_setup.current_site_name,
        public_url=project_setup.public_tunnel_url,
        induction_picker_records=induction_picker_records,
        active_attendance_entries=active_attendance_entries,
        is_kiosk=False,
    )


def _render_manager_attendance_export_tab(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render poster generation and attendance print controls."""

    st.caption(
        "Generate the live kiosk poster and print any saved UHSF16.09 attendance day into the official File 2 register."
    )
    poster_action_columns = st.columns([1.1, 2.4], gap="large")
    with poster_action_columns[0]:
        if st.button(
            "🪧 Generate Site Poster",
            key="generate_site_induction_poster",
            width="stretch",
        ):
            try:
                poster = generate_site_induction_poster(
                    site_name=project_setup.current_site_name,
                    logo_path=UPLANDS_LOGO if UPLANDS_LOGO.exists() else None,
                    public_url=project_setup.public_tunnel_url,
                )
            except RuntimeError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.error(f"Unable to generate the site poster: {exc}")
            else:
                st.session_state["site_induction_poster_png"] = poster.poster_png
                st.session_state["site_induction_qr_png"] = poster.qr_code_png
                st.session_state["site_induction_qr_url"] = poster.induction_url
    with poster_action_columns[1]:
        st.caption(ATTENDANCE_FORM_METADATA)

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
                    "Daily attendance QR for operatives arriving on site."
                    "</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            if UPLANDS_LOGO.exists():
                st.image(str(UPLANDS_LOGO), width=260)
            st.image(qr_png, width=360)
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
                width="stretch",
                key="download_site_induction_poster",
            )
        with poster_columns[1]:
            st.image(
                poster_png,
                caption="Printable site sign-in poster",
                width="stretch",
            )

    if "attendance_register_print_date" not in st.session_state:
        st.session_state["attendance_register_print_date"] = date.today()

    shortcut_columns = st.columns(4, gap="small")
    with shortcut_columns[0]:
        if st.button(
            "Today",
            key="attendance_register_date_today",
            width="stretch",
        ):
            st.session_state["attendance_register_print_date"] = date.today()
            st.rerun()
    with shortcut_columns[1]:
        if st.button(
            "Yesterday",
            key="attendance_register_date_yesterday",
            width="stretch",
        ):
            st.session_state["attendance_register_print_date"] = (
                date.today() - timedelta(days=1)
            )
            st.rerun()
    with shortcut_columns[2]:
        if st.button(
            "This Monday",
            key="attendance_register_date_monday",
            width="stretch",
        ):
            st.session_state["attendance_register_print_date"] = (
                date.today() - timedelta(days=date.today().weekday())
            )
            st.rerun()
    with shortcut_columns[3]:
        if st.button(
            "Last Friday",
            key="attendance_register_date_last_friday",
            width="stretch",
        ):
            days_since_friday = (date.today().weekday() - 4) % 7
            if days_since_friday == 0:
                days_since_friday = 7
            st.session_state["attendance_register_print_date"] = (
                date.today() - timedelta(days=days_since_friday)
            )
            st.rerun()

    selected_register_date = st.date_input(
        "Register Date",
        max_value=date.today(),
        key="attendance_register_print_date",
    )
    selected_register_entries = list_daily_attendance_entries(
        repository,
        site_name=project_setup.current_site_name,
        on_date=selected_register_date,
        active_only=False,
    )

    print_columns = st.columns([0.9, 0.75, 1.45], gap="large")
    with print_columns[0]:
        if st.button(
            "🖨️ Print Attendance Register",
            key="print_daily_attendance_register",
            width="stretch",
        ):
            try:
                generated_register = generate_attendance_register_document(
                    repository,
                    site_name=project_setup.current_site_name,
                    on_date=selected_register_date,
                )
            except TemplateValidationError as exc:
                st.error(
                    f"Official attendance register template failed validation: {exc}"
                )
            except Exception as exc:
                st.error(f"Unable to generate attendance register: {exc}")
            else:
                _open_file_for_printing(generated_register.output_path)
                st.success(
                    f"Attendance register ready: {generated_register.output_path}"
                )
                st.caption(
                    "Rows printed: "
                    f"{generated_register.row_count} | Output folder: "
                    f"{FILE_2_ATTENDANCE_OUTPUT_DIR.name}"
                )
    with print_columns[1]:
        _render_inline_metric(
            "Selected Day Rows",
            str(len(selected_register_entries)),
            icon="📋",
        )
    with print_columns[2]:
        st.caption(
            "Print any saved UHSF16.09 day into the official File 2 register, including the live sign-in and sign-out signatures."
        )
        st.caption(
            f"Current print date: {selected_register_date.strftime('%d/%m/%Y')}"
        )


def _render_site_induction_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
    *,
    is_kiosk: bool = False,
    ) -> None:
    """Render the UHSF16.09 attendance station plus manager induction tools."""

    if st.session_state.pop("site_attendance_reset_pending", False):
        _reset_site_attendance_form_state()
    if st.session_state.pop("site_induction_reset_pending", False):
        _reset_site_induction_form_state()
    if "site_induction_competency_expiry_date" not in st.session_state:
        st.session_state["site_induction_competency_expiry_date"] = (
            date.today() + timedelta(days=365)
        )

    kiosk_active_view = (
        _get_kiosk_view_from_query_params()
        or str(st.session_state.get("site_kiosk_active_view", "attendance")).strip().lower()
    )
    if kiosk_active_view not in {"attendance", "induction"}:
        kiosk_active_view = "attendance"
        st.session_state["site_kiosk_active_view"] = kiosk_active_view

    if is_kiosk:
        completed_message = str(
            st.session_state.get("site_attendance_kiosk_complete_message", "")
        )
        completed_at = float(
            st.session_state.get("site_attendance_kiosk_complete_at", 0.0)
        )
        if completed_message and completed_at and (time.time() - completed_at) >= 10:
            st.session_state.pop("site_attendance_kiosk_complete_message", None)
            st.session_state.pop("site_attendance_kiosk_complete_at", None)
            _route_kiosk_to_induction_station(kiosk_view="attendance")
            st.rerun()
    else:
        completed_message = ""
        st.session_state.pop("site_kiosk_active_view", None)
        st.session_state.pop("site_attendance_kiosk_complete_message", None)
        st.session_state.pop("site_attendance_kiosk_complete_at", None)
        st.session_state.pop("site_induction_kiosk_complete_name", None)
        st.session_state.pop("site_induction_kiosk_complete_at", None)
        st.session_state.pop("site_induction_kiosk_complete_doc_id", None)
        attendance_flash_message = st.session_state.pop("site_attendance_flash", None)
        if attendance_flash_message is not None:
            st.success(attendance_flash_message)
        induction_flash_message = st.session_state.pop("site_induction_flash", None)
        if induction_flash_message is not None:
            st.success(induction_flash_message)
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
    induction_picker_records = _build_induction_picker_records(inductions)
    induction_company_options = _build_induction_company_options(
        repository,
        site_name=project_setup.current_site_name,
        induction_documents=inductions,
    )
    todays_attendance_entries = list_daily_attendance_entries(
        repository,
        site_name=project_setup.current_site_name,
        on_date=date.today(),
        active_only=False,
    )
    active_attendance_entries = list_daily_attendance_entries(
        repository,
        site_name=project_setup.current_site_name,
        on_date=date.today(),
        active_only=True,
    )

    if UPLANDS_LOGO.exists():
        logo_columns = st.columns([1, 1.2, 1])
        with logo_columns[1]:
            st.image(str(UPLANDS_LOGO), width="stretch")

    if is_kiosk and kiosk_active_view == "attendance":
        st.markdown(
            (
                "<div class='panel-card'>"
                "<div class='panel-heading'>📅 SITE ATTENDANCE REGISTER (UHSF16.09)</div>"
                "<div class='panel-title'>Daily Sign-In / Sign-Out</div>"
                "<div class='panel-caption'>"
                "Select your inducted record, sign in or out, and sign on screen."
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        if completed_message:
            st.success(completed_message)
            st.info("Please return the device or close the tab. This form will reset automatically.")
            components.html(
                """
                <script>
                setTimeout(function () {
                    const url = new URL(window.parent.location.href);
                    url.searchParams.set("station", "induction");
                    url.searchParams.set("mode", "kiosk");
                    url.searchParams.set("kiosk_view", "attendance");
                    window.parent.location.href = url.toString();
                }, 10000);
                </script>
                """,
                height=0,
            )
            return
        _render_kiosk_new_starter_call_to_action()
        _render_site_attendance_console(
            repository,
            project_setup=project_setup,
            site_name=project_setup.current_site_name,
            public_url=project_setup.public_tunnel_url,
            induction_picker_records=induction_picker_records,
            active_attendance_entries=active_attendance_entries,
            is_kiosk=True,
        )
        return
    if is_kiosk and kiosk_active_view == "induction":
        induction_complete_name = str(
            st.session_state.get("site_induction_kiosk_complete_name", "")
        ).strip()
        induction_complete_at = float(
            st.session_state.get("site_induction_kiosk_complete_at", 0.0)
        )
        if (
            induction_complete_name
            and induction_complete_at
            and (time.time() - induction_complete_at) >= 8
        ):
            induction_complete_doc_id = str(
                st.session_state.get("site_induction_kiosk_complete_doc_id", "") or ""
            ).strip()
            st.session_state.pop("site_induction_kiosk_complete_name", None)
            st.session_state.pop("site_induction_kiosk_complete_at", None)
            _route_kiosk_to_induction_station(kiosk_view="attendance")
            st.session_state["site_attendance_action_mode"] = "sign_in"
            st.session_state["site_attendance_worker_search"] = induction_complete_name
            st.session_state["site_attendance_prefill_induction_doc_id"] = (
                induction_complete_doc_id
            )
            st.session_state["site_attendance_selected_induction_doc_id"] = (
                induction_complete_doc_id
            )
            st.session_state.pop("site_induction_kiosk_complete_doc_id", None)
            st.rerun()
        st.markdown(
            (
                "<div class='panel-card'>"
                "<div class='panel-heading'>📝 UHSF16.01 SITE INDUCTION</div>"
                "<div class='panel-title'>First-Time Site Induction</div>"
                "<div class='panel-caption'>"
                "Complete your induction below. Once submitted, the kiosk will return you to Daily Attendance so you can sign in."
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        if st.button(
            "← Back to Daily Attendance",
            key="return_kiosk_to_attendance",
            width="stretch",
            type="secondary",
        ):
            _route_kiosk_to_induction_station(kiosk_view="attendance")
            st.session_state["site_induction_reset_pending"] = True
            st.session_state["site_induction_canvas_revision"] = (
                int(st.session_state.get("site_induction_canvas_revision", 0)) + 1
            )
            components.html(
                """
                <script>
                const url = new URL(window.parent.location.href);
                url.searchParams.set("station", "induction");
                url.searchParams.set("mode", "kiosk");
                url.searchParams.set("kiosk_view", "attendance");
                window.parent.location.href = url.toString();
                </script>
                """,
                height=0,
            )
            st.stop()
        if induction_complete_name and induction_complete_at:
            st.success(
                f"Induction Complete. Welcome to site, {induction_complete_name}!"
            )
            st.info(
                "Returning you to Daily Attendance so you can sign in for the day."
            )
            components.html(
                """
                <script>
                setTimeout(function () {
                    const url = new URL(window.parent.location.href);
                    url.searchParams.set("station", "induction");
                    url.searchParams.set("mode", "kiosk");
                    url.searchParams.set("kiosk_view", "attendance");
                    window.parent.location.href = url.toString();
                }, 8000);
                </script>
                """,
                height=0,
            )
            return

    if not is_kiosk:
        _render_workspace_hero(
            icon="🦺",
            kicker="Live Operations",
            title="Site Attendance & Induction",
            caption="Run the live gate, manage first-time inductions, watch the fire roll, and print the official attendance record from one workspace.",
        )
        summary_columns = st.columns([1.2, 0.8], gap="large")
        with summary_columns[0]:
            st.markdown(
                (
                    "<div class='panel-card'>"
                    "<div class='panel-heading'>📅 SITE ATTENDANCE REGISTER (UHSF16.09)</div>"
                    f"<div class='panel-title'>{html.escape(project_setup.current_site_name)} Daily Sign-In Engine</div>"
                    "<div class='panel-caption'>"
                    "Live sign-in, sign-out, fire roll, and vehicle visibility powered by existing induction records."
                    "</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
        with summary_columns[1]:
            _render_metric_card(
                title="Inductions Available",
                icon="📝",
                value=str(len(induction_picker_records)),
                caption="Inducted operatives available for fast daily sign-in.",
                body_html=(
                    "<div class='data-card-subtext'>"
                    f"Signatures folder: <strong>{html.escape(FILE_2_ATTENDANCE_SIGNATURES_DIR.name)}</strong>"
                    "</div>"
                ),
            )

        manager_tabs = st.tabs(
            ["Daily Register", "Print & Export", "Manual Induction", "Recent Inductions"]
        )
        with manager_tabs[0]:
            _render_manager_attendance_register_tab(
                repository,
                project_setup,
                induction_picker_records=induction_picker_records,
                active_attendance_entries=active_attendance_entries,
                todays_attendance_entries=todays_attendance_entries,
            )
        with manager_tabs[1]:
            _render_manager_attendance_export_tab(repository, project_setup)
        with manager_tabs[2]:
            _render_workspace_zone_heading(
                "UHSF16.01 Induction Capture",
                "Use this builder when a new operative needs their first induction before they can appear in the daily sign-in roster.",
            )
            manual_induction_metrics = st.columns(4, gap="large")
            with manual_induction_metrics[0]:
                _render_inline_metric(
                    "Saved Inductions",
                    str(len(inductions)),
                    icon="📝",
                )
            with manual_induction_metrics[1]:
                _render_inline_metric(
                    "Added Today",
                    str(sum(induction.created_at.date() == date.today() for induction in inductions)),
                    icon="📅",
                )
            with manual_induction_metrics[2]:
                _render_inline_metric(
                    "Manual Handling Missing",
                    str(
                        sum(
                            not _induction_has_evidence_label(
                                induction,
                                MANDATORY_MANUAL_HANDLING_LABEL,
                            )
                            for induction in inductions
                        )
                    ),
                    icon="⚠️",
                )
            with manual_induction_metrics[3]:
                _render_inline_metric(
                    "Current Site",
                    project_setup.current_site_name,
                    icon="📍",
                )
            if st.button(
                "↺ Reset Manual Induction Form",
                key="manager_reset_site_induction_form",
                width="stretch",
            ):
                st.session_state["site_induction_reset_pending"] = True
                st.session_state["site_induction_canvas_revision"] = (
                    int(st.session_state.get("site_induction_canvas_revision", 0)) + 1
                )
                st.session_state.pop("site_induction_kiosk_complete_name", None)
                st.session_state.pop("site_induction_kiosk_complete_at", None)
                st.session_state["site_induction_flash"] = (
                    "Manual induction form reset."
                )
                st.rerun()
            st.caption(
                "Reset clears the current manual induction form, signature canvas, and staged competency uploads, including the mandatory manual handling evidence."
            )
            with st.expander("Open Manual Induction Builder", expanded=False):
                _render_site_induction_capture_form(
                    repository,
                    project_setup,
                    induction_company_options=induction_company_options,
                    is_kiosk=False,
                    st_canvas=st_canvas,
                )
        with manager_tabs[3]:
            _render_workspace_zone_heading(
                "Completed Inductions",
                "Open a richer saved-record view, print the full induction pack, or correct the filed details without leaving the app.",
            )
            _render_site_induction_recent_submissions(repository, inductions)
        return

    _render_site_induction_capture_form(
        repository,
        project_setup,
        induction_company_options=induction_company_options,
        is_kiosk=True,
        st_canvas=st_canvas,
    )
    return


def _render_file_4_station(
    repository: DocumentRepository,
    project_setup: ProjectSetup,
) -> None:
    """Render File 4: Permits & Temp Works."""

    _render_workspace_hero(
        icon="⚡",
        kicker="File 4",
        title="Permits & Temp Works",
        caption="Issue permits from live roster data, maintain the running permit history, and keep the physical register ready to print.",
    )
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

    overview_tab, register_tab = st.tabs(["Permit Control", "Live Register"])
    with overview_tab:
        _render_workspace_zone_heading(
            "Primary Action",
            "Issue new ladder permits from the File 4 helper, then use the latest output panel here to check what has just been created.",
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
        st.divider()
        _render_workspace_zone_heading(
            "Export / Print",
            "The printable physical register lives on the Live Register tab so the running permit history stays separate from the control notes.",
        )
        st.info(
            "Use the File 4 quick action in the sidebar to generate a permit. Use the Live Register tab when you need to print the full physical register."
        )
    with register_tab:
        register_summary_columns = st.columns(3)
        with register_summary_columns[0]:
            _render_inline_metric("Active Permits", str(len(ladder_permits)), icon="📄")
        with register_summary_columns[1]:
            _render_inline_metric("Draft Permits", str(len(draft_permits)), icon="✍️")
        with register_summary_columns[2]:
            _render_inline_metric("Indexed Files", str(len(indexed_permits)), icon="📚")

        st.divider()
        _render_workspace_zone_heading(
            "Live Register / History",
            "This is the current File 4 permit history used for the physical UHSF21.00 register.",
        )
        st.markdown(
            "<div class='file-2-section-heading'>Live Permit Register</div>",
            unsafe_allow_html=True,
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
            width="stretch",
            hide_index=True,
        )

        st.divider()
        _render_workspace_zone_heading(
            "Export / Print",
            "Generate the official physical permit register from the live File 4 history.",
        )
        action_columns = st.columns([1.1, 2.4])
        with action_columns[0]:
            if st.button(
                "🖨️ Print Physical Register",
                key="print_physical_register",
                width="stretch",
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


def _render_dispatch_audit_card(
    *,
    label: str,
    value: str,
    caption: str = "",
) -> None:
    """Render one compact audit card inside a dispatch history expander."""

    compact_class = " dispatch-audit-value-compact" if len(value) > 18 else ""
    st.markdown(
        (
            "<div class='dispatch-audit-card'>"
            f"<div class='dispatch-audit-label'>{html.escape(label)}</div>"
            f"<div class='dispatch-audit-value{compact_class}'>{html.escape(value)}</div>"
            f"<div class='dispatch-audit-copy'>{html.escape(caption)}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_dispatch_message_box(message_body: str) -> None:
    """Render one wrapped message body panel for broadcast history."""

    st.markdown(
        f"<div class='dispatch-message-box'>{html.escape(message_body)}</div>",
        unsafe_allow_html=True,
    )


def _render_inline_metric(
    label: str,
    value: str,
    *,
    icon: str,
    border: bool = True,
) -> None:
    """Render a compact metric with a subtle icon in the label."""

    st.metric(f"{icon} {label}", value, border=border)


def _render_workspace_zone_heading(
    title: str,
    caption: str = "",
) -> None:
    """Render a consistent section heading inside a manager workspace."""

    st.markdown(
        f"<div class='file-2-section-heading'>{html.escape(title)}</div>",
        unsafe_allow_html=True,
    )
    if caption:
        st.caption(caption)


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


def _list_workspace_files(directory: Path) -> List[Path]:
    """Return files in one workspace directory sorted newest-first."""

    if not directory.exists():
        return []
    return sorted(
        [
            path
            for path in directory.iterdir()
            if path.is_file()
            and not path.name.startswith(".")
            and not path.name.startswith("._")
        ],
        key=lambda path: (path.stat().st_mtime, path.name.casefold()),
        reverse=True,
    )


def _format_workspace_file_size(byte_count: int) -> str:
    """Return one human-readable file size string."""

    size = float(byte_count)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{int(byte_count)} B"


def _build_workspace_file_rows(files: List[Path]) -> List[Dict[str, str]]:
    """Return UI rows for one workspace file list."""

    rows: List[Dict[str, str]] = []
    for file_path in files:
        file_stat = file_path.stat()
        rows.append(
            {
                "File": file_path.name,
                "Type": file_path.suffix.lstrip(".").upper() or "-",
                "Updated": datetime.fromtimestamp(file_stat.st_mtime).strftime("%d/%m/%Y %H:%M"),
                "Size": _format_workspace_file_size(file_stat.st_size),
            }
        )
    return rows


def _render_file_3_vault_tab(
    *,
    heading: str,
    caption: str,
    directory: Path,
    files: List[Path],
    selection_key: str,
    open_folder_label: str,
    empty_message: str,
) -> None:
    """Render one document-vault tab inside File 3."""

    _render_workspace_zone_heading(heading, caption)
    st.caption(f"Folder: {directory}")
    if files:
        st.dataframe(
            pd.DataFrame(_build_workspace_file_rows(files)),
            hide_index=True,
            width="stretch",
        )
        selected_path = st.selectbox(
            "Select Filed Document",
            options=files,
            format_func=lambda path: path.name,
            key=selection_key,
        )
        action_columns = st.columns(3)
        with action_columns[0]:
            if st.button(open_folder_label, key=f"{selection_key}-open-folder", width="stretch"):
                _open_workspace_path(directory)
        with action_columns[1]:
            if st.button("📂 Open Selected File", key=f"{selection_key}-open-file", width="stretch"):
                _open_workspace_path(selected_path)
        with action_columns[2]:
            st.download_button(
                "📥 Download Selected File",
                data=selected_path.read_bytes(),
                file_name=selected_path.name,
                mime=_guess_download_mime_type(selected_path),
                key=f"{selection_key}-download-file",
                width="stretch",
            )
    else:
        st.info(empty_message)
        if st.button(open_folder_label, key=f"{selection_key}-open-folder-empty", width="stretch"):
            _open_workspace_path(directory)


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
        width="stretch",
        hide_index=True,
    )


def _render_file_2_site_checks_panel(
    repository: DocumentRepository,
    *,
    site_name: str,
    latest_site_check: Optional[WeeklySiteCheck],
) -> None:
    """Render the File 2 weekly template grid and latest result."""

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
    summary_columns = st.columns(3)
    with summary_columns[0]:
        _render_inline_metric(
            "Latest Status",
            _weekly_site_check_dashboard_status(latest_site_check),
            icon="✅",
        )
    with summary_columns[1]:
        _render_inline_metric(
            "Week Commencing",
            selected_week_commencing.strftime("%d/%m/%Y"),
            icon="🗓️",
        )
    with summary_columns[2]:
        _render_inline_metric(
            "Saved This Week",
            "Yes" if weekly_site_check is not None else "No",
            icon="💾",
        )

    _render_workspace_hero(
        icon="✅",
        kicker="Daily Checks",
        title="Daily / Weekly Site Checks",
        caption="Run the live UHSF19.1 check sheet, save the active day, and generate the printable record when the week is ready.",
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

    namespace = _weekly_site_check_namespace(site_name, selected_week_commencing)
    _ensure_weekly_site_check_editor_state(
        namespace=namespace,
        week_commencing=selected_week_commencing,
        weekly_site_check=weekly_site_check,
        row_definitions=row_definitions,
    )
    checked_by_key = _weekly_site_check_state_key(namespace, kind="checked-by")
    active_day_key_key = _weekly_site_check_state_key(namespace, kind="active-day")
    checklist_mode_key = _weekly_site_check_state_key(namespace, kind="mode")
    pending_active_day_key = _weekly_site_check_state_key(
        namespace,
        kind="pending-active-day",
    )
    st.session_state.setdefault(checklist_mode_key, "daily")
    queued_active_day_key = st.session_state.pop(pending_active_day_key, None)
    if queued_active_day_key in SITE_CHECK_WEEKDAY_KEYS:
        st.session_state[active_day_key_key] = queued_active_day_key

    st.caption(f"Template output folder: {FILE_2_CHECKLIST_OUTPUT_DIR}")
    st.divider()
    _render_workspace_zone_heading(
        "Primary Action",
        "Set the live week details, complete the active day checks, then save the result before printing.",
    )

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
        checklist_mode = st.radio(
            "Checklist Scope",
            options=list(WEEKLY_SITE_CHECK_MODE_LABELS),
            key=checklist_mode_key,
            horizontal=True,
            format_func=lambda mode_key: WEEKLY_SITE_CHECK_MODE_LABELS[mode_key],
        )

    active_day_key = str(
        st.session_state.get(active_day_key_key, _current_active_day_key())
    ).strip().lower()
    if checklist_mode == "daily":
        active_day_columns = st.columns([1.6, 1.3, 1.5, 4.0])
        with active_day_columns[0]:
            active_day_key = st.selectbox(
                "Active Day",
                options=list(SITE_CHECK_WEEKDAY_KEYS),
                key=active_day_key_key,
                format_func=lambda day_key: SITE_CHECK_WEEKDAY_LABELS[day_key],
            )
        with active_day_columns[1]:
            st.markdown(
                "<div class='inline-field-label'>Smart Jump</div>",
                unsafe_allow_html=True,
            )
        with active_day_columns[2]:
            st.markdown(
                "<div class='inline-field-label'>Visible Questions</div>",
                unsafe_allow_html=True,
            )
            st.caption("Daily + shared items")
        with active_day_columns[3]:
            st.caption(
                "Daily scope shows questions that must be completed for the selected day, plus shared items that also appear in the weekly review."
            )
        prefill_marker_key = _weekly_site_check_state_key(
            namespace,
            kind="prefilled-day",
        )
    else:
        st.caption(
            "Weekly scope shows only end-of-week questions and the shared items that also need a weekly confirmation."
        )

    active_day_label = SITE_CHECK_WEEKDAY_LABELS[active_day_key]
    editable_day_key = _weekly_site_check_active_column_key(
        checklist_mode=checklist_mode,
        active_day_key=active_day_key,
    )
    editable_day_label = (
        "Weekly"
        if editable_day_key == "weekly"
        else SITE_CHECK_WEEKDAY_LABELS[editable_day_key]
    )
    visible_row_definitions = _weekly_site_check_visible_row_definitions(
        row_definitions,
        checklist_mode=checklist_mode,
    )
    if checklist_mode == "daily" and st.session_state.get(prefill_marker_key) != active_day_key:
        _prefill_weekly_site_check_day_from_previous_day(
            namespace=namespace,
            row_definitions=row_definitions,
            target_day_key=active_day_key,
            valid_template_tags=valid_template_tags,
        )
        st.session_state[prefill_marker_key] = active_day_key
    recommended_day_key = _recommended_weekly_site_check_day_key(
        week_commencing=selected_week_commencing,
        weekly_site_check=weekly_site_check,
        daily_initials_map=(
            st.session_state.get(
                _weekly_site_check_signoff_cache_key(namespace, field_name="initials"),
                {},
            )
            if checklist_mode == "daily"
            else None
        ),
        daily_time_markers_map=(
            st.session_state.get(
                _weekly_site_check_signoff_cache_key(namespace, field_name="time"),
                {},
            )
            if checklist_mode == "daily"
            else None
        ),
    )

    if checklist_mode == "daily":
        day_hint = (
            "Today"
            if selected_week_commencing == _current_week_commencing()
            else "Recommended"
        )
        st.caption(
            f"{day_hint} day: {SITE_CHECK_WEEKDAY_LABELS[recommended_day_key]}. "
            f"Active editor day: {SITE_CHECK_WEEKDAY_LABELS[active_day_key]}."
        )
        with active_day_columns[1]:
            jump_disabled = active_day_key == recommended_day_key
            if st.button(
                "↪ Jump",
                key=f"weekly-site-check-jump-{namespace}",
                width="stretch",
                disabled=jump_disabled,
            ):
                st.session_state[pending_active_day_key] = recommended_day_key
                st.rerun()

    st.caption(
        f"Bulk update: stamp the whole {editable_day_label} column before fine-tuning individual checklist rows."
    )
    bulk_columns = st.columns(3)
    bulk_actions = [
        (
            bulk_columns[0],
            f"{editable_day_label} all ✔",
            True,
            f"weekly-site-check-bulk-{namespace}-{editable_day_key}-tick",
        ),
        (
            bulk_columns[1],
            f"{editable_day_label} all ✘",
            False,
            f"weekly-site-check-bulk-{namespace}-{editable_day_key}-cross",
        ),
        (
            bulk_columns[2],
            f"Clear {editable_day_label}",
            None,
            f"weekly-site-check-bulk-{namespace}-{editable_day_key}-clear",
        ),
    ]
    for column, label, value, key in bulk_actions:
        with column:
            if st.button(label, key=key, width="stretch"):
                _set_weekly_site_check_column_value(
                    namespace=namespace,
                    row_definitions=visible_row_definitions,
                    day_key=editable_day_key,
                    value=value,
                    valid_template_tags=valid_template_tags,
                )
                st.rerun()

    st.caption(
        f"Showing {len(visible_row_definitions)} of {len(row_definitions)} UHSF19.1 questions for {WEEKLY_SITE_CHECK_MODE_LABELS[checklist_mode]}."
    )
    header_columns = st.columns([2.0, 6.1, 1.5, 1.1])
    header_columns[0].markdown("**Section**")
    header_columns[1].markdown("**Checklist Item**")
    header_columns[2].markdown("**Scope**")
    header_columns[3].markdown(f"**{editable_day_label}**")

    for row_definition in visible_row_definitions:
        row_columns = st.columns([2.0, 6.1, 1.5, 1.1])
        row_columns[0].markdown(
            f"<div class='weekly-grid-section'>{row_definition.section}</div>",
            unsafe_allow_html=True,
        )
        row_columns[1].markdown(
            f"<div class='weekly-grid-prompt'>{row_definition.prompt}</div>",
            unsafe_allow_html=True,
        )
        row_columns[2].markdown(
            (
                "<div class='weekly-grid-cell' "
                "style='font-weight: 700; color: var(--text-muted);'>"
                f"{html.escape(row_definition.frequency.label)}"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        template_tag = _weekly_site_check_template_tag(
            editable_day_key,
            row_definition.row_number,
        )
        cell_state_key = _weekly_site_check_state_key(
            namespace,
            kind="cell",
            row_number=row_definition.row_number,
            day_key=editable_day_key,
        )
        current_value = st.session_state.get(cell_state_key)
        if template_tag not in valid_template_tags:
            row_columns[3].markdown(
                (
                    "<div class='weekly-grid-cell' "
                    "style='background: #e2e8f0; border: none;'>&nbsp;</div>"
                ),
                unsafe_allow_html=True,
            )
            continue

        clicked = row_columns[3].button(
            _weekly_site_check_status_label(current_value) or "·",
            key=(
                "weekly-site-check-button-"
                f"{namespace}-{row_definition.row_number}-{editable_day_key}"
            ),
            width="stretch",
            help="Click to cycle blank, tick, and cross.",
        )
        if clicked:
            st.session_state[cell_state_key] = _cycle_weekly_site_check_value(
                current_value
            )
            st.rerun()

    if checklist_mode == "daily":
        st.markdown(
            "<div class='file-2-section-heading'>Daily Sign-Off</div>",
            unsafe_allow_html=True,
        )
        initials_cache_key = _weekly_site_check_signoff_cache_key(
            namespace,
            field_name="initials",
        )
        time_cache_key = _weekly_site_check_signoff_cache_key(
            namespace,
            field_name="time",
        )
        initials_widget_key = _weekly_site_check_signoff_widget_key(
            namespace,
            field_name="initials",
        )
        time_widget_key = _weekly_site_check_signoff_widget_key(
            namespace,
            field_name="time",
        )
        active_signoff_day_key = _weekly_site_check_state_key(
            namespace,
            kind="active-signoff-day",
        )
        daily_initials_cache = {
            day_key: str(value).strip().upper()
            for day_key, value in dict(
                st.session_state.get(
                    initials_cache_key,
                    (
                        dict(weekly_site_check.daily_initials)
                        if weekly_site_check is not None
                        else {}
                    ),
                )
            ).items()
        }
        daily_time_markers_cache = {
            day_key: str(value).strip()
            for day_key, value in dict(
                st.session_state.get(
                    time_cache_key,
                    (
                        dict(weekly_site_check.daily_time_markers)
                        if weekly_site_check is not None
                        else {}
                    ),
                )
            ).items()
        }
        for day_key in SITE_CHECK_WEEKDAY_KEYS:
            daily_initials_cache.setdefault(day_key, "")
            daily_time_markers_cache.setdefault(day_key, "")

        if st.session_state.get(active_signoff_day_key) != active_day_key:
            st.session_state[active_signoff_day_key] = active_day_key
            st.session_state[initials_widget_key] = daily_initials_cache.get(
                active_day_key,
                "",
            )
            st.session_state[time_widget_key] = daily_time_markers_cache.get(
                active_day_key,
                "",
            )

        current_initials_value = str(
            st.session_state.get(initials_widget_key, daily_initials_cache.get(active_day_key, ""))
        ).strip().upper()
        st.session_state[initials_widget_key] = current_initials_value
        suggested_initials = _initials_from_name(checked_by)
        initials_options = [""]
        for candidate in (suggested_initials, current_initials_value):
            cleaned_candidate = str(candidate).strip()
            if cleaned_candidate and cleaned_candidate not in initials_options:
                initials_options.append(cleaned_candidate)
        signoff_columns = st.columns(2)
        with signoff_columns[0]:
            st.selectbox(
                f"Initials ({SITE_CHECK_WEEKDAY_LABELS[active_day_key]})",
                options=initials_options,
                key=initials_widget_key,
            )
        with signoff_columns[1]:
            st.selectbox(
                f"AM/PM ({SITE_CHECK_WEEKDAY_LABELS[active_day_key]})",
                options=["", "AM", "PM"],
                key=time_widget_key,
            )
        daily_initials_cache[active_day_key] = str(
            st.session_state.get(initials_widget_key, "")
        ).strip().upper()
        daily_time_markers_cache[active_day_key] = str(
            st.session_state.get(time_widget_key, "")
        ).strip()
        st.session_state[initials_cache_key] = daily_initials_cache
        st.session_state[time_cache_key] = daily_time_markers_cache
        st.caption(
            "Signed days stay on the sheet for the whole week. Use this summary to check what is already stored before moving to the next day."
        )
        st.dataframe(
            _weekly_site_check_signoff_snapshot(
                daily_initials_map=daily_initials_cache,
                daily_time_markers_map=daily_time_markers_cache,
                active_day_key=active_day_key,
            ),
            width="stretch",
            hide_index=True,
        )
    else:
        st.info(
            "Weekly scope does not require the daily initials/time sign-off fields."
        )

    save_submitted = st.button(
        "✅ Submit Check",
        key=f"weekly-site-check-save-{namespace}",
        width="stretch",
    )

    st.divider()
    _render_workspace_zone_heading(
        "Live Register / History",
        "This shows the most recent saved File 2 checklist record for the active site.",
    )
    _render_file_list_panel(
        heading="Morning Station",
        title="Latest Site Check Sheet",
        caption="The most recent tick sheet saved to SQLite for File 2.",
        items=_weekly_site_check_items(latest_site_check),
        empty_message="No daily/weekly site checks have been submitted yet.",
    )

    st.divider()
    _render_workspace_zone_heading(
        "Export / Print",
        "Generate the official printable checklist once the live week has been saved.",
    )
    generate_submitted = st.button(
        "🖨️ Generate Printable Checklist",
        key=f"weekly-site-check-generate-{namespace}",
        width="stretch",
    )

    if save_submitted or generate_submitted:
        row_lookup = {
            row_state.row_number: row_state
            for row_state in (weekly_site_check.row_states if weekly_site_check else [])
        }
        signoff_initials_cache = {
            day_key: str(value).strip().upper()
            for day_key, value in dict(
                st.session_state.get(
                    _weekly_site_check_signoff_cache_key(
                        namespace,
                        field_name="initials",
                    ),
                    {},
                )
            ).items()
        }
        signoff_time_cache = {
            day_key: str(value).strip()
            for day_key, value in dict(
                st.session_state.get(
                    _weekly_site_check_signoff_cache_key(
                        namespace,
                        field_name="time",
                    ),
                    {},
                )
            ).items()
        }
        grid_values = {
            row_definition.row_number: {
                day_key: (
                    st.session_state.get(
                        _weekly_site_check_state_key(
                            namespace,
                            kind="cell",
                            row_number=row_definition.row_number,
                            day_key=day_key,
                        )
                    )
                    if day_key == editable_day_key
                    else (
                        row_lookup[row_definition.row_number].get_value(day_key)
                        if row_definition.row_number in row_lookup
                        else None
                    )
                )
                for day_key in list(SITE_CHECK_WEEKDAY_KEYS) + ["weekly"]
            }
            for row_definition in row_definitions
        }
        existing_initials_map = (
            dict(weekly_site_check.daily_initials) if weekly_site_check is not None else {}
        )
        existing_time_markers_map = (
            dict(weekly_site_check.daily_time_markers)
            if weekly_site_check is not None
            else {}
        )
        daily_initials_map = {
            day_key: (
                str(signoff_initials_cache.get(day_key, "")).strip().upper()
                if checklist_mode == "daily"
                else str(existing_initials_map.get(day_key, "")).strip().upper()
            )
            for day_key in SITE_CHECK_WEEKDAY_KEYS
        }
        daily_time_markers_map = {
            day_key: (
                str(signoff_time_cache.get(day_key, "")).strip()
                if checklist_mode == "daily"
                else str(existing_time_markers_map.get(day_key, "")).strip()
            )
            for day_key in SITE_CHECK_WEEKDAY_KEYS
        }
        saved_check = _save_weekly_site_check(
            repository,
            site_name=site_name,
            week_commencing=week_commencing,
            checked_by=checked_by,
            active_day_key=active_day_key,
            checklist_mode=checklist_mode,
            grid_values=grid_values,
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
                f"{WEEKLY_SITE_CHECK_MODE_LABELS[checklist_mode]} saved."
                if saved_check.overall_safe_to_start
                else (
                    f"{WEEKLY_SITE_CHECK_MODE_LABELS[checklist_mode]} saved, "
                    "but one or more checklist items are marked ✘ or left blank."
                )
            ),
        }
        if checklist_mode == "daily":
            next_day_key = _recommended_weekly_site_check_day_key(
                week_commencing=saved_check.week_commencing,
                weekly_site_check=saved_check,
                daily_initials_map=daily_initials_map,
                daily_time_markers_map=daily_time_markers_map,
            )
            st.session_state[pending_active_day_key] = next_day_key
        st.rerun()


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

    return "On Hire" if asset.status != DocumentStatus.ARCHIVED else asset.status.label


def _plant_asset_requires_inspection_reference(asset: PlantAssetDocument) -> bool:
    """Return True when the live record still needs an inspection/cert reference logged."""

    return is_pending_plant_inspection_reference(asset.inspection)


def _plant_asset_inspection_alert_label(asset: PlantAssetDocument) -> str:
    """Return the inspection attention label for one plant asset."""

    if _plant_asset_requires_inspection_reference(asset):
        return "Ref Needed"
    due_date = asset.inspection_due_date()
    if due_date is None:
        return "Logged"
    return "CRITICAL" if asset.inspection_requires_attention() else "OK"


def _plant_asset_inspection_display_value(asset: PlantAssetDocument) -> str:
    """Return the inspection cell label shown in the live File 2 register."""

    return format_plant_inspection_reference(
        asset.inspection_type,
        asset.inspection,
    )


def _plant_asset_inspection_type_label(asset: PlantAssetDocument) -> str:
    """Return the human-readable evidence type for one plant asset."""

    return asset.inspection_type.label


def _plant_asset_inspection_input_value(asset: PlantAssetDocument) -> str:
    """Return the editable inspection reference value for the update form."""

    if is_pending_plant_inspection_reference(asset.inspection):
        return ""
    return asset.inspection


def _plant_asset_serial_display_value(asset: PlantAssetDocument) -> str:
    """Return the best available plant identifier for the serial column."""

    return asset.serial or asset.stock_code or "Pending"


def _render_file_2_plant_register_panel(
    repository: DocumentRepository,
    *,
    project_setup: ProjectSetup,
) -> None:
    """Render the live File 2 plant register panel and print action."""

    _render_workspace_hero(
        icon="🏗️",
        kicker="Plant Register",
        title="Site Plant Register",
        caption="Keep live hire assets current, review stock references and inspection evidence, and print the File 2 plant register when needed.",
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
    inspection_reference_outstanding_count = sum(
        1 for asset in plant_assets if _plant_asset_requires_inspection_reference(asset)
    )
    inspection_attention_count = sum(
        1 for asset in plant_assets if asset.inspection_requires_attention()
    )

    summary_columns = st.columns(3)
    with summary_columns[0]:
        _render_inline_metric("Assets On Register", str(len(plant_assets)), icon="🏗️")
    with summary_columns[1]:
        _render_inline_metric(
            "Inspection Refs Outstanding",
            str(inspection_reference_outstanding_count),
            icon="🧾",
        )
    with summary_columns[2]:
        _render_inline_metric(
            "Inspection Attention",
            str(inspection_attention_count),
            icon="⚠️",
        )

    if not plant_assets:
        st.info("SYNC WORKSPACE to file HSS/MEP plant hire paperwork into File 2.")
        return

    st.divider()
    _render_workspace_zone_heading(
        "Primary Action",
        "Review stock references and log inspection or certificate details here so the live hire register stays site-ready.",
    )
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
                "Serial Number / Stock Ref",
                value=selected_asset.serial,
            )
            inspection_type_value = st.selectbox(
                "Inspection Evidence Type",
                options=list(PlantInspectionType),
                index=list(PlantInspectionType).index(selected_asset.inspection_type),
                format_func=lambda item: item.label,
            )
            inspection_value = st.text_input(
                "Inspection / Cert Reference",
                value=_plant_asset_inspection_input_value(selected_asset),
                placeholder="Report / cert / service sheet / next due / asset tag",
            )
            status_value = st.selectbox(
                "Hire Record State",
                options=[DocumentStatus.ACTIVE, DocumentStatus.ARCHIVED],
                index=[DocumentStatus.ACTIVE, DocumentStatus.ARCHIVED].index(
                    selected_asset.status
                    if selected_asset.status in {DocumentStatus.ACTIVE, DocumentStatus.ARCHIVED}
                    else DocumentStatus.ACTIVE
                ),
                format_func=lambda item: (
                    "Live On-Hire Asset"
                    if item == DocumentStatus.ACTIVE
                    else "Archive / Off Hire"
                ),
            )
            submitted = st.form_submit_button(
                "💾 Save Plant Asset",
                width="stretch",
            )

        if submitted:
            repository.save(
                PlantAssetDocument(
                    doc_id=selected_asset.doc_id,
                    site_name=selected_asset.site_name,
                    created_at=selected_asset.created_at,
                    status=status_value,
                    hire_num=selected_asset.hire_num,
                    description=selected_asset.description,
                    company=selected_asset.company,
                    phone=selected_asset.phone,
                    on_hire=selected_asset.on_hire,
                    hired_by=selected_asset.hired_by,
                    serial=serial_value,
                    stock_code=selected_asset.stock_code,
                    inspection_type=inspection_type_value,
                    inspection=inspection_value.strip() or PLANT_PENDING_INSPECTION_TEXT,
                    source_reference=selected_asset.source_reference,
                    purchase_order=selected_asset.purchase_order,
                )
            )
            st.session_state["plant_register_flash"] = {
                "level": "success",
                "message": f"Updated {selected_asset.hire_num}.",
            }
            st.rerun()

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
                "Serial / Stock Ref": _plant_asset_serial_display_value(plant_asset),
                "Evidence Type": _plant_asset_inspection_type_label(plant_asset),
                "Inspection / Cert": _plant_asset_inspection_display_value(plant_asset),
                "Inspection Ref Status": _plant_asset_inspection_alert_label(plant_asset),
                "Hire Status": _plant_asset_status_label(plant_asset),
            }
        )

    dataframe = pd.DataFrame(register_rows)

    def _plant_register_row_styles(row: pd.Series) -> List[str]:
        styles = [""] * len(row)
        if row["Inspection Ref Status"] == "CRITICAL":
            inspection_index = row.index.get_loc("Inspection / Cert")
            styles[inspection_index] = (
                "background-color: #FEE2E2; color: #991B1B; font-weight: 700;"
            )
        if row["Inspection Ref Status"] == "Ref Needed":
            inspection_index = row.index.get_loc("Inspection / Cert")
            styles[inspection_index] = (
                "background-color: #FEF3C7; color: #92400E; font-weight: 600;"
            )
        return styles

    styled_dataframe = (
        dataframe.style.apply(_plant_register_row_styles, axis=1)
        .hide(axis="index")
    )
    st.divider()
    _render_workspace_zone_heading(
        "Live Register / History",
        "This is the live plant register for the active site. Inspection cells highlight when attention is needed.",
    )
    st.dataframe(styled_dataframe, width="stretch", hide_index=True)

    st.divider()
    _render_workspace_zone_heading(
        "Export / Print",
        "Generate the official plant register or open the File 2 plant folder for direct access.",
    )
    action_columns = st.columns(2)
    with action_columns[0]:
        if st.button(
            "🖨️ Print Plant Register",
            key="file_2_print_plant_register",
            width="stretch",
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
            "📂 Open Plant Folder",
            key="file_2_open_plant_folder",
            width="stretch",
        ):
            PLANT_HIRE_REGISTER_DIR.mkdir(parents=True, exist_ok=True)
            _open_workspace_path(PLANT_HIRE_REGISTER_DIR)
            st.toast("Opened Plant_Hire_Register", icon="📂")


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
        if st.button(button_label, key=f"open-{destination.name}", width="stretch"):
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
    checklist_mode: str,
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

    row_definitions = get_weekly_site_check_row_definitions()
    for row_definition in row_definitions:
        row_state = saved_check.get_row_state(row_definition.row_number)
        for day_key in list(SITE_CHECK_WEEKDAY_KEYS) + ["weekly"]:
            if (
                _weekly_site_check_template_tag(day_key, row_definition.row_number)
                not in valid_template_tags
                or not row_definition.supports_day_key(day_key)
            ):
                row_state.set_value(day_key, None)
                continue
            row_state.set_value(
                day_key,
                grid_values.get(row_definition.row_number, {}).get(day_key),
            )

    relevant_day_key = _weekly_site_check_active_column_key(
        checklist_mode=checklist_mode,
        active_day_key=active_day_key,
    )
    relevant_values = [
        saved_check.get_row_state(row_definition.row_number).get_value(relevant_day_key)
        for row_definition in row_definitions
        if row_definition.supports_day_key(relevant_day_key)
        and _weekly_site_check_template_tag(relevant_day_key, row_definition.row_number)
        in valid_template_tags
    ]
    saved_check.overall_safe_to_start = bool(relevant_values) and all(
        value is True for value in relevant_values
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
    row_definitions = get_weekly_site_check_row_definitions()
    active_day_values = [
        weekly_site_check.get_row_state(row_definition.row_number).get_value(
            weekly_site_check.active_day_key
        )
        for row_definition in row_definitions
        if row_definition.supports_day_key(weekly_site_check.active_day_key)
        and _weekly_site_check_template_tag(
            weekly_site_check.active_day_key,
            row_definition.row_number,
        )
        in valid_template_tags
    ]
    weekly_values = [
        weekly_site_check.get_row_state(row_definition.row_number).get_value("weekly")
        for row_definition in row_definitions
        if row_definition.supports_day_key("weekly")
        and _weekly_site_check_template_tag("weekly", row_definition.row_number)
        in valid_template_tags
    ]
    if not any(
        value is not None for value in list(active_day_values) + list(weekly_values)
    ):
        return "PENDING"
    return "OK" if weekly_site_check.overall_safe_to_start else "REVIEW"


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


def _normalize_file_3_review_value(value: str) -> str:
    """Return a punctuation-light comparison string for File 3 review heuristics."""

    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def _trim_file_3_reference_text(reference: str) -> str:
    """Remove leading code tokens from one reference for comparison."""

    cleaned_reference = _normalize_file_3_review_value(reference)
    return re.sub(r"^(?:[a-z]{1,4}\d+[a-z0-9./ -]*|ms\d+[a-z0-9./ -]*)\s+", "", cleaned_reference).strip()


def _looks_like_file_3_generic_company(company: str, *, title: str, reference: str) -> bool:
    """Return True when one File 3 company value reads like parser noise."""

    normalized_company = _normalize_file_3_review_value(company)
    normalized_title = _normalize_file_3_review_value(title)
    trimmed_reference = _trim_file_3_reference_text(reference)
    if not normalized_company:
        return True
    if company[:1] and not company[:1].isalnum():
        return True
    if any(token in company for token in ("@", "http://", "https://", ":")):
        return True
    if any(character.isdigit() for character in company) and not any(
        marker in normalized_company for marker in ("ltd", "limited", "uk")
    ):
        return True
    if len(normalized_company) <= 3 and normalized_company not in {"tde", "msk", "msuk"}:
        return True
    if normalized_company == normalized_title or (
        trimmed_reference and normalized_company == trimmed_reference
    ):
        return True
    if normalized_company and trimmed_reference and normalized_company in trimmed_reference:
        return True
    if normalized_company in {
        "site contractor",
        "limits",
        "review",
        "air",
        "manual handling",
        "electrical work",
        "installation and use of temporary electrical supplies",
        "disposal of waste materials",
        "work in confined spaces",
        "use of mobile scaffold towers",
        "installing or replacing luminaires",
        "electrical testing and commissioning",
    }:
        return True
    if company[:1].islower():
        return True
    return False


def _looks_like_file_3_generic_title(title: str, *, company: str = "", reference: str = "") -> bool:
    """Return True when one File 3 title/substance value looks like OCR junk."""

    normalized_title = _normalize_file_3_review_value(title)
    normalized_company = _normalize_file_3_review_value(company)
    trimmed_reference = _trim_file_3_reference_text(reference)
    if not normalized_title:
        return True
    if title[:1] and not title[:1].isalnum():
        return True
    if normalized_title in {
        "rams document",
        "mixture identification",
        "qualified electrician",
        "severity s",
        "title signature date date",
        "confined space training",
        "risks",
        "s",
    }:
        return True
    if normalized_company and normalized_title == normalized_company:
        return True
    if trimmed_reference and normalized_title == trimmed_reference:
        return True
    if len(normalized_title) <= 2:
        return True
    return False


def _looks_like_file_3_generic_reference(reference: str) -> bool:
    """Return True when one File 3 reference value does not look trustworthy."""

    normalized_reference = _normalize_file_3_review_value(reference)
    if not normalized_reference:
        return True
    if normalized_reference in {"to", "copy", "documents", "documents copied", "copy."}:
        return True
    return False


def _looks_like_file_3_generic_version(version: str) -> bool:
    """Return True when one File 3 version value does not look trustworthy."""

    return re.fullmatch(r"\d+(?:\.\d+)*[a-zA-Z]?", version.strip()) is None


def _build_file_3_source_lookup(
    repository: DocumentRepository,
    documents: Iterable[RAMSDocument | COSHHDocument],
) -> Dict[str, Path]:
    """Return the latest indexed source path for each File 3 live document."""

    source_lookup: Dict[str, Path] = {}
    for document in documents:
        indexed_files = [
            indexed_file
            for indexed_file in repository.list_indexed_files(related_doc_id=document.doc_id)
            if indexed_file.file_path.exists()
        ]
        if indexed_files:
            source_lookup[document.doc_id] = indexed_files[0].file_path
    return source_lookup


def _build_file_3_review_candidates(
    repository: DocumentRepository,
    *,
    rams_documents: List[RAMSDocument],
    coshh_documents: List[COSHHDocument],
    include_clean: bool = False,
) -> List[File3ReviewCandidate]:
    """Return File 3 live documents that probably need a manual review."""

    source_lookup = _build_file_3_source_lookup(
        repository,
        [*rams_documents, *coshh_documents],
    )
    candidates: List[File3ReviewCandidate] = []

    for document in rams_documents:
        findings: List[str] = []
        if _looks_like_file_3_generic_company(
            document.contractor_name,
            title=document.activity_description,
            reference=document.reference,
        ):
            findings.append("Company")
        if _looks_like_file_3_generic_title(
            document.activity_description,
            company=document.contractor_name,
            reference=document.reference,
        ):
            findings.append("Activity")
        if _looks_like_file_3_generic_reference(document.reference):
            findings.append("Reference")
        if _looks_like_file_3_generic_version(document.version):
            findings.append("Version")
        if findings or include_clean:
            candidates.append(
                File3ReviewCandidate(
                    document_type="RAMS",
                    doc_id=document.doc_id,
                    company=document.contractor_name,
                    title=document.activity_description,
                    reference=document.reference,
                    version=document.version,
                    findings=tuple(findings),
                    source_path=source_lookup.get(document.doc_id),
                )
            )

    for document in coshh_documents:
        findings = []
        if _looks_like_file_3_generic_company(
            document.contractor_name,
            title=document.substance_name,
            reference=document.reference,
        ):
            findings.append("Company")
        if _looks_like_file_3_generic_title(
            document.substance_name,
            company=document.contractor_name,
            reference=document.reference,
        ):
            findings.append("Substance")
        if _looks_like_file_3_generic_title(
            document.manufacturer,
            company=document.contractor_name,
            reference=document.reference,
        ):
            findings.append("Supplier")
        if _looks_like_file_3_generic_reference(document.reference):
            findings.append("Reference")
        if _looks_like_file_3_generic_version(document.version):
            findings.append("Version")
        if findings or include_clean:
            candidates.append(
                File3ReviewCandidate(
                    document_type="COSHH",
                    doc_id=document.doc_id,
                    company=document.contractor_name,
                    title=document.substance_name,
                    reference=document.reference,
                    version=document.version,
                    findings=tuple(findings),
                    source_path=source_lookup.get(document.doc_id),
                )
            )

    return sorted(
        candidates,
        key=lambda candidate: (
            candidate.document_type,
            len(candidate.findings),
            candidate.company.casefold(),
            candidate.title.casefold(),
        ),
        reverse=True,
    )


def _filter_file_3_review_candidates(
    candidates: List[File3ReviewCandidate],
    *,
    document_type_filter: str = "All",
    finding_filter: str = "Any finding",
    search_query: str = "",
) -> List[File3ReviewCandidate]:
    """Return the File 3 review queue after applying UI filters."""

    filtered_candidates = candidates
    if document_type_filter in {"RAMS", "COSHH"}:
        filtered_candidates = [
            candidate
            for candidate in filtered_candidates
            if candidate.document_type == document_type_filter
        ]

    finding_aliases = {
        "Title / Substance": {"Activity", "Substance"},
    }
    if finding_filter != "Any finding":
        accepted_findings = finding_aliases.get(finding_filter, {finding_filter})
        filtered_candidates = [
            candidate
            for candidate in filtered_candidates
            if any(finding in accepted_findings for finding in candidate.findings)
        ]

    normalized_query = " ".join(search_query.casefold().split())
    if normalized_query:
        query_parts = normalized_query.split()
        filtered_candidates = [
            candidate
            for candidate in filtered_candidates
            if all(
                part in " ".join(
                    [
                        candidate.document_type,
                        candidate.company,
                        candidate.title,
                        candidate.reference,
                        candidate.version,
                        " ".join(candidate.findings),
                        candidate.source_path.name if candidate.source_path is not None else "",
                    ]
                ).casefold()
                for part in query_parts
            )
        ]

    return filtered_candidates


def _get_file_3_review_adjacent_doc_ids(
    candidates: List[File3ReviewCandidate],
    current_doc_id: str,
) -> tuple[Optional[str], Optional[str]]:
    """Return the previous and next doc ids for the current File 3 review row."""

    doc_ids = [candidate.doc_id for candidate in candidates]
    if current_doc_id not in doc_ids:
        return None, None
    current_index = doc_ids.index(current_doc_id)
    previous_doc_id = doc_ids[current_index - 1] if current_index > 0 else None
    next_doc_id = doc_ids[current_index + 1] if current_index < len(doc_ids) - 1 else None
    return previous_doc_id, next_doc_id


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


def _get_latest_daily_attendance_entry_for_induction(
    repository: DocumentRepository,
    induction_document: InductionDocument,
    *,
    site_name: Optional[str] = None,
) -> Optional[DailyAttendanceEntryDocument]:
    """Return the latest daily attendance entry for one inducted operative."""

    attendance_entries = [
        document
        for document in repository.list_documents(
            document_type=DailyAttendanceEntryDocument.document_type,
            site_name=site_name,
        )
        if isinstance(document, DailyAttendanceEntryDocument)
    ]
    matching_entries = [
        attendance_entry
        for attendance_entry in attendance_entries
        if (
            induction_document.doc_id
            and attendance_entry.linked_induction_doc_id == induction_document.doc_id
        )
        or (
            attendance_entry.individual_name.casefold()
            == induction_document.individual_name.casefold()
            and attendance_entry.contractor_name.casefold()
            == induction_document.contractor_name.casefold()
        )
    ]
    if not matching_entries:
        return None
    return max(
        matching_entries,
        key=lambda attendance_entry: (
            attendance_entry.time_in,
            attendance_entry.created_at,
        ),
    )


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
    latest_attendance_records_by_worker_name: Dict[str, SiteAttendanceRecord] = {}
    for attendance_record in latest_attendance_records.values():
        worker_name_key = attendance_record.workerName.casefold()
        existing_record = latest_attendance_records_by_worker_name.get(worker_name_key)
        if existing_record is None or (
            attendance_record.date,
            attendance_record.timeOut,
            attendance_record.timeIn,
        ) > (
            existing_record.date,
            existing_record.timeOut,
            existing_record.timeIn,
        ):
            latest_attendance_records_by_worker_name[worker_name_key] = attendance_record
    worker_options: Dict[str, tuple[SiteWorker, SiteAttendanceRecord]] = {}
    for worker in roster:
        attendance_record = latest_attendance_records.get(
            (worker.company.casefold(), worker.worker_name.casefold())
        )
        if attendance_record is None:
            attendance_record = latest_attendance_records_by_worker_name.get(
                worker.worker_name.casefold()
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


def _build_file_4_company_options(
    repository: DocumentRepository,
    *,
    site_name: str,
    worker_name: str,
    default_company: str,
) -> List[str]:
    """Return selectable company options for one permit operative."""

    company_names_by_key: Dict[str, str] = {}

    def _remember_company(raw_company_name: str) -> None:
        cleaned_company_name = str(raw_company_name or "").strip()
        if not cleaned_company_name:
            return
        company_names_by_key.setdefault(
            cleaned_company_name.casefold(),
            cleaned_company_name,
        )

    _remember_company(default_company)

    for worker in build_site_worker_roster(site_name=site_name):
        if worker.worker_name.casefold() == worker_name.casefold():
            _remember_company(worker.company)

    for attendance_register in repository.list_documents(
        document_type=SiteAttendanceRegister.document_type,
        site_name=site_name,
    ):
        if not isinstance(attendance_register, SiteAttendanceRegister):
            continue
        for attendance_record in attendance_register.attendance_records:
            if attendance_record.workerName.casefold() == worker_name.casefold():
                _remember_company(attendance_record.company)

    for induction_document in repository.list_documents(
        document_type=InductionDocument.document_type,
        site_name=site_name,
    ):
        if (
            isinstance(induction_document, InductionDocument)
            and induction_document.individual_name.casefold() == worker_name.casefold()
        ):
            _remember_company(induction_document.contractor_name)

    for attendance_entry in repository.list_documents(
        document_type=DailyAttendanceEntryDocument.document_type,
        site_name=site_name,
    ):
        if (
            isinstance(attendance_entry, DailyAttendanceEntryDocument)
            and attendance_entry.individual_name.casefold() == worker_name.casefold()
        ):
            _remember_company(attendance_entry.contractor_name)

    for global_company_name in _build_induction_company_options(
        repository,
        site_name=site_name,
        induction_documents=[
            document
            for document in repository.list_documents(
                document_type=InductionDocument.document_type,
                site_name=site_name,
            )
            if isinstance(document, InductionDocument)
        ],
    ):
        if global_company_name in {"-- Select Company --", "🏢 New Company (Type Below)"}:
            continue
        _remember_company(global_company_name)

    sorted_companies = sorted(company_names_by_key.values(), key=str.casefold)
    return [*sorted_companies, "🏢 Other Company (Type Below)"]


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


def _format_optional_date_label(value: Optional[date]) -> str:
    """Return one induction-friendly date string."""

    return value.strftime("%d/%m/%Y") if isinstance(value, date) else "-"


def _build_induction_role_labels(induction: InductionDocument) -> List[str]:
    """Return enabled role labels for one induction."""

    return [
        role_label
        for role_label, enabled in (
            ("First Aider", induction.first_aider),
            ("Fire Warden", induction.fire_warden),
            ("Supervisor", induction.supervisor),
            ("SMSTS", induction.smsts),
        )
        if enabled
    ]


def _resolve_induction_evidence_label(competency_path: Path) -> str:
    """Best-effort label for one saved induction evidence file."""

    normalized_name = competency_path.name.casefold().replace("_", " ").replace("-", " ")
    label_keywords = {
        "CSCS Card": ("cscs",),
        MANDATORY_MANUAL_HANDLING_LABEL: ("manual handling",),
        "Asbestos Certificate": ("asbestos",),
        "CISRS Card": ("cisrs",),
        "First Aid Certificate": ("first aid",),
        "Fire Warden Certificate": ("fire warden",),
        "Supervisor Certificate": ("supervisor",),
        "SMSTS Certificate": ("smsts",),
        "CPCS Card": ("cpcs",),
        "Client Training Evidence": ("client training",),
    }
    for label, keywords in label_keywords.items():
        if any(keyword in normalized_name for keyword in keywords):
            return label
    return competency_path.stem.replace("_", " ").replace("-", " ").strip() or competency_path.name


def _build_induction_evidence_rows(
    induction: InductionDocument,
) -> List[Dict[str, str]]:
    """Return UI rows for one induction's saved evidence pack."""

    rows: List[Dict[str, str]] = []
    label_order = {label: index for index, label in enumerate(INDUCTION_EVIDENCE_LABEL_ORDER)}
    for competency_path in _split_induction_competency_paths(induction):
        resolved_path = competency_path.expanduser()
        if not resolved_path.exists():
            continue
        evidence_label = _resolve_induction_evidence_label(resolved_path)
        rows.append(
            {
                "Evidence": evidence_label,
                "File": resolved_path.name,
                "Type": resolved_path.suffix.lstrip(".").upper() or "-",
                "Updated": datetime.fromtimestamp(resolved_path.stat().st_mtime).strftime(
                    "%d/%m/%Y %H:%M"
                ),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            label_order.get(row["Evidence"], len(label_order) + 1),
            row["Evidence"].casefold(),
            row["File"].casefold(),
        ),
    )


def _induction_has_evidence_label(
    induction: InductionDocument,
    evidence_label: str,
) -> bool:
    """Return True when one saved induction has a specific evidence label."""

    target = evidence_label.casefold()
    return any(
        row["Evidence"].casefold() == target
        for row in _build_induction_evidence_rows(induction)
    )


def _render_induction_key_value_table(
    title: str,
    rows: List[tuple[str, str]],
) -> None:
    """Render one clean key/value table for the induction detail view."""

    st.markdown(f"**{title}**")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "Field": field_label,
                    "Value": value_text.strip() if value_text.strip() else "-",
                }
                for field_label, value_text in rows
            ]
        ),
        hide_index=True,
        width="stretch",
        column_config={
            "Field": st.column_config.TextColumn("Field", width="medium"),
            "Value": st.column_config.TextColumn("Value", width="large"),
        },
    )


def _render_site_induction_edit_panel(
    repository: DocumentRepository,
    induction: InductionDocument,
) -> None:
    """Render the editable saved-induction form inside the detail workspace."""

    edit_key_prefix = f"edit-induction-{induction.doc_id}"
    with st.form(f"{edit_key_prefix}-form"):
        basics_columns = st.columns(3, gap="medium")
        with basics_columns[0]:
            full_name = st.text_input(
                "Full Name",
                value=induction.individual_name,
                key=f"{edit_key_prefix}-full-name",
            )
        with basics_columns[1]:
            company = st.text_input(
                "Company",
                value=induction.contractor_name,
                key=f"{edit_key_prefix}-company",
            )
        with basics_columns[2]:
            occupation = st.text_input(
                "Occupation",
                value=induction.occupation,
                key=f"{edit_key_prefix}-occupation",
            )

        contact_columns = st.columns(2, gap="medium")
        with contact_columns[0]:
            home_address = st.text_area(
                "Home Address",
                value=induction.home_address,
                key=f"{edit_key_prefix}-home-address",
                height=100,
            )
        with contact_columns[1]:
            contact_number = st.text_input(
                "Contact Number",
                value=induction.contact_number,
                key=f"{edit_key_prefix}-contact-number",
            )
            emergency_contact = st.text_input(
                "Emergency Contact",
                value=induction.emergency_contact,
                key=f"{edit_key_prefix}-emergency-contact",
            )
            emergency_tel = st.text_input(
                "Emergency Tel",
                value=induction.emergency_tel,
                key=f"{edit_key_prefix}-emergency-tel",
            )

        medical = st.text_area(
            "Medical Information",
            value=induction.medical,
            key=f"{edit_key_prefix}-medical",
            height=90,
        )

        cscs_columns = st.columns(3, gap="medium")
        with cscs_columns[0]:
            cscs_number = st.text_input(
                "CSCS Number",
                value=induction.cscs_number,
                key=f"{edit_key_prefix}-cscs-number",
            )
        with cscs_columns[1]:
            cscs_expiry = st.date_input(
                "CSCS Expiry",
                value=induction.cscs_expiry,
                key=f"{edit_key_prefix}-cscs-expiry",
            )
        with cscs_columns[2]:
            competency_expiry_date = st.date_input(
                "Primary Competency Expiry",
                value=induction.competency_expiry_date,
                key=f"{edit_key_prefix}-competency-expiry",
            )

        asbestos_cert = st.checkbox(
            "Asbestos Awareness Certificate",
            value=induction.asbestos_cert,
            key=f"{edit_key_prefix}-asbestos-cert",
        )
        erect_scaffold = st.checkbox(
            "Are you erecting scaffold?",
            value=induction.erect_scaffold,
            key=f"{edit_key_prefix}-erect-scaffold",
        )
        if erect_scaffold:
            scaffold_columns = st.columns(2, gap="medium")
            with scaffold_columns[0]:
                cisrs_no = st.text_input(
                    "CISRS Number",
                    value=induction.cisrs_no,
                    key=f"{edit_key_prefix}-cisrs-no",
                )
            with scaffold_columns[1]:
                cisrs_expiry = st.date_input(
                    "CISRS Expiry",
                    value=induction.cisrs_expiry,
                    key=f"{edit_key_prefix}-cisrs-expiry",
                )
        else:
            cisrs_no = ""
            cisrs_expiry = None

        operate_plant = st.checkbox(
            "Are you operating plant?",
            value=induction.operate_plant,
            key=f"{edit_key_prefix}-operate-plant",
        )
        if operate_plant:
            plant_columns = st.columns(2, gap="medium")
            with plant_columns[0]:
                cpcs_no = st.text_input(
                    "CPCS Number",
                    value=induction.cpcs_no,
                    key=f"{edit_key_prefix}-cpcs-no",
                )
            with plant_columns[1]:
                cpcs_expiry = st.date_input(
                    "CPCS Expiry",
                    value=induction.cpcs_expiry,
                    key=f"{edit_key_prefix}-cpcs-expiry",
                )
        else:
            cpcs_no = ""
            cpcs_expiry = None

        client_training_desc = st.text_area(
            "Client Training Description",
            value=induction.client_training_desc,
            key=f"{edit_key_prefix}-client-training-desc",
            height=90,
        )
        client_training_columns = st.columns(2, gap="medium")
        with client_training_columns[0]:
            client_training_date = st.date_input(
                "Client Training Date",
                value=induction.client_training_date,
                key=f"{edit_key_prefix}-client-training-date",
            )
        with client_training_columns[1]:
            client_training_expiry = st.date_input(
                "Client Training Expiry",
                value=induction.client_training_expiry,
                key=f"{edit_key_prefix}-client-training-expiry",
            )

        role_columns = st.columns(4, gap="medium")
        with role_columns[0]:
            first_aider = st.checkbox(
                "First Aider",
                value=induction.first_aider,
                key=f"{edit_key_prefix}-first-aider",
            )
        with role_columns[1]:
            fire_warden = st.checkbox(
                "Fire Warden",
                value=induction.fire_warden,
                key=f"{edit_key_prefix}-fire-warden",
            )
        with role_columns[2]:
            supervisor = st.checkbox(
                "Supervisor",
                value=induction.supervisor,
                key=f"{edit_key_prefix}-supervisor",
            )
        with role_columns[3]:
            smsts = st.checkbox(
                "SMSTS",
                value=induction.smsts,
                key=f"{edit_key_prefix}-smsts",
            )

        save_induction_edit = st.form_submit_button(
            "💾 Save Induction Changes",
            width="stretch",
        )

    if not save_induction_edit:
        return

    try:
        updated_document = update_site_induction_document(
            repository,
            induction_doc_id=induction.doc_id,
            full_name=full_name,
            home_address=home_address,
            contact_number=contact_number,
            company=company,
            occupation=occupation,
            emergency_contact=emergency_contact,
            emergency_tel=emergency_tel,
            medical=medical,
            cscs_number=cscs_number,
            cscs_expiry=cscs_expiry,
            asbestos_cert=asbestos_cert,
            erect_scaffold=erect_scaffold,
            cisrs_no=cisrs_no,
            cisrs_expiry=cisrs_expiry,
            operate_plant=operate_plant,
            cpcs_no=cpcs_no,
            cpcs_expiry=cpcs_expiry,
            client_training_desc=client_training_desc,
            client_training_date=client_training_date,
            client_training_expiry=client_training_expiry,
            first_aider=first_aider,
            fire_warden=fire_warden,
            supervisor=supervisor,
            smsts=smsts,
            competency_expiry_date=competency_expiry_date,
        )
    except ValidationError as exc:
        st.error(str(exc))
    except Exception as exc:
        st.error(f"Unable to update the saved induction: {exc}")
    else:
        st.session_state["site_induction_edit_flash"] = (
            f"Updated induction for {updated_document.induction_document.individual_name}."
        )
        st.rerun()


def _render_site_induction_extra_evidence_panel(
    repository: DocumentRepository,
    induction: InductionDocument,
) -> None:
    """Render a post-save evidence upload workflow for one saved induction."""

    form_key_prefix = f"add-evidence-{induction.doc_id}"
    with st.expander("➕ Add Extra Evidence", expanded=False):
        st.caption(
            "Use this when a certificate arrives after the induction has already been saved. New evidence is appended straight into the existing induction pack."
        )
        with st.form(f"{form_key_prefix}-form"):
            evidence_type = st.selectbox(
                "Evidence Type",
                options=[*INDUCTION_EVIDENCE_LABEL_ORDER, OTHER_INDUCTION_EVIDENCE_OPTION],
                key=f"{form_key_prefix}-type",
            )
            custom_label = ""
            if evidence_type == OTHER_INDUCTION_EVIDENCE_OPTION:
                custom_label = st.text_input(
                    "Custom Evidence Label",
                    key=f"{form_key_prefix}-custom-label",
                    placeholder="Example: Face Fit Certificate",
                )
            uploaded_files = st.file_uploader(
                "Upload Evidence Files",
                type=["png", "jpg", "jpeg", "pdf", "doc", "docx"],
                accept_multiple_files=True,
                key=f"{form_key_prefix}-files",
            )
            submit_extra_evidence = st.form_submit_button(
                "📎 Add Evidence to Saved Induction",
                width="stretch",
            )

        if not submit_extra_evidence:
            return

        resolved_label = (
            custom_label.strip()
            if evidence_type == OTHER_INDUCTION_EVIDENCE_OPTION
            else evidence_type
        )
        try:
            if not resolved_label:
                raise ValidationError("Enter a label for the extra evidence you are uploading.")
            updated_induction = add_site_induction_evidence_files(
                repository,
                induction_doc_id=induction.doc_id,
                competency_files=_build_site_induction_competency_file_payloads(
                    [(resolved_label, uploaded_files)]
                ),
            )
        except ValidationError as exc:
            st.error(str(exc))
        except Exception as exc:
            st.error(f"Unable to add extra evidence: {exc}")
        else:
            st.session_state["site_induction_edit_flash"] = (
                f"Added {resolved_label} evidence to {updated_induction.individual_name}."
            )
            st.rerun()


def _render_site_induction_recent_submissions(
    repository: DocumentRepository,
    inductions: List[InductionDocument],
) -> None:
    """Render a richer searchable workspace for saved induction records."""

    pending_delete_doc_id = st.session_state.get("site_induction_delete_pending_doc_id")
    selected_view_doc_id = st.session_state.get("site_induction_view_doc_id")
    induction_edit_flash_message = st.session_state.pop("site_induction_edit_flash", None)
    if induction_edit_flash_message:
        st.success(induction_edit_flash_message)

    _render_site_induction_bulk_reset_panel(repository, inductions)

    if not inductions:
        st.info("No inductions have been logged for this site yet.")
        return

    sorted_inductions = sorted(
        inductions,
        key=lambda induction_record: induction_record.created_at,
        reverse=True,
    )
    companies = sorted(
        {induction.contractor_name for induction in sorted_inductions if induction.contractor_name},
        key=str.casefold,
    )

    summary_columns = st.columns(4, gap="large")
    with summary_columns[0]:
        _render_inline_metric("Saved Inductions", str(len(sorted_inductions)), icon="📝")
    with summary_columns[1]:
        _render_inline_metric(
            "Added Today",
            str(sum(induction.created_at.date() == date.today() for induction in sorted_inductions)),
            icon="📅",
        )
    with summary_columns[2]:
        _render_inline_metric(
            "Evidence Packs Ready",
            str(sum(bool(_build_induction_evidence_rows(induction)) for induction in sorted_inductions)),
            icon="🎫",
        )
    with summary_columns[3]:
        _render_inline_metric(
            "Manual Handling Missing",
            str(
                sum(
                    not _induction_has_evidence_label(
                        induction,
                        MANDATORY_MANUAL_HANDLING_LABEL,
                    )
                    for induction in sorted_inductions
                )
            ),
            icon="⚠️",
        )

    _render_workspace_zone_heading(
        "Saved Induction Records",
        "Search the roster, open a richer record view, and print or correct the filed induction pack without leaving the app.",
    )
    filter_columns = st.columns([1.8, 1.1, 1.1], gap="medium")
    with filter_columns[0]:
        induction_search = st.text_input(
            "Search by operative, company, or contact number",
            key="site_induction_history_search",
            placeholder="Start typing a name, company, or mobile number",
        ).strip()
    with filter_columns[1]:
        company_filter = st.selectbox(
            "Company Filter",
            options=["All companies", *companies],
            key="site_induction_history_company_filter",
        )
    with filter_columns[2]:
        _render_inline_metric(
            "Matching Records",
            str(
                len(
                    [
                        induction
                        for induction in sorted_inductions
                        if (
                            company_filter == "All companies"
                            or induction.contractor_name == company_filter
                        )
                        and (
                            not induction_search
                            or induction_search.casefold()
                            in " ".join(
                                [
                                    induction.individual_name,
                                    induction.contractor_name,
                                    induction.contact_number,
                                    induction.occupation,
                                ]
                            ).casefold()
                        )
                    ]
                )
            ),
            icon="🔎",
        )

    filtered_inductions = [
        induction
        for induction in sorted_inductions
        if (
            company_filter == "All companies"
            or induction.contractor_name == company_filter
        )
        and (
            not induction_search
            or induction_search.casefold()
            in " ".join(
                [
                    induction.individual_name,
                    induction.contractor_name,
                    induction.contact_number,
                    induction.occupation,
                ]
            ).casefold()
        )
    ]

    if not filtered_inductions:
        st.info("No saved inductions match the current search and company filter.")
        return

    for induction in filtered_inductions:
        evidence_rows = _build_induction_evidence_rows(induction)
        role_labels = _build_induction_role_labels(induction)
        manual_handling_ready = _induction_has_evidence_label(
            induction,
            MANDATORY_MANUAL_HANDLING_LABEL,
        )
        print_pack_paths = _build_induction_print_pack_paths(induction)
        print_pack_count = len(print_pack_paths["default"]) + len(print_pack_paths["preview"])

        row_columns = st.columns([2.8, 0.95, 0.95, 0.95, 0.7, 0.55], gap="medium")
        with row_columns[0]:
            st.markdown(
                f"**{html.escape(induction.individual_name)}**  \n"
                f"{html.escape(induction.contractor_name)}"
            )
            st.caption(
                f"{induction.created_at:%d/%m/%Y %H:%M} · "
                f"{induction.occupation or 'Occupation not recorded'}"
            )
            st.caption(
                "Roles: "
                + (", ".join(role_labels) if role_labels else "No specialist roles recorded")
            )
        with row_columns[1]:
            _render_inline_metric(
                "Evidence",
                str(len(evidence_rows)),
                icon="🎫",
            )
        with row_columns[2]:
            _render_inline_metric(
                "Contact",
                "Ready" if induction.contact_number else "Missing",
                icon="📱",
            )
        with row_columns[3]:
            _render_inline_metric(
                "Manual",
                "Ready" if manual_handling_ready else "Missing",
                icon="🧰",
            )
        if row_columns[4].button(
            "View",
            key=f"view-induction-{induction.doc_id}",
            width="stretch",
        ):
            if selected_view_doc_id == induction.doc_id:
                st.session_state.pop("site_induction_view_doc_id", None)
            else:
                st.session_state["site_induction_view_doc_id"] = induction.doc_id
            st.rerun()
        if row_columns[5].button(
            "🗑️",
            key=f"delete-induction-{induction.doc_id}",
            help="Delete this induction record",
            width="stretch",
        ):
            st.session_state["site_induction_delete_pending_doc_id"] = induction.doc_id
            st.rerun()

        if selected_view_doc_id == induction.doc_id:
            st.markdown(
                (
                    "<div class='panel-card'>"
                    "<div class='panel-heading'>Saved Induction Record</div>"
                    f"<div class='panel-title'>{html.escape(induction.individual_name)}</div>"
                    "<div class='panel-caption'>"
                    f"{html.escape(induction.contractor_name)}"
                    f" | {html.escape(induction.occupation or 'Occupation not recorded')}"
                    f" | Created {induction.created_at:%d/%m/%Y %H:%M}"
                    "</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
            detail_metrics = st.columns(4, gap="large")
            with detail_metrics[0]:
                _render_inline_metric(
                    "Phone",
                    induction.contact_number or "Missing",
                    icon="📞",
                )
            with detail_metrics[1]:
                _render_inline_metric(
                    "Evidence Files",
                    str(len(evidence_rows)),
                    icon="📎",
                )
            with detail_metrics[2]:
                _render_inline_metric(
                    "Manual Handling",
                    "Ready" if manual_handling_ready else "Missing",
                    icon="🧰",
                )
            with detail_metrics[3]:
                _render_inline_metric(
                    "Print Pack",
                    str(print_pack_count),
                    icon="🖨️",
                )

            detail_tabs = st.tabs(
                ["Overview", "Competency & Roles", "Files", "Edit"]
            )
            with detail_tabs[0]:
                overview_columns = st.columns(2, gap="large")
                with overview_columns[0]:
                    _render_induction_key_value_table(
                        "Operative Details",
                        [
                            ("Full Name", induction.individual_name),
                            ("Company", induction.contractor_name),
                            ("Occupation", induction.occupation or "-"),
                            ("Home Address", induction.home_address or "-"),
                            ("Contact Number", induction.contact_number or "-"),
                        ],
                    )
                with overview_columns[1]:
                    _render_induction_key_value_table(
                        "Emergency & Welfare",
                        [
                            ("Emergency Contact", induction.emergency_contact or "-"),
                            ("Emergency Tel", induction.emergency_tel or "-"),
                            ("Medical", induction.medical or "-"),
                            ("Signature File", Path(induction.signature_image_path).name if induction.signature_image_path else "-"),
                            ("Saved Document", Path(induction.completed_document_path).name if induction.completed_document_path else "-"),
                        ],
                    )
            with detail_tabs[1]:
                competence_columns = st.columns(2, gap="large")
                with competence_columns[0]:
                    _render_induction_key_value_table(
                        "Core Competence",
                        [
                            ("CSCS No.", induction.cscs_number or "-"),
                            ("CSCS Expiry", _format_optional_date_label(induction.cscs_expiry)),
                            (
                                "Primary Competency Expiry",
                                _format_optional_date_label(induction.competency_expiry_date),
                            ),
                            (
                                "Asbestos Awareness",
                                "Yes" if induction.asbestos_cert else "No",
                            ),
                            (
                                MANDATORY_MANUAL_HANDLING_LABEL,
                                "Evidence held" if manual_handling_ready else "Missing",
                            ),
                        ],
                    )
                with competence_columns[1]:
                    _render_induction_key_value_table(
                        "Activities, Roles & Training",
                        [
                            ("Erecting Scaffold", "Yes" if induction.erect_scaffold else "No"),
                            ("CISRS No.", induction.cisrs_no or "-"),
                            ("CISRS Expiry", _format_optional_date_label(induction.cisrs_expiry)),
                            ("Operating Plant", "Yes" if induction.operate_plant else "No"),
                            ("CPCS No.", induction.cpcs_no or "-"),
                            ("CPCS Expiry", _format_optional_date_label(induction.cpcs_expiry)),
                            (
                                "Client Training",
                                induction.client_training_desc or "No client-specific training recorded",
                            ),
                            (
                                "Client Training Expiry",
                                _format_optional_date_label(induction.client_training_expiry),
                            ),
                            (
                                "Role Flags",
                                ", ".join(role_labels) if role_labels else "No specialist roles recorded",
                            ),
                        ],
                    )
                st.markdown("**Saved Evidence Pack**")
                if evidence_rows:
                    st.dataframe(
                        pd.DataFrame(evidence_rows),
                        hide_index=True,
                        width="stretch",
                    )
                else:
                    st.info("No competency evidence has been uploaded for this induction yet.")
            with detail_tabs[2]:
                file_columns = st.columns([1.05, 1.15], gap="large")
                with file_columns[0]:
                    completed_document_path = Path(induction.completed_document_path)
                    if st.button(
                        "🖨️ Print Induction Pack",
                        key=f"print-induction-pack-{induction.doc_id}",
                        width="stretch",
                        disabled=print_pack_count == 0,
                    ):
                        opened_count = _open_induction_print_pack(induction)
                        if opened_count:
                            st.success(
                                f"Opened {opened_count} induction pack file(s) for printing."
                            )
                        else:
                            st.warning(
                                "No induction form or competency certificates were available to print."
                            )
                    st.caption(
                        f"Pack contents: {print_pack_count} file(s) including the induction form and uploaded competency certificates."
                    )
                    if induction.completed_document_path and completed_document_path.exists():
                        st.download_button(
                            "📥 Download Induction DOCX",
                            data=completed_document_path.read_bytes(),
                            file_name=completed_document_path.name,
                            mime=_guess_download_mime_type(completed_document_path),
                            key=f"download-induction-docx-{induction.doc_id}",
                            width="stretch",
                        )
                    signature_path = Path(induction.signature_image_path)
                    if induction.signature_image_path and signature_path.exists():
                        st.download_button(
                            "📥 Download Signature",
                            data=signature_path.read_bytes(),
                            file_name=signature_path.name,
                            mime=_guess_download_mime_type(signature_path),
                            key=f"download-induction-signature-{induction.doc_id}",
                            width="stretch",
                        )
                    if evidence_rows:
                        st.markdown("**Evidence Downloads**")
                        for competency_path in _split_induction_competency_paths(induction):
                            resolved_path = competency_path.expanduser()
                            if not resolved_path.exists():
                                continue
                            st.download_button(
                                f"📥 {resolved_path.name}",
                                data=resolved_path.read_bytes(),
                                file_name=resolved_path.name,
                                mime=_guess_download_mime_type(resolved_path),
                                key=f"download-competency-{induction.doc_id}-{resolved_path.name}",
                                width="stretch",
                            )
                    _render_site_induction_extra_evidence_panel(repository, induction)
                with file_columns[1]:
                    signature_path = Path(induction.signature_image_path)
                    if induction.signature_image_path and signature_path.exists():
                        st.image(
                            str(signature_path),
                            caption="Captured signature",
                            width="stretch",
                        )
                    preview_paths = [
                        competency_path.expanduser()
                        for competency_path in _split_induction_competency_paths(induction)
                        if competency_path.expanduser().exists()
                        and competency_path.suffix.lower() in {".png", ".jpg", ".jpeg"}
                    ]
                    if preview_paths:
                        st.markdown("**Image Evidence Previews**")
                        for preview_path in preview_paths:
                            st.image(
                                str(preview_path),
                                caption=_resolve_induction_evidence_label(preview_path),
                                width="stretch",
                            )
                    elif evidence_rows:
                        st.caption("This induction evidence pack contains documents without image previews.")
                    else:
                        st.caption("No evidence files are currently available for preview.")
            with detail_tabs[3]:
                st.caption(
                    "Use this editor to correct induction details and regenerate the saved UHSF16.01 form without redoing the entire capture."
                )
                _render_site_induction_edit_panel(repository, induction)
            st.divider()

        if pending_delete_doc_id == induction.doc_id:
            st.warning(
                "Delete this induction record? This removes the saved record from the app and deletes the linked signature, evidence files, and completed induction document where possible."
            )
            confirm_columns = st.columns([1.2, 1.0, 4.0], gap="small")
            if confirm_columns[0].button(
                "Confirm Delete",
                key=f"confirm-delete-induction-{induction.doc_id}",
                width="stretch",
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
                width="stretch",
            ):
                st.session_state.pop("site_induction_delete_pending_doc_id", None)
                st.rerun()

        st.divider()


def _render_site_induction_bulk_reset_panel(
    repository: DocumentRepository,
    inductions: List[InductionDocument],
) -> None:
    """Render a dedicated top-level reset panel for saved induction records."""

    pending_bulk_delete = bool(st.session_state.get("site_induction_delete_all_pending"))

    st.markdown(
        "<div class='file-2-section-heading'>Reset Saved Inductions</div>",
        unsafe_allow_html=True,
    )
    st.warning(
        "This removes the saved induction records for the active site from the app and "
        "deletes linked DOCX, signature, and competency-card files."
    )
    if st.button(
        "🗑️ Reset Saved Inductions",
        key="reset-all-saved-inductions",
        width="stretch",
        type="secondary",
        disabled=not inductions,
    ):
        st.session_state["site_induction_delete_all_pending"] = True
        st.session_state.pop("site_induction_delete_pending_doc_id", None)
        st.rerun()

    if not inductions:
        st.caption("No saved inductions are currently stored for this site.")
    else:
        st.caption(f"{len(inductions)} saved induction(s) currently stored for this site.")

    if pending_bulk_delete:
        st.error(
            "Confirm reset: this will remove every saved induction record for this site."
        )
        bulk_confirm_columns = st.columns([1.2, 1.0, 3.8], gap="small")
        if bulk_confirm_columns[0].button(
            "Confirm Reset",
            key="confirm-clear-all-saved-inductions",
            width="stretch",
        ):
            deleted_paths = repository.delete_documents_and_files(
                induction.doc_id for induction in inductions
            )
            st.session_state.pop("site_induction_delete_all_pending", None)
            st.session_state.pop("site_induction_delete_pending_doc_id", None)
            st.session_state.pop("site_induction_view_doc_id", None)
            st.session_state["site_induction_delete_flash"] = (
                f"Cleared {len(inductions)} saved induction(s)."
                + (
                    f" Removed {len(deleted_paths)} linked file(s)."
                    if deleted_paths
                    else " No linked files were present on disk."
                )
            )
            st.rerun()
        if bulk_confirm_columns[1].button(
            "Cancel",
            key="cancel-clear-all-saved-inductions",
            width="stretch",
        ):
            st.session_state.pop("site_induction_delete_all_pending", None)
            st.rerun()

    st.divider()


def _build_live_waste_register_rows(
    waste_notes: List[WasteTransferNoteDocument],
    *,
    waste_source_conflict_lookup: Optional[Dict[str, Any]] = None,
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
            "Collection Type": _get_waste_note_collection_type_label(waste_note),
            "Waste Reg / Ticket": _format_waste_register_reference_for_ui(waste_note),
            "Description": waste_note.waste_description,
            "Tonnes": _format_waste_note_tonnage_label(waste_note),
            "QA": _get_waste_note_quality_status(
                waste_note,
                waste_source_conflict_lookup=waste_source_conflict_lookup or {},
            ),
            "Carrier Status": waste_note.verification_status.value,
        }
        for waste_note in sorted_waste_notes
    ]


def _get_waste_note_collection_type_label(
    waste_note: WasteTransferNoteDocument,
) -> str:
    """Return the best available collection type label for one saved WTN."""

    if not waste_note.source_conflict_candidates:
        return "-"

    if waste_note.canonical_source_path:
        canonical_source_path = waste_note.canonical_source_path.strip()
        for source_candidate in waste_note.source_conflict_candidates:
            if source_candidate.source_path.strip() == canonical_source_path:
                return source_candidate.collection_type or "-"

    for source_candidate in waste_note.source_conflict_candidates:
        if source_candidate.collection_type:
            return source_candidate.collection_type
    return "-"


def _format_waste_note_tonnage_label(
    waste_note: WasteTransferNoteDocument,
) -> str:
    """Return the File 1 tonnage display for one waste note."""

    if waste_note.quantity_tonnes > 0:
        return f"{waste_note.quantity_tonnes:.2f}"

    if waste_note.tonnage_review_status == "Weight not shown on supplier ticket":
        return "Not shown on ticket"
    if waste_note.tonnage_review_status == "Awaiting monthly waste report":
        return "Awaiting report"
    if waste_note.tonnage_review_status == "Resolved by manager":
        return "Manager reviewed"
    return "Needs review"


def _is_tanker_waste_note(waste_note: WasteTransferNoteDocument) -> bool:
    """Return True when the saved WTN represents a tanker run."""

    return any(
        "tanker" in source_candidate.collection_type.casefold()
        for source_candidate in waste_note.source_conflict_candidates
    )


def _get_waste_note_conflict_lookup_key(waste_note: WasteTransferNoteDocument) -> tuple[str, str]:
    """Return the key used to match a saved WTN to one source-conflict entry."""

    if _is_tanker_waste_note(waste_note):
        return (waste_note.wtn_number, waste_note.date.isoformat())
    return (waste_note.wtn_number, "")


def _build_waste_source_conflict_lookup(
    waste_notes: List[WasteTransferNoteDocument],
    waste_source_conflicts: List[Any],
) -> Dict[tuple[str, str], Any]:
    """Return a lookup that handles repeated tanker ticket numbers safely."""

    lookup: Dict[tuple[str, str], Any] = {}
    note_keys = {
        _get_waste_note_conflict_lookup_key(waste_note)
        for waste_note in waste_notes
    }
    for source_conflict in waste_source_conflicts:
        collection_type = source_conflict.canonical_source.scanned_note.collection_type
        conflict_key = (
            source_conflict.wtn_number,
            source_conflict.canonical_source.scanned_note.ticket_date.isoformat()
            if "tanker" in collection_type.casefold()
            else "",
        )
        if conflict_key in note_keys:
            lookup[conflict_key] = source_conflict
            continue
        if (source_conflict.wtn_number, "") in note_keys:
            lookup[(source_conflict.wtn_number, "")] = source_conflict
    return lookup


def _get_waste_source_conflict_for_note(
    waste_note: WasteTransferNoteDocument,
    waste_source_conflict_lookup: Dict[Any, Any],
) -> Optional[Any]:
    """Return the matching source conflict for one saved waste note."""

    return waste_source_conflict_lookup.get(
        _get_waste_note_conflict_lookup_key(waste_note)
    ) or waste_source_conflict_lookup.get(waste_note.wtn_number)


def _waste_note_requires_queue_review(
    waste_note: WasteTransferNoteDocument,
    *,
    waste_source_conflict_lookup: Dict[Any, Any],
) -> bool:
    """Return True when the WTN still needs operator action in File 1."""

    if _get_waste_source_conflict_for_note(
        waste_note,
        waste_source_conflict_lookup,
    ) is not None:
        return True
    return waste_note.quantity_tonnes <= 0 and not waste_note.tonnage_review_status


def _get_waste_note_quality_status(
    waste_note: WasteTransferNoteDocument,
    *,
    waste_source_conflict_lookup: Dict[tuple[str, str], Any],
) -> str:
    """Return the register QA label for one waste note."""

    if _get_waste_source_conflict_for_note(
        waste_note,
        waste_source_conflict_lookup,
    ) is not None:
        return "Source Conflict"
    if waste_note.quantity_tonnes <= 0:
        if waste_note.tonnage_review_status:
            return waste_note.tonnage_review_status
        return "Needs Review"
    return "Ready"


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


def _guess_download_mime_type(file_path: Path) -> str:
    """Return a sensible download MIME type for one induction attachment."""

    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return "application/pdf"
    if suffix == ".png":
        return "image/png"
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".docx":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return "application/octet-stream"


def _split_induction_competency_paths(induction: InductionDocument) -> List[Path]:
    """Return saved competency-card paths for one induction record."""

    return [
        Path(path_text)
        for path_text in induction.competency_card_paths.split(",")
        if path_text.strip()
    ]


def _build_induction_print_pack_paths(
    induction: InductionDocument,
) -> Dict[str, List[Path]]:
    """Return the grouped file paths that make up one induction print pack."""

    default_paths: List[Path] = []
    preview_paths: List[Path] = []

    completed_document_path = Path(induction.completed_document_path)
    if induction.completed_document_path and completed_document_path.exists():
        default_paths.append(completed_document_path)

    for competency_path in _split_induction_competency_paths(induction):
        if not competency_path.exists():
            continue
        if competency_path.suffix.lower() in {".pdf", ".png", ".jpg", ".jpeg"}:
            preview_paths.append(competency_path)
        else:
            default_paths.append(competency_path)

    return {"default": default_paths, "preview": preview_paths}


def _open_induction_print_pack(induction: InductionDocument) -> int:
    """Open the induction form and cert pack in print-friendly apps on macOS."""

    pack_paths = _build_induction_print_pack_paths(induction)
    opened_count = 0

    if pack_paths["default"]:
        try:
            subprocess.run(
                ["open", *[str(path) for path in pack_paths["default"]]],
                check=False,
                capture_output=True,
                text=True,
            )
            opened_count += len(pack_paths["default"])
        except OSError:
            pass

    if pack_paths["preview"]:
        try:
            subprocess.run(
                ["open", "-a", "Preview", *[str(path) for path in pack_paths["preview"]]],
                check=False,
                capture_output=True,
                text=True,
            )
            opened_count += len(pack_paths["preview"])
        except OSError:
            for preview_path in pack_paths["preview"]:
                _open_workspace_path(preview_path)
                opened_count += 1

    return opened_count


def _get_file_1_waste_note_source_path(
    repository: DocumentRepository,
    waste_note: WasteTransferNoteDocument,
    *,
    source_conflict_lookup: Optional[Dict[tuple[str, str], Any]] = None,
) -> Optional[Path]:
    """Return the physical filed PDF linked to one File 1 WTN."""

    if waste_note.canonical_source_path:
        canonical_source_path = Path(waste_note.canonical_source_path)
        if canonical_source_path.exists():
            return canonical_source_path

    if source_conflict_lookup is not None:
        source_conflict = _get_waste_source_conflict_for_note(
            waste_note,
            source_conflict_lookup,
        )
        if source_conflict is not None:
            return source_conflict.canonical_source.source_path
    else:
        source_conflicts = _get_cached_file_1_waste_source_conflicts(
            site_name=waste_note.site_name,
            repository=repository,
        )
        for source_conflict in source_conflicts:
            if source_conflict.wtn_number != waste_note.wtn_number:
                continue
            if (
                "tanker" in source_conflict.canonical_source.scanned_note.collection_type.casefold()
                and source_conflict.canonical_source.scanned_note.ticket_date != waste_note.date
            ):
                continue
            return source_conflict.canonical_source.source_path

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
