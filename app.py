"""Streamlit dashboard for the Uplands Lovedean site management portal."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import streamlit as st

from uplands_site_command_centre import (
    ATTENDANCE_DESTINATION,
    BASE_DATA_DIR,
    DATABASE_PATH,
    INBOX,
    WASTE_DESTINATION,
    CarrierComplianceDocument,
    CarrierComplianceDocumentType,
    ComplianceAlertStatus,
    DocumentRepository,
    SiteAttendanceRegister,
    WasteRegister,
    WasteTransferNoteDocument,
    check_carrier_compliance,
    file_and_index_all,
)


APP_ROOT = Path(__file__).resolve().parent
UPLANDS_LOGO = APP_ROOT / "Home Uplands.png"
NATIONAL_GRID_LOGO = APP_ROOT / "Ng logo.png"

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
USER_NAME = "Ceri Edwards"
USER_ROLE = "Site Manager"
ABUCS_NAME = "Abucs"


@dataclass(frozen=True)
class AbucsStatusRow:
    """UI-ready compliance row for the Abucs card."""

    label: str
    status: ComplianceAlertStatus
    reason: str

    @property
    def indicator_colour(self) -> str:
        return SUCCESS_GREEN if self.status == ComplianceAlertStatus.OK else ALERT_RED


def main() -> None:
    """Render the Streamlit portal."""

    st.set_page_config(
        page_title=SITE_TITLE,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_styles()

    repository = _build_repository()

    with st.sidebar:
        _render_sidebar(repository)

    _render_top_bar()
    _render_hero_card()
    _render_overview_tab(repository)
    _render_workspace_tab(repository)
    _render_compliance_tab(repository)


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
            .block-container {{
                padding-top: 1.75rem;
                padding-bottom: 2rem;
                max-width: 1380px;
            }}
            section[data-testid="stSidebar"] {{
                background: {SIDEBAR_BACKGROUND};
                border-right: 1px solid #e7e9ee;
            }}
            section[data-testid="stSidebar"] * {{
                color: {TEXT_DARK};
            }}
            section[data-testid="stSidebar"] .stImage img {{
                background: #ffffff;
                border: 1px solid #eceef2;
                border-radius: 12px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.05);
                padding: 0.7rem;
            }}
            section[data-testid="stSidebar"] div.stButton > button {{
                background: #ffffff !important;
                color: {TEXT_DARK} !important;
                border: 1px solid #cfc9ea !important;
                border-left: 4px solid {UPLANDS_PINK} !important;
                border-radius: 999px !important;
                box-shadow: 0 8px 18px rgba(147, 51, 234, 0.08);
                font-weight: 700 !important;
                justify-content: flex-start !important;
                min-height: 3rem;
                padding-left: 1rem !important;
            }}
            section[data-testid="stSidebar"] div.stButton > button:hover {{
                border-color: {UPLANDS_PINK} !important;
                color: {UPLANDS_PINK} !important;
            }}
            .stProgress > div > div > div > div {{
                background-color: {UPLANDS_PINK};
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
            .top-bar-pill {{
                display: inline-flex;
                flex-direction: column;
                justify-content: center;
                background:
                    linear-gradient(#ffffff, #ffffff) padding-box,
                    linear-gradient(90deg, {UPLANDS_PINK}, {UPLANDS_BLUE}) border-box;
                border: 1px solid transparent;
                border-radius: 999px;
                color: {TEXT_DARK};
                min-height: 88px;
                padding: 0.85rem 1.25rem;
                box-shadow: 0 12px 24px rgba(147, 51, 234, 0.07);
            }}
            .top-bar-pill-kicker {{
                color: #8da0bb;
                font-size: 0.78rem;
                font-weight: 800;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                margin-bottom: 0.35rem;
            }}
            .top-bar-pill-main {{
                color: {TEXT_DARK};
                font-size: 0.98rem;
                font-weight: 800;
                line-height: 1.2;
            }}
            .top-bar-right {{
                display: flex;
                justify-content: flex-end;
            }}
            .hero-card {{
                background: #ffffff;
                border-radius: 12px;
                background:
                    linear-gradient(#ffffff, #ffffff) padding-box,
                    linear-gradient(90deg, rgba(209, 34, 142, 0.9), rgba(91, 141, 239, 0.9)) border-box;
                border: 1px solid transparent;
                box-shadow: 0 18px 28px rgba(189, 77, 154, 0.10);
                padding: 1.7rem 1.8rem 1.6rem 1.8rem;
                margin-top: 1rem;
                margin-bottom: 1.35rem;
            }}
            .hero-kicker {{
                color: {TEXT_MUTED};
                font-size: 0.78rem;
                font-weight: 800;
                letter-spacing: 0.18em;
                text-transform: uppercase;
                margin-bottom: 0.65rem;
            }}
            .hero-title {{
                color: {TEXT_DARK};
                font-size: 2.35rem;
                font-weight: 800;
                line-height: 1.05;
                letter-spacing: -0.03em;
                margin: 0;
            }}
            .hero-subtext {{
                color: {TEXT_MUTED};
                font-size: 1rem;
                margin-top: 0.7rem;
            }}
            .hero-underline {{
                width: 180px;
                height: 4px;
                border-radius: 999px;
                margin-top: 1rem;
                background: linear-gradient(90deg, {UPLANDS_PINK}, {UPLANDS_BLUE});
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
                min-height: 245px;
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
                font-size: 2.2rem;
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
                font-size: 0.94rem;
            }}
            .indicator-reason {{
                color: {TEXT_MUTED};
                font-size: 0.9rem;
                line-height: 1.4;
            }}
            .section-shell {{
                background: #ffffff;
                border-radius: 12px;
                background:
                    linear-gradient(#ffffff, #ffffff) padding-box,
                    linear-gradient(90deg, rgba(209, 34, 142, 0.75), rgba(91, 141, 239, 0.75)) border-box;
                border: 1px solid transparent;
                box-shadow: 0 14px 24px rgba(189, 77, 154, 0.08);
                padding: 1.1rem 1.15rem;
                margin-top: 1rem;
            }}
            .section-shell h3 {{
                color: {TEXT_DARK};
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


def _render_sidebar(repository: DocumentRepository) -> None:
    """Render the branded sidebar and sync controls."""

    if UPLANDS_LOGO.exists():
        st.image(str(UPLANDS_LOGO), width=220)
    if NATIONAL_GRID_LOGO.exists():
        st.image(str(NATIONAL_GRID_LOGO), width=120)

    if st.button("SYNC WORKSPACE", use_container_width=True):
        with st.spinner("Syncing Uplands workspace..."):
            filed_assets = file_and_index_all(repository)
        st.session_state["sync_summary"] = {
            "moved_count": len(filed_assets),
            "file_names": [asset.destination_path.name for asset in filed_assets],
        }
        st.rerun()

    sync_summary = st.session_state.get("sync_summary")
    if sync_summary is not None:
        st.markdown("<div class='sync-summary'>", unsafe_allow_html=True)
        st.progress(100)
        st.caption(f"{sync_summary['moved_count']} file(s) filed into the workspace.")
        if sync_summary["file_names"]:
            st.write("\n".join(f"- {file_name}" for file_name in sync_summary["file_names"]))
        else:
            st.write("- No new files detected in the ingest folder.")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='sidebar-heading'>Workspace Status</div>", unsafe_allow_html=True)
    for label in ("📁 Ingest Inbox", "📁 Waste Notes", "📁 Registers"):
        st.markdown(
            (
                "<div class='sidebar-status-row'>"
                f"<span class='sidebar-status-label'>{label}</span>"
                "<span class='sidebar-status-value'>Active</span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )


def _render_top_bar() -> None:
    """Render the top bar pills."""

    left_column, right_column = st.columns([2.1, 1.2])
    with left_column:
        st.markdown(
            (
                "<div class='top-bar-pill'>"
                "<div class='top-bar-pill-kicker'>Active Project</div>"
                "<div class='top-bar-pill-main'>🟢 NG Lovedean Substation</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
    with right_column:
        st.markdown(
            (
                "<div class='top-bar-right'>"
                "<div class='top-bar-pill'>"
                f"<div class='top-bar-pill-main'>{USER_NAME} 👤</div>"
                f"<div class='top-bar-pill-kicker'>{USER_ROLE}</div>"
                "</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )


def _render_hero_card() -> None:
    """Render the hero introduction card."""

    st.markdown(
        (
            "<div class='hero-card'>"
            "<div class='hero-kicker'>Site Command</div>"
            "<h1 class='hero-title'>NG Lovedean Substation</h1>"
            "<div class='hero-subtext'>"
            "Upload attendance sheets, validate extracted entries, and monitor compliance."
            "</div>"
            "<div class='hero-underline'></div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_overview_tab(repository: DocumentRepository) -> None:
    """Render the main operational cards."""

    waste_notes = _get_lovedean_waste_notes(repository)
    attendance_register = _get_lovedean_attendance_register(repository)
    abucs_rows = _get_abucs_status_rows(repository)

    waste_tonnage = sum(
        note.quantity_tonnes
        for note in waste_notes
        if note.date.month == date.today().month and note.date.year == date.today().year
    )
    monthly_waste_count = len(
        [
            note
            for note in waste_notes
            if note.date.month == date.today().month and note.date.year == date.today().year
        ]
    )

    total_attendance_rows = (
        len(attendance_register.attendance_records) if attendance_register else 0
    )
    total_attendance_hours = (
        sum(record.totalHours for record in attendance_register.attendance_records)
        if attendance_register
        else 0.0
    )
    unique_workers = (
        len({record.workerName for record in attendance_register.attendance_records})
        if attendance_register
        else 0
    )

    cards = st.columns(3)
    with cards[0]:
        _render_metric_card(
            title="Waste Tonnage",
            icon="♻",
            value=f"{waste_tonnage:.2f} t",
            caption=f"{monthly_waste_count} waste transfer note(s) recorded for {date.today():%B %Y}.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Tracked waste records for Lovedean: <strong>{len(waste_notes)}</strong>."
                "</div>"
            ),
        )
    with cards[1]:
        _render_metric_card(
            title="Site Attendance",
            icon="👥",
            value=str(total_attendance_rows),
            caption="Attendance rows currently indexed for the latest Lovedean register.",
            body_html=(
                "<div class='data-card-subtext'>"
                f"Unique workers: <strong>{unique_workers}</strong><br>"
                f"Total logged hours: <strong>{total_attendance_hours:.1f}</strong>"
                "</div>"
            ),
        )
    with cards[2]:
        _render_metric_card(
            title="Abucs Compliance Status",
            icon="🛡",
            value=_abucs_overall_label(abucs_rows),
            caption="Licence and liability insurance gatekeeper status for incoming Abucs waste notes.",
            body_html=_build_abucs_indicator_html(abucs_rows),
        )

    st.markdown("<div class='section-shell'>", unsafe_allow_html=True)
    st.subheader("Current Lovedean Snapshot")
    summary_columns = st.columns(3)
    summary_columns[0].metric(
        "Unverified WTNs",
        len(
            [
                note
                for note in waste_notes
                if note.verification_status.value == "UNVERIFIED"
            ]
        ),
    )
    summary_columns[1].metric(
        "Carrier Compliance Records",
        len(_get_lovedean_carrier_documents(repository)),
    )
    summary_columns[2].metric(
        "Indexed Workspace Files",
        len(repository.list_indexed_files()),
    )
    st.markdown("</div>", unsafe_allow_html=True)


def _render_workspace_tab(repository: DocumentRepository) -> None:
    """Render file index visibility for the workspace."""

    st.markdown("<div class='section-shell'>", unsafe_allow_html=True)
    st.subheader("Workspace Index")
    indexed_files = repository.list_indexed_files()
    if not indexed_files:
        st.info(
            "No files have been indexed yet. Drop files into the ingest folder and run SYNC WORKSPACE."
        )
    else:
        st.dataframe(
            [
                {
                    "File Name": record.file_name,
                    "Category": record.file_category,
                    "File Group": record.file_group.value,
                    "Site": record.site_name or "-",
                    "Related Document": record.related_doc_id or "-",
                    "Path": str(record.file_path),
                }
                for record in indexed_files
            ],
            use_container_width=True,
            hide_index=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='section-shell'>", unsafe_allow_html=True)
    st.subheader("Destination Map")
    destination_columns = st.columns(3)
    destination_columns[0].metric("Waste Note Folder", str(WASTE_DESTINATION))
    destination_columns[1].metric("Attendance Folder", str(ATTENDANCE_DESTINATION))
    destination_columns[2].metric("Inbox Folder", str(INBOX))
    st.markdown("</div>", unsafe_allow_html=True)


def _render_compliance_tab(repository: DocumentRepository) -> None:
    """Render the carrier and waste verification view."""

    st.markdown("<div class='section-shell'>", unsafe_allow_html=True)
    st.subheader("Compliance Monitor")
    findings = check_carrier_compliance(repository)
    if not findings:
        st.warning("No carrier compliance records are indexed yet.")
    else:
        st.dataframe(
            [
                {
                    "Carrier": finding.carrier_name,
                    "Document": finding.carrier_document_type.label,
                    "Status": finding.status.value,
                    "Reference": finding.reference_number or "-",
                    "Expiry Date": (
                        finding.expiry_date.isoformat() if finding.expiry_date else "-"
                    ),
                    "Days to Expiry": (
                        finding.days_to_expiry
                        if finding.days_to_expiry is not None
                        else "-"
                    ),
                    "Reason": finding.reason,
                }
                for finding in findings
            ],
            use_container_width=True,
            hide_index=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='section-shell'>", unsafe_allow_html=True)
    st.subheader("Waste Verification Queue")
    waste_notes = _get_lovedean_waste_notes(repository)
    unverified_notes = [
        note
        for note in waste_notes
        if note.verification_status.value == "UNVERIFIED"
    ]
    if not unverified_notes:
        st.success(
            "All Lovedean waste transfer notes currently pass the carrier gatekeeper."
        )
    else:
        st.dataframe(
            [
                {
                    "WTN Number": note.wtn_number,
                    "Carrier": note.carrier_name,
                    "Date": note.date.isoformat(),
                    "Verification": note.verification_status.value,
                    "Notes": note.verification_notes or "-",
                }
                for note in unverified_notes
            ],
            use_container_width=True,
            hide_index=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)


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


def _get_lovedean_waste_notes(repository: DocumentRepository) -> List[WasteTransferNoteDocument]:
    """Return Lovedean WTNs, preferring direct WTN documents over nested registers."""

    direct_waste_notes = _filter_for_lovedean(
        repository.list_documents(document_type=WasteTransferNoteDocument.document_type),
    )
    if direct_waste_notes:
        return [
            note
            for note in direct_waste_notes
            if isinstance(note, WasteTransferNoteDocument)
        ]

    waste_notes: Dict[str, WasteTransferNoteDocument] = {}
    for waste_register in _filter_for_lovedean(
        repository.list_documents(document_type=WasteRegister.document_type)
    ):
        if not isinstance(waste_register, WasteRegister):
            continue
        for waste_note in waste_register.waste_transfer_notes:
            waste_notes[waste_note.wtn_number] = waste_note
    return list(waste_notes.values())


def _get_lovedean_attendance_register(
    repository: DocumentRepository,
) -> Optional[SiteAttendanceRegister]:
    """Return the latest attendance register for Lovedean."""

    attendance_registers = [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=SiteAttendanceRegister.document_type)
        )
        if isinstance(document, SiteAttendanceRegister)
    ]
    if not attendance_registers:
        return None
    return max(attendance_registers, key=lambda register: register.created_at)


def _get_lovedean_carrier_documents(
    repository: DocumentRepository,
) -> List[CarrierComplianceDocument]:
    """Return carrier compliance records linked to Lovedean."""

    return [
        document
        for document in _filter_for_lovedean(
            repository.list_documents(document_type=CarrierComplianceDocument.document_type)
        )
        if isinstance(document, CarrierComplianceDocument)
    ]


def _filter_for_lovedean(documents: Iterable[Any]) -> List[Any]:
    """Return documents whose site name is clearly tied to Lovedean."""

    document_list = list(documents)
    filtered_documents = [
        document
        for document in document_list
        if hasattr(document, "site_name")
        and SITE_DATA_KEYWORD in str(document.site_name).casefold()
    ]
    if filtered_documents:
        return filtered_documents
    return document_list


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
                    reason="No document indexed.",
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


def _abucs_overall_label(rows: List[AbucsStatusRow]) -> str:
    """Return the headline label for the Abucs card."""

    if rows and all(row.status == ComplianceAlertStatus.OK for row in rows):
        return "COMPLIANT"
    return "ACTION REQUIRED"


def _build_abucs_indicator_html(rows: List[AbucsStatusRow]) -> str:
    """Return the green/red indicator markup for the Abucs card."""

    indicator_rows = []
    for row in rows:
        indicator_rows.append(
            (
                "<div class='indicator-row'>"
                f"<span class='indicator-dot' style='background:{row.indicator_colour};'></span>"
                "<div>"
                f"<div class='indicator-label'>{row.label}</div>"
                f"<div class='indicator-reason'>{row.reason}</div>"
                "</div>"
                "</div>"
            )
        )
    return "".join(indicator_rows)


if __name__ == "__main__":
    main()
