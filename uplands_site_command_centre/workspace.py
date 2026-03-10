"""Workspace file movement and indexing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import shutil
from typing import List, Optional

from uplands_site_command_centre import config
from uplands_site_command_centre.permits.ingestion_engine import IngestionEngine
from uplands_site_command_centre.permits.models import FileGroup
from uplands_site_command_centre.permits.repository import (
    DocumentRepository,
    IndexedFileRecord,
)


ABUCS_PDF_PATTERN = re.compile(r"^\d+\.pdf$", re.IGNORECASE)


@dataclass(frozen=True)
class FiledAsset:
    """A file moved from the ingest inbox into the workspace."""

    original_path: Path
    destination_path: Path
    file_category: str
    related_doc_id: Optional[str] = None


def file_and_index_all(repository: DocumentRepository) -> List[FiledAsset]:
    """Move supported inbox files into the workspace and index their new paths."""

    repository.create_schema()

    inbox = config.INBOX
    waste_destination = config.WASTE_DESTINATION
    carrier_docs_destination = config.CARRIER_DOCS_DESTINATION
    waste_reports_destination = config.WASTE_REPORTS_DESTINATION
    attendance_destination = config.ATTENDANCE_DESTINATION

    inbox.mkdir(parents=True, exist_ok=True)
    waste_destination.mkdir(parents=True, exist_ok=True)
    carrier_docs_destination.mkdir(parents=True, exist_ok=True)
    waste_reports_destination.mkdir(parents=True, exist_ok=True)
    attendance_destination.mkdir(parents=True, exist_ok=True)

    attendance_engine = IngestionEngine(repository)
    filed_assets: List[FiledAsset] = []

    for source_path in sorted(inbox.iterdir(), key=lambda path: path.name.lower()):
        if not source_path.is_file():
            continue

        if _is_carrier_compliance_pdf(source_path):
            destination_path = _move_file(source_path, carrier_docs_destination)
            repository.index_file(
                file_name=destination_path.name,
                file_path=destination_path,
                file_category="carrier_doc_pdf",
                file_group=FileGroup.FILE_1,
            )
            filed_assets.append(
                FiledAsset(
                    original_path=source_path,
                    destination_path=destination_path,
                    file_category="carrier_doc_pdf",
                )
            )
            continue

        if ABUCS_PDF_PATTERN.match(source_path.name):
            destination_path = _move_file(source_path, waste_destination)
            repository.index_file(
                file_name=destination_path.name,
                file_path=destination_path,
                file_category="abucs_pdf",
                file_group=FileGroup.FILE_1,
            )
            filed_assets.append(
                FiledAsset(
                    original_path=source_path,
                    destination_path=destination_path,
                    file_category="abucs_pdf",
                )
            )
            continue

        if source_path.suffix.lower() in {".xls", ".xlsx"}:
            destination_path = _move_file(source_path, waste_reports_destination)
            repository.index_file(
                file_name=destination_path.name,
                file_path=destination_path,
                file_category="waste_report_excel",
                file_group=FileGroup.FILE_1,
            )
            filed_assets.append(
                FiledAsset(
                    original_path=source_path,
                    destination_path=destination_path,
                    file_category="waste_report_excel",
                )
            )
            continue

        if source_path.suffix.lower() == ".json":
            destination_path = _move_file(source_path, attendance_destination)
            register = attendance_engine.ingest_site_attendance_json(destination_path)
            repository.index_file(
                file_name=destination_path.name,
                file_path=destination_path,
                file_category="kpi_json",
                file_group=FileGroup.FILE_2,
                site_name=register.site_name,
                related_doc_id=register.doc_id,
            )
            filed_assets.append(
                FiledAsset(
                    original_path=source_path,
                    destination_path=destination_path,
                    file_category="kpi_json",
                    related_doc_id=register.doc_id,
                )
            )

    return filed_assets


def _is_carrier_compliance_pdf(source_path: Path) -> bool:
    """Return True when the filename suggests a carrier licence or insurance PDF."""

    if source_path.suffix.lower() != ".pdf":
        return False

    lowered_name = source_path.name.lower()
    return "insurance" in lowered_name or "carrier" in lowered_name


def _move_file(source_path: Path, destination_directory: Path) -> Path:
    """Move a file into its destination directory without overwriting an existing file."""

    destination_path = _build_available_destination(source_path, destination_directory)
    moved_path = Path(shutil.move(str(source_path), str(destination_path)))
    return moved_path.resolve()


def _build_available_destination(source_path: Path, destination_directory: Path) -> Path:
    """Return a destination path that avoids clobbering an existing file."""

    candidate = destination_directory / source_path.name
    counter = 1
    while candidate.exists():
        candidate = destination_directory / (
            f"{source_path.stem}-{counter}{source_path.suffix}"
        )
        counter += 1
    return candidate
