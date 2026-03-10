"""JSON ingestion utilities for attendance and other external data feeds."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Optional, Union

from uplands_site_command_centre.permits.models import (
    DocumentStatus,
    SiteAttendanceRecord,
    SiteAttendanceRegister,
)
from uplands_site_command_centre.permits.repository import DocumentRepository


class IngestionEngine:
    """Map external JSON exports into typed Uplands documents."""

    def __init__(self, repository: DocumentRepository) -> None:
        self.repository = repository

    def ingest_site_attendance_json(
        self,
        json_path: Union[str, Path],
        *,
        site_name: Optional[str] = None,
        register_doc_id: Optional[str] = None,
    ) -> SiteAttendanceRegister:
        """Ingest attendance rows into a File 2 site attendance register."""

        source_path = Path(json_path)
        payload = self._load_json_payload(source_path)
        resolved_site_name = self._resolve_site_name(payload, site_name=site_name)
        rows = list(self._extract_rows(payload))

        existing_registers = [
            document
            for document in self.repository.list_documents(
                document_type=SiteAttendanceRegister.document_type,
                site_name=resolved_site_name,
            )
            if isinstance(document, SiteAttendanceRegister)
        ]
        existing_keys = {
            record.duplicate_key()
            for register in existing_registers
            for record in register.attendance_records
        }

        target_doc_id = register_doc_id or self._default_register_doc_id(resolved_site_name)
        target_register = next(
            (
                register
                for register in existing_registers
                if register.doc_id == target_doc_id
            ),
            None,
        )
        if target_register is None:
            target_register = SiteAttendanceRegister(
                doc_id=target_doc_id,
                site_name=resolved_site_name,
                created_at=datetime.now(),
                status=DocumentStatus.ACTIVE,
                attendance_records=[],
            )

        for row_index, row in enumerate(rows, start=1):
            try:
                attendance_record = SiteAttendanceRecord.from_json_row(row)
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid attendance row {row_index} in {source_path.name}: {exc}"
                ) from exc

            if attendance_record.duplicate_key() in existing_keys:
                continue

            target_register.add_attendance_record(attendance_record)
            existing_keys.add(attendance_record.duplicate_key())

        self.repository.save(target_register)
        return target_register

    def _load_json_payload(self, json_path: Path) -> Any:
        """Load and return the raw JSON payload."""

        with json_path.open("r", encoding="utf-8") as file_handle:
            return json.load(file_handle)

    def _extract_rows(self, payload: Any) -> Iterable[Mapping[str, Any]]:
        """Support common JSON export wrappers around the row array."""

        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            extracted_rows = payload.get("extractedRows")
            if isinstance(extracted_rows, Mapping):
                combined_rows: List[Mapping[str, Any]] = []
                for key in ("weekly", "eom"):
                    candidate = extracted_rows.get(key)
                    if isinstance(candidate, list):
                        combined_rows.extend(candidate)
                if combined_rows or any(
                    isinstance(extracted_rows.get(key), list) for key in ("weekly", "eom")
                ):
                    return combined_rows
            for key in ("records", "rows", "data"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    return candidate
        raise ValueError(
            "Attendance JSON must be a list, a dict containing 'records', 'rows', or 'data', "
            "or a KPI export with 'extractedRows.weekly'/'extractedRows.eom'."
        )

    def _resolve_site_name(
        self,
        payload: Any,
        *,
        site_name: Optional[str],
    ) -> str:
        """Prefer an explicit site name but fall back to the KPI export settings."""

        if site_name:
            return site_name
        if isinstance(payload, Mapping):
            settings = payload.get("settings")
            if isinstance(settings, Mapping):
                resolved_site_name = settings.get("siteName")
                if isinstance(resolved_site_name, str) and resolved_site_name.strip():
                    return resolved_site_name.strip()
        raise ValueError(
            "site_name must be provided unless the JSON export includes settings.siteName."
        )

    def _default_register_doc_id(self, site_name: str) -> str:
        """Build a predictable File 2 register identifier for a site."""

        return "site-attendance-register-" + "-".join(site_name.lower().split())
