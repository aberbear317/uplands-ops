"""SQLite persistence and compliance summary logic for Uplands documents."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional, Type, Union

from uplands_site_command_centre.permits.models import BaseDocument, DocumentStatus, FileGroup


class DocumentNotFoundError(LookupError):
    """Raised when a document cannot be found in SQLite storage."""


PermitNotFoundError = DocumentNotFoundError


@dataclass(frozen=True)
class IndexedFileRecord:
    """A physical file path tracked alongside structured document data."""

    file_name: str
    file_path: Path
    file_category: str
    file_group: FileGroup
    indexed_at: datetime
    site_name: Optional[str] = None
    related_doc_id: Optional[str] = None


@dataclass
class DocumentRepository:
    """Persist any supported Uplands document into SQLite."""

    database_path: Union[str, Path]
    document_types: Dict[str, Type[BaseDocument]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.database_path = Path(self.database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def register_document_type(self, document_model: Type[BaseDocument]) -> None:
        """Allow explicit overrides in addition to the automatic registry."""

        self.document_types[document_model.document_type] = document_model

    def _known_document_types(self) -> Dict[str, Type[BaseDocument]]:
        """Merge the automatic registry with any local overrides."""

        known_types = BaseDocument.get_document_registry()
        known_types.update(self.document_types)
        return known_types

    def create_schema(self) -> None:
        """Create the generic documents table when it does not exist."""

        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    document_type TEXT NOT NULL,
                    document_name TEXT NOT NULL,
                    file_group TEXT NOT NULL,
                    site_name TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    contractor_name TEXT,
                    linked_document_id TEXT,
                    reference_number TEXT,
                    carrier_name TEXT,
                    payload_json TEXT NOT NULL
                )
                """
            )
            self._ensure_column(connection, "documents", "contractor_name", "TEXT")
            self._ensure_column(connection, "documents", "linked_document_id", "TEXT")
            self._ensure_column(connection, "documents", "reference_number", "TEXT")
            self._ensure_column(connection, "documents", "carrier_name", "TEXT")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS indexed_files (
                    file_path TEXT PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    file_category TEXT NOT NULL,
                    file_group TEXT NOT NULL,
                    site_name TEXT,
                    related_doc_id TEXT,
                    indexed_at TEXT NOT NULL
                )
                """
            )

    def index_file(
        self,
        *,
        file_name: str,
        file_path: Union[str, Path],
        file_category: str,
        file_group: FileGroup,
        site_name: Optional[str] = None,
        related_doc_id: Optional[str] = None,
    ) -> IndexedFileRecord:
        """Persist a physical file location for direct opening in the app."""

        indexed_record = IndexedFileRecord(
            file_name=file_name,
            file_path=Path(file_path).resolve(),
            file_category=file_category,
            file_group=file_group,
            indexed_at=datetime.now(),
            site_name=site_name,
            related_doc_id=related_doc_id,
        )

        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO indexed_files (
                    file_path,
                    file_name,
                    file_category,
                    file_group,
                    site_name,
                    related_doc_id,
                    indexed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_path) DO UPDATE SET
                    file_name = excluded.file_name,
                    file_category = excluded.file_category,
                    file_group = excluded.file_group,
                    site_name = excluded.site_name,
                    related_doc_id = excluded.related_doc_id,
                    indexed_at = excluded.indexed_at
                """,
                (
                    str(indexed_record.file_path),
                    indexed_record.file_name,
                    indexed_record.file_category,
                    indexed_record.file_group.value,
                    indexed_record.site_name,
                    indexed_record.related_doc_id,
                    indexed_record.indexed_at.isoformat(timespec="seconds"),
                ),
            )

        return indexed_record

    def save(self, document: BaseDocument) -> None:
        """Insert or update a document record."""

        self._apply_document_gatekeepers(document)
        self._persist_document(document)
        self._sync_related_documents_after_save(document)

    def delete_document(self, doc_id: str) -> None:
        """Delete a document and any indexed files linked to it."""

        with sqlite3.connect(self.database_path) as connection:
            document_row = connection.execute(
                "SELECT 1 FROM documents WHERE doc_id = ?",
                (doc_id,),
            ).fetchone()
            if document_row is None:
                raise DocumentNotFoundError(f"Document {doc_id!r} was not found.")

            connection.execute(
                "DELETE FROM indexed_files WHERE related_doc_id = ?",
                (doc_id,),
            )
            connection.execute(
                "DELETE FROM documents WHERE doc_id = ?",
                (doc_id,),
            )

    def delete_document_and_files(self, doc_id: str) -> List[Path]:
        """Delete a document, its indexed-file rows, and any linked physical files."""

        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            document_row = connection.execute(
                "SELECT payload_json FROM documents WHERE doc_id = ?",
                (doc_id,),
            ).fetchone()
            if document_row is None:
                raise DocumentNotFoundError(f"Document {doc_id!r} was not found.")

            indexed_file_rows = connection.execute(
                "SELECT file_path FROM indexed_files WHERE related_doc_id = ?",
                (doc_id,),
            ).fetchall()
            payload = json.loads(str(document_row["payload_json"]))

            candidate_paths = {
                Path(str(row["file_path"])).resolve()
                for row in indexed_file_rows
                if row["file_path"]
            }
            for payload_key in (
                "signature_image_path",
                "completed_document_path",
                "sign_in_signature_path",
                "sign_out_signature_path",
                "generated_document_path",
            ):
                payload_value = payload.get(payload_key)
                if not payload_value:
                    continue
                candidate_paths.add(Path(str(payload_value)).resolve())
            competency_card_paths = str(payload.get("competency_card_paths", "") or "")
            for path_text in competency_card_paths.split(","):
                if not path_text.strip():
                    continue
                candidate_paths.add(Path(path_text.strip()).resolve())

            connection.execute(
                "DELETE FROM indexed_files WHERE related_doc_id = ?",
                (doc_id,),
            )
            connection.execute(
                "DELETE FROM documents WHERE doc_id = ?",
                (doc_id,),
            )

        deleted_paths: List[Path] = []
        for candidate_path in sorted(candidate_paths):
            try:
                if candidate_path.exists():
                    candidate_path.unlink()
                    deleted_paths.append(candidate_path)
            except OSError:
                continue
        return deleted_paths

    def delete_documents_and_files(self, doc_ids: Iterable[str]) -> List[Path]:
        """Delete multiple documents and collect every linked physical file removed."""

        deleted_paths: List[Path] = []
        seen_doc_ids = set()
        for doc_id in doc_ids:
            resolved_doc_id = str(doc_id).strip()
            if not resolved_doc_id or resolved_doc_id in seen_doc_ids:
                continue
            seen_doc_ids.add(resolved_doc_id)
            deleted_paths.extend(self.delete_document_and_files(resolved_doc_id))
        return deleted_paths

    def delete_indexed_file(self, file_path: Union[str, Path]) -> None:
        """Delete one indexed physical file record by absolute path."""

        resolved_path = str(Path(file_path).resolve())
        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                "DELETE FROM indexed_files WHERE file_path = ?",
                (resolved_path,),
            )

    def _persist_document(self, document: BaseDocument) -> None:
        """Write the document payload to SQLite without any follow-up sync."""

        payload = document.to_storage_dict()
        metadata = document.get_repository_metadata()

        with sqlite3.connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO documents (
                    doc_id,
                    document_type,
                    document_name,
                    file_group,
                    site_name,
                    created_at,
                    status,
                    file_path,
                    contractor_name,
                    linked_document_id,
                    reference_number,
                    carrier_name,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(doc_id) DO UPDATE SET
                    document_type = excluded.document_type,
                    document_name = excluded.document_name,
                    file_group = excluded.file_group,
                    site_name = excluded.site_name,
                    created_at = excluded.created_at,
                    status = excluded.status,
                    file_path = excluded.file_path,
                    contractor_name = excluded.contractor_name,
                    linked_document_id = excluded.linked_document_id,
                    reference_number = excluded.reference_number,
                    carrier_name = excluded.carrier_name,
                    payload_json = excluded.payload_json
                """,
                (
                    document.doc_id,
                    document.document_type,
                    document.document_name,
                    document.file_group.value,
                    document.site_name,
                    document.created_at.isoformat(timespec="seconds"),
                    document.status.value,
                    str(document.get_file_path()),
                    metadata.get("contractor_name"),
                    metadata.get("linked_document_id"),
                    metadata.get("reference_number") or metadata.get("wtn_number"),
                    metadata.get("carrier_name"),
                    json.dumps(payload, sort_keys=True),
                ),
            )

    def get(self, doc_id: str) -> BaseDocument:
        """Load a single document by identifier."""

        known_types = self._known_document_types()

        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT payload_json, document_type FROM documents WHERE doc_id = ?",
                (doc_id,),
            ).fetchone()

        if row is None:
            raise DocumentNotFoundError(f"Document {doc_id!r} was not found.")

        document_type = str(row["document_type"])
        document_model = known_types.get(document_type)
        if document_model is None:
            raise ValueError(f"Unsupported document type {document_type!r}.")

        return document_model.from_storage_dict(json.loads(str(row["payload_json"])))

    def list_documents(
        self,
        *,
        document_type: Optional[str] = None,
        file_group: Optional[FileGroup] = None,
        site_name: Optional[str] = None,
        contractor_name: Optional[str] = None,
        reference_number: Optional[str] = None,
        carrier_name: Optional[str] = None,
    ) -> List[BaseDocument]:
        """Return stored documents with optional filtering."""

        known_types = self._known_document_types()
        query = "SELECT payload_json, document_type FROM documents"
        filters = []
        values: List[Any] = []

        if document_type:
            filters.append("document_type = ?")
            values.append(document_type)
        if file_group:
            filters.append("file_group = ?")
            values.append(file_group.value)
        if site_name:
            filters.append("site_name = ?")
            values.append(site_name)
        if contractor_name:
            filters.append("contractor_name = ? COLLATE NOCASE")
            values.append(contractor_name)
        if reference_number:
            filters.append("reference_number = ? COLLATE NOCASE")
            values.append(reference_number)
        if carrier_name:
            filters.append("carrier_name = ? COLLATE NOCASE")
            values.append(carrier_name)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY created_at DESC, doc_id ASC"

        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(query, values).fetchall()

        documents: List[BaseDocument] = []
        for row in rows:
            resolved_type = str(row["document_type"])
            document_model = known_types.get(resolved_type)
            if document_model is None:
                raise ValueError(f"Unsupported document type {resolved_type!r}.")
            documents.append(
                document_model.from_storage_dict(json.loads(str(row["payload_json"])))
            )
        return documents

    def search_by_contractor_name(self, contractor_name: str) -> List[BaseDocument]:
        """Return every indexed document associated with a contractor."""

        return self.list_documents(contractor_name=contractor_name)

    def search_by_wtn_number(self, wtn_number: str) -> List[BaseDocument]:
        """Return every indexed document associated with a waste transfer note number."""

        return self.list_documents(reference_number=wtn_number)

    def search_by_carrier_name(self, carrier_name: str) -> List[BaseDocument]:
        """Return every indexed document associated with a waste carrier."""

        return self.list_documents(carrier_name=carrier_name)

    def list_indexed_files(
        self,
        *,
        file_group: Optional[FileGroup] = None,
        file_category: Optional[str] = None,
        related_doc_id: Optional[str] = None,
    ) -> List[IndexedFileRecord]:
        """Return tracked physical files for direct application access."""

        query = "SELECT * FROM indexed_files"
        filters = []
        values: List[Any] = []

        if file_group:
            filters.append("file_group = ?")
            values.append(file_group.value)
        if file_category:
            filters.append("file_category = ?")
            values.append(file_category)
        if related_doc_id:
            filters.append("related_doc_id = ?")
            values.append(related_doc_id)
        if filters:
            query += " WHERE " + " AND ".join(filters)
        query += " ORDER BY indexed_at DESC, file_name ASC"

        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(query, values).fetchall()

        return [
            IndexedFileRecord(
                file_name=str(row["file_name"]),
                file_path=Path(str(row["file_path"])),
                file_category=str(row["file_category"]),
                file_group=FileGroup(str(row["file_group"])),
                site_name=(
                    str(row["site_name"])
                    if row["site_name"] is not None
                    else None
                ),
                related_doc_id=(
                    str(row["related_doc_id"])
                    if row["related_doc_id"] is not None
                    else None
                ),
                indexed_at=datetime.fromisoformat(str(row["indexed_at"])),
            )
            for row in rows
        ]

    def _apply_document_gatekeepers(self, document: BaseDocument) -> None:
        """Apply document-specific pre-save checks that depend on repository state."""

        from uplands_site_command_centre.permits.carrier_compliance import (
            evaluate_waste_transfer_note_verification,
        )
        from uplands_site_command_centre.permits.models import (
            WasteRegister,
            WasteTransferNoteDocument,
        )

        if isinstance(document, WasteTransferNoteDocument):
            evaluate_waste_transfer_note_verification(document, self)
            return

        if isinstance(document, WasteRegister):
            for waste_transfer_note in document.waste_transfer_notes:
                evaluate_waste_transfer_note_verification(waste_transfer_note, self)

    def _sync_related_documents_after_save(self, document: BaseDocument) -> None:
        """Refresh dependent documents after a record that affects them changes."""

        from uplands_site_command_centre.permits.models import (
            CarrierComplianceDocument,
            WasteRegister,
            WasteTransferNoteDocument,
        )

        if not isinstance(document, CarrierComplianceDocument):
            return

        for waste_transfer_note in self.list_documents(
            document_type=WasteTransferNoteDocument.document_type,
            carrier_name=document.carrier_name,
        ):
            if not isinstance(waste_transfer_note, WasteTransferNoteDocument):
                continue
            self._apply_document_gatekeepers(waste_transfer_note)
            self._persist_document(waste_transfer_note)

        for waste_register in self.list_documents(document_type=WasteRegister.document_type):
            if not isinstance(waste_register, WasteRegister):
                continue
            if not any(
                waste_transfer_note.carrier_name.casefold() == document.carrier_name.casefold()
                for waste_transfer_note in waste_register.waste_transfer_notes
            ):
                continue
            self._apply_document_gatekeepers(waste_register)
            self._persist_document(waste_register)

    def get_site_compliance_summary(self, site_name: str) -> Dict[str, Any]:
        """Return an auditor-friendly dashboard grouped by file group."""

        summary = {
            "site_name": site_name,
            "files": {
                file_group.value: {
                    "count": 0,
                    "total_count": 0,
                    "status_counts": {
                        status.value: 0 for status in DocumentStatus
                    },
                    "active_documents": [],
                }
                for file_group in FileGroup
            },
        }

        with sqlite3.connect(self.database_path) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT
                    doc_id,
                    document_type,
                    document_name,
                    file_group,
                    status,
                    file_path
                FROM documents
                WHERE site_name = ?
                ORDER BY file_group ASC, document_name ASC, doc_id ASC
                """,
                (site_name,),
            ).fetchall()

        for row in rows:
            group_summary = summary["files"][str(row["file_group"])]
            status_value = str(row["status"])
            group_summary["total_count"] += 1
            group_summary["status_counts"][status_value] += 1

            if status_value == DocumentStatus.ACTIVE.value:
                group_summary["count"] += 1
                group_summary["active_documents"].append(
                    {
                        "doc_id": str(row["doc_id"]),
                        "document_type": str(row["document_type"]),
                        "document_name": str(row["document_name"]),
                        "status": status_value,
                        "file_path": str(row["file_path"]),
                    }
                )

        return summary

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_type: str,
    ) -> None:
        """Add a column when upgrading an existing local SQLite database."""

        existing_columns = {
            str(row[1])
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in existing_columns:
            return
        connection.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        )


PermitRepository = DocumentRepository
