"""Document hierarchy and typed site document models for Uplands."""

from __future__ import annotations

from abc import ABC
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta
from enum import Enum
import json
from pathlib import Path
import re
from typing import Any, ClassVar, Dict, FrozenSet, List, Mapping, MutableMapping, Optional, Tuple, Type, TypeVar


class DocumentStatus(str, Enum):
    """Shared lifecycle states for all site documents."""

    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"

    @property
    def label(self) -> str:
        """Return a human-readable label for template and JSON export."""

        return self.value.title()


class FileGroup(str, Enum):
    """The four-file documentation structure used by Uplands."""

    FILE_1 = "File 1"
    FILE_2 = "File 2"
    FILE_3 = "File 3"
    FILE_4 = "File 4"


class ValidationError(ValueError):
    """Raised when document data fails a business-rule validation check."""


class LadderStabilisationMethod(str, Enum):
    """Ways a ladder can be stabilised on site."""

    FOOTED = "footed"
    TIED_AT_TOP = "tied_at_top"
    TIED_AT_BOTTOM = "tied_at_bottom"

    @property
    def label(self) -> str:
        """Return a human-readable label for document export."""

        return self.value.replace("_", " ").title()


class IncidentType(str, Enum):
    """Incident categories used in the File 1 incident log."""

    ACCIDENT = "accident"
    NEAR_MISS = "near_miss"
    PROPERTY_DAMAGE = "property_damage"

    @property
    def label(self) -> str:
        """Return a human-readable label for document export."""

        return self.value.replace("_", " ").title()


class CarrierComplianceDocumentType(str, Enum):
    """Carrier compliance document categories required for waste movement."""

    LICENCE = "licence"
    INSURANCE = "insurance"

    @property
    def label(self) -> str:
        """Return a human-readable label for template and JSON export."""

        return self.value.title()


class ComplianceAlertStatus(str, Enum):
    """Severity levels for carrier compliance monitoring."""

    OK = "OK"
    CRITICAL = "CRITICAL"


class VerificationStatus(str, Enum):
    """Verification state applied to waste transfer notes."""

    VERIFIED = "VERIFIED"
    UNVERIFIED = "UNVERIFIED"


class TemplateRegistry:
    """Hard-coded registry of approved official templates."""

    PROJECT_ROOT: ClassVar[Path] = Path(__file__).resolve().parents[2]
    TEMPLATE_PATHS: ClassVar[Dict[str, Path]] = {
        "ladder_permit": Path("UHSF21.09 Step Ladders Permit.docx"),
    }

    @classmethod
    def resolve_template_path(cls, document_type: str) -> Path:
        """Return the approved template path for the given document type."""

        registered_path = cls.TEMPLATE_PATHS.get(document_type)
        if registered_path is None:
            raise KeyError(f"No approved template is registered for {document_type!r}.")

        if registered_path.suffix.lower() != ".docx":
            raise ValueError(
                f"Registered template path for {document_type!r} must point to a .docx file."
            )

        if registered_path.is_absolute():
            return registered_path.resolve()
        return (cls.PROJECT_ROOT / registered_path).resolve()


COMMON_CONSTRUCTION_EWC_CODES: FrozenSet[str] = frozenset(
    {
        "15 01 01",
        "15 01 02",
        "15 01 03",
        "15 01 04",
        "15 01 06",
        "15 01 07",
        "17 01 01",
        "17 01 02",
        "17 01 03",
        "17 01 07",
        "17 02 01",
        "17 02 02",
        "17 02 03",
        "17 03 02",
        "17 04 01",
        "17 04 02",
        "17 04 05",
        "17 04 07",
        "17 05 04",
        "17 06 04",
        "17 06 05*",
        "17 08 02",
        "17 09 03*",
        "17 09 04",
        "20 01 21*",
        "20 01 35*",
    }
)


def _require_text(value: str, field_name: str) -> str:
    """Reject blank strings so invalid documents never enter storage."""

    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string.")

    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} must not be blank.")
    return cleaned


def _normalise_optional_text(value: Optional[str], field_name: str) -> str:
    """Allow optional text fields while still normalising whitespace."""

    if value is None:
        return ""
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string or None.")
    return value.strip()


def _normalise_text_list(values: List[str], field_name: str) -> List[str]:
    """Normalise a list of required text values and reject blanks."""

    if not isinstance(values, list):
        raise TypeError(f"{field_name} must be a list of strings.")

    cleaned_values: List[str] = []
    for value in values:
        cleaned_values.append(_require_text(value, field_name))
    return cleaned_values


def _coerce_date(value: date, field_name: str) -> date:
    """Accept ``date`` or ``datetime`` and normalise to ``date``."""

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            try:
                return datetime.strptime(value, "%d/%m/%Y").date()
            except ValueError as exc:
                raise ValueError(
                    f"{field_name} must be an ISO date string or use DD/MM/YYYY."
                ) from exc
    raise TypeError(f"{field_name} must be a date, datetime, or ISO date string.")


def _coerce_datetime(value: datetime, field_name: str) -> datetime:
    """Accept datetimes or ISO strings when rehydrating from storage."""

    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"{field_name} must be a datetime or ISO datetime string.")


def _coerce_time(value: time, field_name: str) -> time:
    """Accept ``time`` objects or ISO strings when rehydrating from storage."""

    if isinstance(value, time):
        return value
    if isinstance(value, str):
        return time.fromisoformat(value)
    raise TypeError(f"{field_name} must be a time or ISO time string.")


def _coerce_status(value: DocumentStatus, field_name: str = "status") -> DocumentStatus:
    """Allow enum instances or their serialized string form."""

    if isinstance(value, DocumentStatus):
        return value
    if isinstance(value, str):
        try:
            return DocumentStatus(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid DocumentStatus.") from exc
    raise TypeError(f"{field_name} must be a DocumentStatus or string.")


def _coerce_stabilisation_method(
    value: LadderStabilisationMethod,
    field_name: str = "ladder_stabilisation_method",
) -> LadderStabilisationMethod:
    """Allow enum instances or their serialized string form."""

    if isinstance(value, LadderStabilisationMethod):
        return value
    if isinstance(value, str):
        try:
            return LadderStabilisationMethod(value)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be a valid LadderStabilisationMethod."
            ) from exc
    raise TypeError(f"{field_name} must be a LadderStabilisationMethod or string.")


def _coerce_incident_type(
    value: IncidentType,
    field_name: str = "incident_type",
) -> IncidentType:
    """Allow enum instances or their serialized string form."""

    if isinstance(value, IncidentType):
        return value
    if isinstance(value, str):
        try:
            return IncidentType(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid IncidentType.") from exc
    raise TypeError(f"{field_name} must be an IncidentType or string.")


def _coerce_carrier_compliance_document_type(
    value: CarrierComplianceDocumentType,
    field_name: str = "carrier_document_type",
) -> CarrierComplianceDocumentType:
    """Allow enum instances or their serialized string form."""

    if isinstance(value, CarrierComplianceDocumentType):
        return value
    if isinstance(value, str):
        try:
            return CarrierComplianceDocumentType(value.lower())
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be a valid CarrierComplianceDocumentType."
            ) from exc
    raise TypeError(f"{field_name} must be a CarrierComplianceDocumentType or string.")


def _coerce_verification_status(
    value: VerificationStatus,
    field_name: str = "verification_status",
) -> VerificationStatus:
    """Allow enum instances or their serialized string form."""

    if isinstance(value, VerificationStatus):
        return value
    if isinstance(value, str):
        try:
            return VerificationStatus(value.upper())
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid VerificationStatus.") from exc
    raise TypeError(f"{field_name} must be a VerificationStatus or string.")


def _require_bool(value: bool, field_name: str) -> bool:
    """Reject integers such as ``0`` and ``1`` for boolean flags."""

    if isinstance(value, bool):
        return value
    raise TypeError(f"{field_name} must be a boolean.")


def _slugify(value: str) -> str:
    """Create predictable folder names for the file-system view."""

    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")


def _coerce_non_negative_float(value: Any, field_name: str) -> float:
    """Convert numeric values and reject negative totals."""

    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be a numeric value.")
    if isinstance(value, (int, float)):
        numeric_value = float(value)
    elif isinstance(value, str):
        numeric_value = float(value)
    else:
        raise TypeError(f"{field_name} must be a numeric value.")

    if numeric_value < 0:
        raise ValueError(f"{field_name} must be zero or greater.")
    return numeric_value


def _normalise_ewc_code(value: str, field_name: str = "ewc_code") -> str:
    """Normalise common EWC code formats into a canonical form."""

    cleaned_value = _require_text(value, field_name).upper()
    digits_only = "".join(character for character in cleaned_value if character.isdigit())
    has_hazard_suffix = "*" in cleaned_value

    if len(digits_only) == 6:
        canonical_code = (
            f"{digits_only[:2]} {digits_only[2:4]} {digits_only[4:6]}"
            f"{'*' if has_hazard_suffix else ''}"
        )
        return canonical_code

    normalised_spacing = " ".join(cleaned_value.split())
    if normalised_spacing.endswith(" *"):
        return normalised_spacing[:-2] + "*"
    return normalised_spacing


def _serialise_value(value: Any) -> Any:
    """Convert dataclass payloads into JSON-safe primitives."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialise_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialise_value(item) for key, item in value.items()}
    return value


def _stringify_for_template(value: Any) -> str:
    """Render values into strings suitable for placeholder replacement."""

    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.strftime("%H:%M")
    if isinstance(value, Enum):
        if hasattr(value, "label"):
            return str(value.label)
        return str(value.value)
    if value is None:
        return ""
    return str(value)


TDocument = TypeVar("TDocument", bound="BaseDocument")


@dataclass
class BaseDocument(ABC):
    """Base abstraction for every file held in the Uplands four-file system."""

    _register_document_type: ClassVar[bool] = False
    _document_registry: ClassVar[Dict[str, Type["BaseDocument"]]] = {}

    doc_id: str
    site_name: str
    created_at: datetime
    status: DocumentStatus

    document_type: ClassVar[str] = "base_document"
    document_name: ClassVar[str] = "Base Document"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_1
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Register concrete document classes for repository discovery."""

        super().__init_subclass__(**kwargs)
        if not getattr(cls, "_register_document_type", False):
            return

        document_type = getattr(cls, "document_type", "")
        if not document_type or document_type == BaseDocument.document_type:
            raise ValueError(
                f"{cls.__name__} must define a unique document_type or disable registration."
            )
        BaseDocument._document_registry[document_type] = cls

    def __post_init__(self) -> None:
        self.doc_id = _require_text(self.doc_id, "doc_id")
        self.site_name = _require_text(self.site_name, "site_name")
        self.created_at = _coerce_datetime(self.created_at, "created_at")
        self.status = _coerce_status(self.status)

    @classmethod
    def get_document_registry(cls) -> Dict[str, Type["BaseDocument"]]:
        """Return a copy of all document types registered in the app."""

        return dict(cls._document_registry)

    def get_file_path(self) -> Path:
        """Return the logical path for this document inside the 4-file system."""

        return (
            Path(self.file_group.value)
            / _slugify(self.site_name)
            / self.document_type
            / self.doc_id
        )

    def to_storage_dict(self) -> Dict[str, Any]:
        """Serialize the document into JSON-safe storage data."""

        payload = _serialise_value(asdict(self))
        payload["document_type"] = self.document_type
        payload["document_name"] = self.document_name
        payload["file_group"] = self.file_group.value
        payload["file_path"] = str(self.get_file_path())
        return payload

    def to_document_dict(self) -> Dict[str, Any]:
        """Return a machine-readable document export."""

        return self.to_storage_dict()

    def to_json(self, indent: int = 2) -> str:
        """Emit a JSON export suitable for downstream integrations."""

        return json.dumps(self.to_document_dict(), indent=indent, sort_keys=True)

    def to_template_context(self) -> Dict[str, str]:
        """Return a flat template context using snake_case placeholders."""

        payload = self.to_storage_dict()
        context: Dict[str, str] = {}
        for key, value in payload.items():
            if isinstance(value, (list, dict)):
                continue
            context[key] = _stringify_for_template(value)
        return context

    def get_repository_metadata(self) -> Dict[str, str]:
        """Return indexed metadata stored alongside the JSON payload."""

        return {}

    @classmethod
    def _deserialize_base_fields(
        cls,
        data: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        """Parse storage primitives back into typed base-document values."""

        payload: MutableMapping[str, Any] = dict(data)
        payload.pop("document_type", None)
        payload.pop("document_name", None)
        payload.pop("file_group", None)
        payload.pop("file_path", None)
        payload["created_at"] = _coerce_datetime(payload["created_at"], "created_at")
        payload["status"] = _coerce_status(payload["status"])
        return payload

    @classmethod
    def from_storage_dict(cls: Type[TDocument], data: Mapping[str, Any]) -> TDocument:
        """Rehydrate a document with only base fields from storage."""

        return cls(**cls._deserialize_base_fields(data))


@dataclass
class PermitDocument(BaseDocument):
    """Base model for permits stored in File 4."""

    _register_document_type: ClassVar[bool] = True

    permit_number: str
    project_name: str
    project_number: str
    location_of_work: str
    description_of_work: str
    valid_from_date: date
    valid_from_time: time
    valid_to_date: date
    valid_to_time: time

    document_type: ClassVar[str] = "permit"
    document_name: ClassVar[str] = "Permit"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_4
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "permit_number",
            "site_name",
            "project_name",
            "project_number",
            "location_of_work",
            "description_of_work",
            "valid_from_date",
            "valid_from_time",
            "valid_to_date",
            "valid_to_time",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.permit_number = _require_text(self.permit_number, "permit_number")
        self.project_name = _require_text(self.project_name, "project_name")
        self.project_number = _require_text(self.project_number, "project_number")
        self.location_of_work = _require_text(self.location_of_work, "location_of_work")
        self.description_of_work = _require_text(
            self.description_of_work,
            "description_of_work",
        )
        self.valid_from_date = _coerce_date(self.valid_from_date, "valid_from_date")
        self.valid_from_time = _coerce_time(self.valid_from_time, "valid_from_time")
        self.valid_to_date = _coerce_date(self.valid_to_date, "valid_to_date")
        self.valid_to_time = _coerce_time(self.valid_to_time, "valid_to_time")

        if self.valid_to_datetime < self.valid_from_datetime:
            raise ValueError("Permit expiry must be on or after the valid-from time.")

    @property
    def valid_from_datetime(self) -> datetime:
        """Return the permit's effective start date/time."""

        return datetime.combine(self.valid_from_date, self.valid_from_time)

    @property
    def valid_to_datetime(self) -> datetime:
        """Return the permit's effective end date/time."""

        return datetime.combine(self.valid_to_date, self.valid_to_time)

    @classmethod
    def _deserialize_permit_fields(
        cls,
        data: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        """Parse the shared permit fields from storage data."""

        payload = cls._deserialize_base_fields(data)
        payload["valid_from_date"] = _coerce_date(
            payload["valid_from_date"],
            "valid_from_date",
        )
        payload["valid_from_time"] = _coerce_time(
            payload["valid_from_time"],
            "valid_from_time",
        )
        payload["valid_to_date"] = _coerce_date(payload["valid_to_date"], "valid_to_date")
        payload["valid_to_time"] = _coerce_time(payload["valid_to_time"], "valid_to_time")
        return payload

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "PermitDocument":
        """Rehydrate a generic permit from storage."""

        return cls(**cls._deserialize_permit_fields(data))


@dataclass
class SiteAttendanceRecord:
    """One attendance row from the KPI export."""

    date: date
    company: str
    workerName: str
    timeIn: time
    timeOut: time
    totalHours: float

    def __post_init__(self) -> None:
        self.date = _coerce_date(self.date, "date")
        self.company = _require_text(self.company, "company")
        self.workerName = _require_text(self.workerName, "workerName")
        self.timeIn = _coerce_time(self.timeIn, "timeIn")
        self.timeOut = _coerce_time(self.timeOut, "timeOut")
        self.totalHours = _coerce_non_negative_float(self.totalHours, "totalHours")

    def duplicate_key(self) -> Tuple[str, str]:
        """Return the key used to detect duplicate attendance rows."""

        return (self.date.isoformat(), self.workerName.strip().casefold())

    def to_template_context(self, index: int) -> Dict[str, str]:
        """Expose indexed placeholders for template rows."""

        return {
            f"attendance_{index}_date": _stringify_for_template(self.date),
            f"attendance_{index}_company": self.company,
            f"attendance_{index}_workerName": self.workerName,
            f"attendance_{index}_timeIn": _stringify_for_template(self.timeIn),
            f"attendance_{index}_timeOut": _stringify_for_template(self.timeOut),
            f"attendance_{index}_totalHours": str(self.totalHours),
        }

    @classmethod
    def from_json_row(cls, row: Mapping[str, Any]) -> "SiteAttendanceRecord":
        """Map one JSON export row into a typed attendance record."""

        if not isinstance(row, Mapping):
            raise TypeError("Attendance row must be a mapping.")
        return cls(
            date=row["date"],
            company=row["company"],
            workerName=row["workerName"],
            timeIn=row["timeIn"],
            timeOut=row["timeOut"],
            totalHours=row["totalHours"],
        )

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "SiteAttendanceRecord":
        """Rehydrate an attendance row from storage."""

        return cls.from_json_row(data)


@dataclass
class SiteAttendanceRegister(BaseDocument):
    """File 2 site attendance register backed by KPI JSON imports."""

    _register_document_type: ClassVar[bool] = True

    attendance_records: List[SiteAttendanceRecord] = field(default_factory=list)

    document_type: ClassVar[str] = "site_attendance_register"
    document_name: ClassVar[str] = "Site Attendance Register"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_2
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {"site_name", "attendance_records"}
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.attendance_records = [
            record
            if isinstance(record, SiteAttendanceRecord)
            else SiteAttendanceRecord.from_storage_dict(record)
            for record in self.attendance_records
        ]

    def has_record(self, record_date: date, worker_name: str) -> bool:
        """Return ``True`` when a matching attendance row already exists."""

        comparison_key = (
            _coerce_date(record_date, "record_date").isoformat(),
            _require_text(worker_name, "worker_name").casefold(),
        )
        return any(
            attendance_record.duplicate_key() == comparison_key
            for attendance_record in self.attendance_records
        )

    def add_attendance_record(self, record: SiteAttendanceRecord) -> bool:
        """Append a record unless the date/worker combination already exists."""

        if self.has_record(record.date, record.workerName):
            return False
        self.attendance_records.append(record)
        return True

    def to_template_context(self) -> Dict[str, str]:
        """Flatten attendance data into template placeholders."""

        context = super().to_template_context()
        summary_lines = []
        for index, record in enumerate(self.attendance_records, start=1):
            context.update(record.to_template_context(index))
            summary_lines.append(
                (
                    f"{record.date.isoformat()} | {record.company} | {record.workerName} | "
                    f"{record.timeIn.strftime('%H:%M')} - {record.timeOut.strftime('%H:%M')} | "
                    f"{record.totalHours:.2f} hours"
                )
            )
        context["attendance_record_count"] = str(len(self.attendance_records))
        context["attendance_records"] = "\n".join(summary_lines)
        return context

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "SiteAttendanceRegister":
        """Rehydrate an attendance register from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["attendance_records"] = [
            SiteAttendanceRecord.from_storage_dict(record)
            for record in payload.get("attendance_records", [])
        ]
        return cls(**payload)


@dataclass
class RAMSDocument(BaseDocument):
    """File 3 RAMS document for contractor method statements."""

    _register_document_type: ClassVar[bool] = True

    contractor_name: str
    activity_description: str
    approval_date: date

    document_type: ClassVar[str] = "rams"
    document_name: ClassVar[str] = "RAMS"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_3
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "site_name",
            "contractor_name",
            "activity_description",
            "approval_date",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.contractor_name = _require_text(self.contractor_name, "contractor_name")
        self.activity_description = _require_text(
            self.activity_description,
            "activity_description",
        )
        self.approval_date = _coerce_date(self.approval_date, "approval_date")

    def has_expired(
        self,
        *,
        on_date: Optional[date] = None,
        max_age: timedelta = timedelta(days=365),
    ) -> bool:
        """Flag RAMS that are older than the allowed age."""

        if not isinstance(max_age, timedelta):
            raise TypeError("max_age must be a datetime.timedelta instance.")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be greater than zero.")

        effective_date = on_date or date.today()
        return effective_date > (self.approval_date + max_age)

    def get_repository_metadata(self) -> Dict[str, str]:
        """Expose contractor indexing metadata for repository search."""

        return {"contractor_name": self.contractor_name}

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "RAMSDocument":
        """Rehydrate RAMS data from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["approval_date"] = _coerce_date(payload["approval_date"], "approval_date")
        return cls(**payload)


@dataclass
class COSHHDocument(BaseDocument):
    """File 3 COSHH assessment for contractor chemical compliance."""

    _register_document_type: ClassVar[bool] = True

    contractor_name: str
    substance_name: str
    hazard_pictograms: List[str] = field(default_factory=list)
    ppe_required: List[str] = field(default_factory=list)
    emergency_first_aid: str = ""

    document_type: ClassVar[str] = "coshh"
    document_name: ClassVar[str] = "COSHH Assessment"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_3
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "site_name",
            "contractor_name",
            "substance_name",
            "hazard_pictograms",
            "ppe_required",
            "emergency_first_aid",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.contractor_name = _require_text(self.contractor_name, "contractor_name")
        self.substance_name = _require_text(self.substance_name, "substance_name")
        self.hazard_pictograms = _normalise_text_list(
            list(self.hazard_pictograms),
            "hazard_pictograms",
        )
        self.ppe_required = _normalise_text_list(list(self.ppe_required), "ppe_required")
        self.emergency_first_aid = _require_text(
            self.emergency_first_aid,
            "emergency_first_aid",
        )

    def to_template_context(self) -> Dict[str, str]:
        """Flatten COSHH data into template placeholders."""

        context = super().to_template_context()
        context.update(
            {
                "hazard_pictograms": "\n".join(self.hazard_pictograms),
                "hazard_pictogram_count": str(len(self.hazard_pictograms)),
                "ppe_required": "\n".join(self.ppe_required),
                "ppe_required_count": str(len(self.ppe_required)),
            }
        )
        for index, pictogram in enumerate(self.hazard_pictograms, start=1):
            context[f"hazard_pictogram_{index}"] = pictogram
        for index, item in enumerate(self.ppe_required, start=1):
            context[f"ppe_required_{index}"] = item
        return context

    def get_repository_metadata(self) -> Dict[str, str]:
        """Expose contractor indexing metadata for repository search."""

        return {"contractor_name": self.contractor_name}

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "COSHHDocument":
        """Rehydrate COSHH data from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["hazard_pictograms"] = list(payload.get("hazard_pictograms", []))
        payload["ppe_required"] = list(payload.get("ppe_required", []))
        return cls(**payload)


@dataclass
class InductionDocument(BaseDocument):
    """File 3 site induction record linked to a RAMS document."""

    _register_document_type: ClassVar[bool] = True

    contractor_name: str
    individual_name: str
    linked_rams_doc_id: str

    document_type: ClassVar[str] = "induction"
    document_name: ClassVar[str] = "Site Induction Log"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_3
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "site_name",
            "contractor_name",
            "individual_name",
            "linked_rams_doc_id",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.contractor_name = _require_text(self.contractor_name, "contractor_name")
        self.individual_name = _require_text(self.individual_name, "individual_name")
        self.linked_rams_doc_id = _require_text(
            self.linked_rams_doc_id,
            "linked_rams_doc_id",
        )

    def get_repository_metadata(self) -> Dict[str, str]:
        """Expose contractor and linkage metadata for repository search."""

        return {
            "contractor_name": self.contractor_name,
            "linked_document_id": self.linked_rams_doc_id,
        }

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "InductionDocument":
        """Rehydrate an induction record from storage."""

        payload = cls._deserialize_base_fields(data)
        return cls(**payload)


@dataclass
class IncidentLogDocument(BaseDocument):
    """Typed File 1 incident report document."""

    _register_document_type: ClassVar[bool] = True

    incident_type: IncidentType
    location: str
    description: str
    witness_list: List[str] = field(default_factory=list)

    document_type: ClassVar[str] = "incident_log"
    document_name: ClassVar[str] = "Incident Report"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_1
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "site_name",
            "incident_type_label",
            "location",
            "description",
            "witness_list",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.incident_type = _coerce_incident_type(self.incident_type)
        self.location = _require_text(self.location, "location")
        self.description = _require_text(self.description, "description")
        self.witness_list = [
            _require_text(witness, "witness_list entry")
            for witness in list(self.witness_list)
        ]

    def to_template_context(self) -> Dict[str, str]:
        """Flatten incident log data into template placeholders."""

        context = super().to_template_context()
        context.update(
            {
                "incident_type_label": self.incident_type.label,
                "witness_list": "\n".join(self.witness_list),
                "witness_count": str(len(self.witness_list)),
            }
        )
        for index, witness in enumerate(self.witness_list, start=1):
            context[f"witness_{index}"] = witness
        return context

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "IncidentLogDocument":
        """Rehydrate an incident log from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["incident_type"] = _coerce_incident_type(payload["incident_type"])
        payload["witness_list"] = list(payload.get("witness_list", []))
        return cls(**payload)


@dataclass
class CarrierComplianceDocument(BaseDocument):
    """File 1 carrier licence or insurance record used for waste gatekeeping."""

    _register_document_type: ClassVar[bool] = True

    carrier_name: str
    carrier_document_type: CarrierComplianceDocumentType
    reference_number: str
    expiry_date: date

    document_type: ClassVar[str] = "carrier_compliance"
    document_name: ClassVar[str] = "Carrier Compliance Document"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_1
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "site_name",
            "carrier_name",
            "document_type",
            "reference_number",
            "expiry_date",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.carrier_name = _require_text(self.carrier_name, "carrier_name")
        self.carrier_document_type = _coerce_carrier_compliance_document_type(
            self.carrier_document_type
        )
        self.reference_number = _require_text(self.reference_number, "reference_number")
        self.expiry_date = _coerce_date(self.expiry_date, "expiry_date")

    def has_expired(self, *, on_date: Optional[date] = None) -> bool:
        """Return ``True`` when the carrier document has already expired."""

        effective_date = on_date or date.today()
        return self.expiry_date < effective_date

    def expires_within(self, days: int = 30, *, on_date: Optional[date] = None) -> bool:
        """Return ``True`` when the document expires within the requested window."""

        if days < 0:
            raise ValueError("days must be zero or greater.")
        effective_date = on_date or date.today()
        return 0 <= (self.expiry_date - effective_date).days <= days

    def to_template_context(self) -> Dict[str, str]:
        """Expose a user-facing ``document_type`` placeholder for templates."""

        context = super().to_template_context()
        context["document_type"] = self.carrier_document_type.label
        context["carrier_document_type"] = self.carrier_document_type.label
        return context

    def get_repository_metadata(self) -> Dict[str, str]:
        """Expose carrier and reference indexing metadata for repository search."""

        return {
            "carrier_name": self.carrier_name,
            "reference_number": self.reference_number,
        }

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "CarrierComplianceDocument":
        """Rehydrate a carrier compliance document from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["carrier_document_type"] = _coerce_carrier_compliance_document_type(
            payload["carrier_document_type"]
        )
        payload["expiry_date"] = _coerce_date(payload["expiry_date"], "expiry_date")
        return cls(**payload)


@dataclass
class WasteTransferNoteDocument(BaseDocument):
    """Typed File 1 waste transfer note for environmental compliance."""

    _register_document_type: ClassVar[bool] = True

    wtn_number: str
    date: date
    waste_description: str
    ewc_code: str
    quantity_tonnes: float
    carrier_name: str
    destination_facility: str
    verification_status: VerificationStatus = VerificationStatus.UNVERIFIED
    verification_notes: str = ""

    document_type: ClassVar[str] = "waste_transfer_note"
    document_name: ClassVar[str] = "Waste Transfer Note"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_1
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "site_name",
            "wtn_number",
            "date",
            "waste_description",
            "ewc_code",
            "quantity_tonnes",
            "carrier_name",
            "destination_facility",
            "verification_status",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.wtn_number = _require_text(self.wtn_number, "wtn_number")
        self.date = _coerce_date(self.date, "date")
        self.waste_description = _require_text(
            self.waste_description,
            "waste_description",
        )
        self.ewc_code = _normalise_ewc_code(self.ewc_code)
        self.quantity_tonnes = _coerce_non_negative_float(
            self.quantity_tonnes,
            "quantity_tonnes",
        )
        self.carrier_name = _require_text(self.carrier_name, "carrier_name")
        self.destination_facility = _require_text(
            self.destination_facility,
            "destination_facility",
        )
        self.verification_status = _coerce_verification_status(self.verification_status)
        self.verification_notes = _normalise_optional_text(
            self.verification_notes,
            "verification_notes",
        )
        self.validate_wtn()

    def validate_wtn(self) -> None:
        """Ensure the WTN uses an approved construction EWC code."""

        if self.ewc_code not in COMMON_CONSTRUCTION_EWC_CODES:
            raise ValidationError(
                f"Invalid EWC code {self.ewc_code!r} for waste transfer note {self.wtn_number!r}."
            )

    def get_repository_metadata(self) -> Dict[str, str]:
        """Expose WTN search keys for repository-level lookups."""

        return {
            "wtn_number": self.wtn_number,
            "reference_number": self.wtn_number,
            "carrier_name": self.carrier_name,
        }

    def set_verification_status(
        self,
        verification_status: VerificationStatus,
        verification_notes: str = "",
    ) -> None:
        """Update the WTN verification state after carrier compliance checks."""

        self.verification_status = _coerce_verification_status(verification_status)
        self.verification_notes = _normalise_optional_text(
            verification_notes,
            "verification_notes",
        )

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "WasteTransferNoteDocument":
        """Rehydrate a waste transfer note from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["date"] = _coerce_date(payload["date"], "date")
        payload["quantity_tonnes"] = _coerce_non_negative_float(
            payload["quantity_tonnes"],
            "quantity_tonnes",
        )
        payload["verification_status"] = _coerce_verification_status(
            payload.get("verification_status", VerificationStatus.UNVERIFIED.value)
        )
        payload["verification_notes"] = payload.get("verification_notes", "")
        return cls(**payload)


@dataclass
class WasteRegister(BaseDocument):
    """File 1 waste register that aggregates waste transfer notes."""

    _register_document_type: ClassVar[bool] = True

    waste_transfer_notes: List[WasteTransferNoteDocument] = field(default_factory=list)

    document_type: ClassVar[str] = "waste_register"
    document_name: ClassVar[str] = "Waste Register"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_1
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "site_name",
            "waste_transfer_notes",
            "waste_transfer_note_count",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.waste_transfer_notes = [
            note
            if isinstance(note, WasteTransferNoteDocument)
            else WasteTransferNoteDocument.from_storage_dict(note)
            for note in self.waste_transfer_notes
        ]

    def add_waste_transfer_note(self, note: WasteTransferNoteDocument) -> None:
        """Append a waste transfer note while enforcing site consistency."""

        if note.site_name != self.site_name:
            raise ValueError(
                "Waste transfer note site_name must match the waste register site_name."
            )
        self.waste_transfer_notes.append(note)

    def get_monthly_tonnage_summary(self, month: int, year: int) -> Dict[str, Any]:
        """Return the monthly waste tonnage total and a simple code breakdown."""

        if not 1 <= month <= 12:
            raise ValueError("month must be between 1 and 12.")
        if year < 1:
            raise ValueError("year must be greater than zero.")

        matching_notes = [
            note
            for note in self.waste_transfer_notes
            if note.date.month == month and note.date.year == year
        ]

        totals_by_ewc_code: Dict[str, float] = {}
        for note in matching_notes:
            totals_by_ewc_code[note.ewc_code] = (
                totals_by_ewc_code.get(note.ewc_code, 0.0) + note.quantity_tonnes
            )

        return {
            "month": month,
            "year": year,
            "note_count": len(matching_notes),
            "total_tonnage": round(
                sum(note.quantity_tonnes for note in matching_notes),
                3,
            ),
            "by_ewc_code": {
                ewc_code: round(total_tonnage, 3)
                for ewc_code, total_tonnage in sorted(totals_by_ewc_code.items())
            },
        }

    def to_template_context(self) -> Dict[str, str]:
        """Flatten waste transfer notes into template placeholders."""

        context = super().to_template_context()
        summary_lines = []
        for index, note in enumerate(self.waste_transfer_notes, start=1):
            summary_lines.append(
                (
                    f"{note.date.isoformat()} | {note.wtn_number} | {note.ewc_code} | "
                    f"{note.waste_description} | {note.quantity_tonnes:.3f} tonnes | "
                    f"{note.carrier_name} | {note.destination_facility}"
                )
            )
            note_context = note.to_template_context()
            for key, value in note_context.items():
                context[f"waste_transfer_note_{index}_{key}"] = value

        context["waste_transfer_note_count"] = str(len(self.waste_transfer_notes))
        context["waste_transfer_notes"] = "\n".join(summary_lines)
        return context

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "WasteRegister":
        """Rehydrate a waste register from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["waste_transfer_notes"] = [
            WasteTransferNoteDocument.from_storage_dict(note)
            for note in payload.get("waste_transfer_notes", [])
        ]
        return cls(**payload)


@dataclass
class LadderInspectionRecord:
    """One inspection entry from the ladder permit register."""

    inspection_date: date
    inspected_by: str
    rungs_ok: bool
    stiles_ok: bool
    feet_ok: bool
    comments_or_action_taken: str
    ok_to_use: bool

    def __post_init__(self) -> None:
        self.inspection_date = _coerce_date(self.inspection_date, "inspection_date")
        self.inspected_by = _require_text(self.inspected_by, "inspected_by")
        self.rungs_ok = _require_bool(self.rungs_ok, "rungs_ok")
        self.stiles_ok = _require_bool(self.stiles_ok, "stiles_ok")
        self.feet_ok = _require_bool(self.feet_ok, "feet_ok")
        self.comments_or_action_taken = _normalise_optional_text(
            self.comments_or_action_taken,
            "comments_or_action_taken",
        )
        self.ok_to_use = _require_bool(self.ok_to_use, "ok_to_use")

    def to_template_context(self, index: int) -> Dict[str, str]:
        """Expose indexed placeholders for template rows."""

        return {
            "inspection_%d_date" % index: _stringify_for_template(self.inspection_date),
            "inspection_%d_inspected_by" % index: self.inspected_by,
            "inspection_%d_rungs_ok" % index: _stringify_for_template(self.rungs_ok),
            "inspection_%d_stiles_ok" % index: _stringify_for_template(self.stiles_ok),
            "inspection_%d_feet_ok" % index: _stringify_for_template(self.feet_ok),
            "inspection_%d_comments_or_action_taken" % index: self.comments_or_action_taken,
            "inspection_%d_ok_to_use" % index: _stringify_for_template(self.ok_to_use),
        }

    @classmethod
    def from_storage_dict(
        cls,
        data: Mapping[str, Any],
    ) -> "LadderInspectionRecord":
        """Rehydrate an inspection entry from storage."""

        return cls(
            inspection_date=_coerce_date(data["inspection_date"], "inspection_date"),
            inspected_by=data["inspected_by"],
            rungs_ok=data["rungs_ok"],
            stiles_ok=data["stiles_ok"],
            feet_ok=data["feet_ok"],
            comments_or_action_taken=data.get("comments_or_action_taken", ""),
            ok_to_use=data["ok_to_use"],
        )


@dataclass
class LadderPermit(PermitDocument):
    """Typed representation of UHSF21.09 Step Ladders Permit."""

    _register_document_type: ClassVar[bool] = True

    safer_alternative_eliminated: bool
    task_specific_rams_prepared_and_approved: bool
    personnel_briefed_and_understand_task: bool
    competent_supervisor_appointed: bool
    competent_supervisor_name: str
    operatives_suitably_trained: bool
    ladder_length_suitable: bool
    conforms_to_bs_class_a: bool
    three_points_of_contact_maintained: bool
    harness_worn_and_secured_above_head_height: bool
    ladder_stabilisation_method: LadderStabilisationMethod
    equipment_inspected_for_defects: bool
    inspection_records: List[LadderInspectionRecord] = field(default_factory=list)

    document_type: ClassVar[str] = "ladder_permit"
    document_name: ClassVar[str] = "UHSF21.09 Step Ladders Permit"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_4
    required_template_placeholders: ClassVar[FrozenSet[str]] = (
        PermitDocument.required_template_placeholders
        | frozenset(
            {
                "competent_supervisor_name",
                "safer_alternative_eliminated",
                "task_specific_rams_prepared_and_approved",
                "personnel_briefed_and_understand_task",
                "three_points_of_contact_maintained",
                "ladder_stabilisation_method_label",
                "inspection_register",
            }
        )
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.safer_alternative_eliminated = _require_bool(
            self.safer_alternative_eliminated,
            "safer_alternative_eliminated",
        )
        self.task_specific_rams_prepared_and_approved = _require_bool(
            self.task_specific_rams_prepared_and_approved,
            "task_specific_rams_prepared_and_approved",
        )
        self.personnel_briefed_and_understand_task = _require_bool(
            self.personnel_briefed_and_understand_task,
            "personnel_briefed_and_understand_task",
        )
        self.competent_supervisor_appointed = _require_bool(
            self.competent_supervisor_appointed,
            "competent_supervisor_appointed",
        )
        self.competent_supervisor_name = _normalise_optional_text(
            self.competent_supervisor_name,
            "competent_supervisor_name",
        )
        if self.competent_supervisor_appointed and not self.competent_supervisor_name:
            raise ValueError(
                "competent_supervisor_name is required when a supervisor is appointed."
            )
        self.operatives_suitably_trained = _require_bool(
            self.operatives_suitably_trained,
            "operatives_suitably_trained",
        )
        self.ladder_length_suitable = _require_bool(
            self.ladder_length_suitable,
            "ladder_length_suitable",
        )
        self.conforms_to_bs_class_a = _require_bool(
            self.conforms_to_bs_class_a,
            "conforms_to_bs_class_a",
        )
        self.three_points_of_contact_maintained = _require_bool(
            self.three_points_of_contact_maintained,
            "three_points_of_contact_maintained",
        )
        self.harness_worn_and_secured_above_head_height = _require_bool(
            self.harness_worn_and_secured_above_head_height,
            "harness_worn_and_secured_above_head_height",
        )
        self.ladder_stabilisation_method = _coerce_stabilisation_method(
            self.ladder_stabilisation_method
        )
        self.equipment_inspected_for_defects = _require_bool(
            self.equipment_inspected_for_defects,
            "equipment_inspected_for_defects",
        )
        self.inspection_records = [
            record
            if isinstance(record, LadderInspectionRecord)
            else LadderInspectionRecord.from_storage_dict(record)
            for record in self.inspection_records
        ]

    def add_inspection_record(
        self,
        inspection_date: date,
        inspected_by: str,
        rungs_ok: bool,
        stiles_ok: bool,
        feet_ok: bool,
        comments_or_action_taken: str = "",
        ok_to_use: Optional[bool] = None,
    ) -> LadderInspectionRecord:
        """Append a new inspection record to the permit."""

        computed_ok_to_use = ok_to_use
        if computed_ok_to_use is None:
            computed_ok_to_use = bool(rungs_ok and stiles_ok and feet_ok)

        record = LadderInspectionRecord(
            inspection_date=inspection_date,
            inspected_by=inspected_by,
            rungs_ok=rungs_ok,
            stiles_ok=stiles_ok,
            feet_ok=feet_ok,
            comments_or_action_taken=comments_or_action_taken,
            ok_to_use=computed_ok_to_use,
        )
        self.inspection_records.append(record)
        return record

    def to_template_context(self) -> Dict[str, str]:
        """Flatten ladder permit data into template placeholders."""

        context = super().to_template_context()
        context.update(
            {
                "status_label": self.status.label,
                "document_name": self.document_name,
                "file_group": self.file_group.value,
                "ladder_stabilisation_method_label": self.ladder_stabilisation_method.label,
                "inspection_record_count": str(len(self.inspection_records)),
            }
        )

        inspection_summary_lines = []
        for index, record in enumerate(self.inspection_records, start=1):
            context.update(record.to_template_context(index))
            inspection_summary_lines.append(
                (
                    f"{index}. {record.inspection_date.isoformat()} | {record.inspected_by} | "
                    f"Rungs: {_stringify_for_template(record.rungs_ok)} | "
                    f"Stiles: {_stringify_for_template(record.stiles_ok)} | "
                    f"Feet: {_stringify_for_template(record.feet_ok)} | "
                    f"OK to use: {_stringify_for_template(record.ok_to_use)} | "
                    f"Comments: {record.comments_or_action_taken or '-'}"
                )
            )

        context["inspection_register"] = "\n".join(inspection_summary_lines)
        return context

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "LadderPermit":
        """Rehydrate a ladder permit including inspection records."""

        payload = cls._deserialize_permit_fields(data)
        payload["ladder_stabilisation_method"] = _coerce_stabilisation_method(
            payload["ladder_stabilisation_method"]
        )
        payload["inspection_records"] = [
            LadderInspectionRecord.from_storage_dict(record)
            for record in payload.get("inspection_records", [])
        ]
        return cls(**payload)


Permit = PermitDocument
