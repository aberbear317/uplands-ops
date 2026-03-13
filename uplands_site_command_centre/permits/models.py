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
        "ladder_permit": Path(
            "templates/UHSF21.09 Step Ladders Permit - tagged-middle-v2.docx"
        ),
        "rams_register": Path("templates/16.4 RAMs Register - tagged.docx"),
        "coshh_register": Path("templates/COSHH Register - tagged.docx"),
        "plant_register": Path(
            "templates/UHSF18.32 Plant Hire Site Register - tagged.docx"
        ),
        "permit_register": Path(
            "templates/UHSF21.00 Permit Register - tagged-direct-celltext.docx"
        ),
        "waste_register": Path(
            "templates/UHSF50.0 Register of Waste Removal - tagged.docx"
        ),
        "site_check_register": Path("templates/UHSF19.1 Daily-Weekly Checklist - tagged.docx"),
        "weekly_site_check": Path("templates/UHSF19.1 Daily-Weekly Checklist - tagged.docx"),
        "site_induction": Path("templates/UHSF16.01_Template.docx"),
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

SITE_CHECK_WEEKDAY_KEYS: Tuple[str, ...] = (
    "mon",
    "tue",
    "wed",
    "thu",
    "fri",
    "sat",
    "sun",
)
SITE_CHECK_WEEKDAY_LABELS: Dict[str, str] = {
    "mon": "Mon",
    "tue": "Tue",
    "wed": "Wed",
    "thu": "Thu",
    "fri": "Fri",
    "sat": "Sat",
    "sun": "Sun",
}
WEEKLY_SITE_CHECK_DAY_KEYS: Tuple[str, ...] = SITE_CHECK_WEEKDAY_KEYS + ("weekly",)
WEEKLY_SITE_CHECK_DAY_LABELS: Dict[str, str] = {
    **SITE_CHECK_WEEKDAY_LABELS,
    "weekly": "Weekly",
}
SITE_CHECK_TEMPLATE_ROW_COUNT = 7
SITE_CHECK_REQUIRED_TEMPLATE_PLACEHOLDERS: FrozenSet[str] = frozenset(
    {"site_name", "week_commencing", "checked_by", "checked_at"}
    | {
        f"site_check_{index}_{day_key}"
        for index in range(1, SITE_CHECK_TEMPLATE_ROW_COUNT + 1)
        for day_key in SITE_CHECK_WEEKDAY_KEYS
    }
)
WEEKLY_SITE_CHECK_ROW_COUNT = 31
WEEKLY_SITE_CHECK_REQUIRED_TEMPLATE_PLACEHOLDERS: FrozenSet[str] = frozenset(
    {"week_commencing", "checked_by"}
    | {
        f"{day_key}_{row_number}"
        for row_number in range(1, WEEKLY_SITE_CHECK_ROW_COUNT + 1)
        for day_key in WEEKLY_SITE_CHECK_DAY_KEYS
    }
    | {f"initials_{day_key}" for day_key in SITE_CHECK_WEEKDAY_KEYS}
    | {f"time_{day_key}" for day_key in SITE_CHECK_WEEKDAY_KEYS}
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


def _format_person_name(value: str) -> str:
    """Return a clean operative name reduced to first and last parts."""

    cleaned_value = _normalise_optional_text(value, "person_name")
    if not cleaned_value:
        return ""

    name_parts = cleaned_value.split()
    if len(name_parts) <= 2:
        return " ".join(name_parts)
    return f"{name_parts[0]} {name_parts[-1]}"


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


def _coerce_week_commencing(value: date, field_name: str = "week_commencing") -> date:
    """Normalise a date onto the Monday of its calendar week."""

    resolved_date = _coerce_date(value, field_name)
    return resolved_date - timedelta(days=resolved_date.weekday())


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


def _normalise_optional_bool(
    value: Optional[bool],
    field_name: str,
) -> Optional[bool]:
    """Accept booleans or ``None`` for partially completed weekly grids."""

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    raise TypeError(f"{field_name} must be a boolean or None.")


def _normalise_day_results(
    value: Mapping[str, Optional[bool]],
    field_name: str = "day_results",
) -> Dict[str, Optional[bool]]:
    """Normalise weekly checklist values onto the Mon-Sun key set."""

    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping of weekday keys to booleans.")

    normalised_results: Dict[str, Optional[bool]] = {
        day_key: None for day_key in SITE_CHECK_WEEKDAY_KEYS
    }
    for key, result in value.items():
        normalised_key = _require_text(str(key), field_name).strip().lower()
        if normalised_key not in normalised_results:
            raise ValueError(
                f"{field_name} contains unsupported weekday key {normalised_key!r}."
            )
        normalised_results[normalised_key] = _normalise_optional_bool(
            result,
            f"{field_name}[{normalised_key}]",
        )
    return normalised_results


def _normalise_weekly_site_check_values(
    value: Mapping[str, Optional[bool]],
    field_name: str,
) -> Dict[str, Optional[bool]]:
    """Normalise Mon-Sun-Weekly values for the tagged File 2 grid."""

    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping of checklist column keys.")

    normalised_values: Dict[str, Optional[bool]] = {
        day_key: None for day_key in WEEKLY_SITE_CHECK_DAY_KEYS
    }
    for key, result in value.items():
        normalised_key = _require_text(str(key), field_name).strip().lower()
        if normalised_key not in normalised_values:
            raise ValueError(
                f"{field_name} contains unsupported checklist key {normalised_key!r}."
            )
        normalised_values[normalised_key] = _normalise_optional_bool(
            result,
            f"{field_name}[{normalised_key}]",
        )
    return normalised_values


def _normalise_text_mapping(
    value: Mapping[str, Optional[str]],
    *,
    allowed_keys: Tuple[str, ...],
    field_name: str,
) -> Dict[str, str]:
    """Normalise a keyed string mapping and preserve blank values."""

    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping of strings.")

    normalised_values: Dict[str, str] = {key: "" for key in allowed_keys}
    for key, item_value in value.items():
        normalised_key = _require_text(str(key), field_name).strip().lower()
        if normalised_key not in normalised_values:
            raise ValueError(f"{field_name} contains unsupported key {normalised_key!r}.")
        normalised_values[normalised_key] = _normalise_optional_text(
            item_value,
            f"{field_name}[{normalised_key}]",
        )
    return normalised_values


def _coerce_weekly_site_check_day_key(
    value: str,
    field_name: str = "active_day_key",
) -> str:
    """Validate the current active checklist day."""

    resolved_value = _require_text(value, field_name).strip().lower()
    if resolved_value not in SITE_CHECK_WEEKDAY_KEYS:
        raise ValueError(
            f"{field_name} must be one of {', '.join(SITE_CHECK_WEEKDAY_KEYS)}."
        )
    return resolved_value


def _tick_cross_symbol(value: Optional[bool]) -> str:
    """Render one checklist cell using printable tick/cross characters."""

    if value is None:
        return ""
    return "✓" if value else "✗"


def _weekly_checklist_symbol(value: Optional[bool]) -> str:
    """Render one UHSF19.1 cell using the required heavy tick/cross symbols."""

    if value is None:
        return ""
    return "✔" if value else "✘"


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
        """Return the key used to aggregate attendance by worker and day."""

        return (self.date.isoformat(), self.workerName.strip().casefold())

    def row_signature(self) -> Tuple[str, str, str, str, str, float]:
        """Return the exact row signature used to drop repeated raw rows."""

        return (
            self.date.isoformat(),
            self.company.casefold(),
            self.workerName.casefold(),
            self.timeIn.strftime("%H:%M:%S"),
            self.timeOut.strftime("%H:%M:%S"),
            round(self.totalHours, 6),
        )

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


@dataclass(frozen=True)
class SiteWorker:
    """One unique worker/company roster entry surfaced from KPI JSON exports."""

    company: str
    worker_name: str
    last_on_site_date: date
    induction_status: str = "Verified (Paper Record)"

    def __post_init__(self) -> None:
        object.__setattr__(self, "company", _require_text(self.company, "company"))
        object.__setattr__(
            self,
            "worker_name",
            _require_text(self.worker_name, "worker_name"),
        )
        object.__setattr__(
            self,
            "last_on_site_date",
            _coerce_date(self.last_on_site_date, "last_on_site_date"),
        )
        object.__setattr__(
            self,
            "induction_status",
            _require_text(self.induction_status, "induction_status"),
        )

    def roster_key(self) -> Tuple[str, str]:
        """Return the dedupe key used by the live contractor roster."""

        return (self.company.casefold(), self.worker_name.casefold())

    @classmethod
    def from_kpi_row(cls, row: Mapping[str, Any]) -> "SiteWorker":
        """Map one KPI export row into a roster entry."""

        if not isinstance(row, Mapping):
            raise TypeError("KPI worker row must be a mapping.")
        return cls(
            company=row["company"],
            worker_name=row["workerName"],
            last_on_site_date=row["date"],
        )


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

    def get_record(
        self,
        record_date: date,
        worker_name: str,
    ) -> Optional[SiteAttendanceRecord]:
        """Return the matching worker/day record when it exists."""

        comparison_key = (
            _coerce_date(record_date, "record_date").isoformat(),
            _require_text(worker_name, "worker_name").casefold(),
        )
        for attendance_record in self.attendance_records:
            if attendance_record.duplicate_key() == comparison_key:
                return attendance_record
        return None

    def add_attendance_record(self, record: SiteAttendanceRecord) -> bool:
        """Append a record unless the date/worker combination already exists."""

        if self.has_record(record.date, record.workerName):
            return False
        self.attendance_records.append(record)
        return True

    def upsert_attendance_record(self, record: SiteAttendanceRecord) -> bool:
        """Insert or replace one worker/day aggregate in the register."""

        existing_record = self.get_record(record.date, record.workerName)
        if existing_record is None:
            self.attendance_records.append(record)
            return True

        existing_record.company = record.company
        existing_record.timeIn = record.timeIn
        existing_record.timeOut = record.timeOut
        existing_record.totalHours = record.totalHours
        return False

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
class PlantAssetDocument(BaseDocument):
    """File 2 plant and equipment asset captured from hire paperwork."""

    _register_document_type: ClassVar[bool] = True

    hire_num: str
    description: str
    company: str
    phone: str
    on_hire: date
    hired_by: str
    serial: str = ""
    inspection: str = ""
    source_reference: str = ""
    purchase_order: str = ""

    document_type: ClassVar[str] = "plant_asset"
    document_name: ClassVar[str] = "Plant Asset"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_2
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "hire_num",
            "description",
            "company",
            "phone",
            "on_hire",
            "hired_by",
            "serial",
            "inspection",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.hire_num = _require_text(self.hire_num, "hire_num")
        self.description = _require_text(self.description, "description")
        self.company = _require_text(self.company, "company")
        self.phone = _normalise_optional_text(self.phone, "phone")
        self.on_hire = _coerce_date(self.on_hire, "on_hire")
        self.hired_by = _require_text(self.hired_by, "hired_by")
        self.serial = _normalise_optional_text(self.serial, "serial")
        self.inspection = _normalise_optional_text(self.inspection, "inspection")
        self.source_reference = _normalise_optional_text(
            self.source_reference,
            "source_reference",
        )
        self.purchase_order = _normalise_optional_text(
            self.purchase_order,
            "purchase_order",
        )

    @property
    def is_pending(self) -> bool:
        """Return True when the asset still needs site receipt details."""

        return self.status == DocumentStatus.DRAFT or not self.serial

    def inspection_due_date(self) -> Optional[date]:
        """Return the next due date parsed from the inspection string when present."""

        lowered_inspection = self.inspection.casefold()
        keyword_patterns = (
            r"next\s+due\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
            r"due\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        )
        for pattern in keyword_patterns:
            match = re.search(pattern, lowered_inspection, flags=re.IGNORECASE)
            if match is None:
                continue
            return _coerce_date(match.group(1), "inspection_due_date")

        date_matches = re.findall(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", self.inspection)
        if len(date_matches) >= 2:
            return _coerce_date(date_matches[-1], "inspection_due_date")
        return None

    def inspection_requires_attention(
        self,
        *,
        on_date: Optional[date] = None,
        within_days: int = 7,
    ) -> bool:
        """Return True when the LOLER/inspection due date is expired or imminent."""

        if within_days < 0:
            raise ValueError("within_days must be zero or greater.")

        due_date = self.inspection_due_date()
        if due_date is None:
            return False

        effective_date = on_date or date.today()
        return due_date <= (effective_date + timedelta(days=within_days))

    def to_template_context(self) -> Dict[str, str]:
        """Flatten plant data into template placeholders."""

        context = super().to_template_context()
        context["on_hire"] = self.on_hire.strftime("%d/%m/%y")
        context["in_file"] = "Yes" if self.source_reference else ""
        return context

    def get_repository_metadata(self) -> Dict[str, str]:
        """Expose stable source references for deduping scanned plant assets."""

        metadata: Dict[str, str] = {}
        if self.source_reference:
            metadata["reference_number"] = self.source_reference
        return metadata

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "PlantAssetDocument":
        """Rehydrate one plant asset from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["on_hire"] = _coerce_date(payload["on_hire"], "on_hire")
        return cls(**payload)


@dataclass(frozen=True)
class WeeklySiteCheckRowDefinition:
    """One checklist row as defined by the official UHSF19.1 template."""

    row_number: int
    section: str
    prompt: str

    def __post_init__(self) -> None:
        if not isinstance(self.row_number, int):
            raise TypeError("row_number must be an integer.")
        if self.row_number < 1 or self.row_number > WEEKLY_SITE_CHECK_ROW_COUNT:
            raise ValueError(
                f"row_number must be between 1 and {WEEKLY_SITE_CHECK_ROW_COUNT}."
            )
        object.__setattr__(self, "section", _require_text(self.section, "section"))
        object.__setattr__(self, "prompt", _require_text(self.prompt, "prompt"))


@dataclass
class WeeklySiteCheckRowState:
    """The stored tick/cross state for one File 2 template row."""

    row_number: int
    values: Dict[str, Optional[bool]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.row_number, int):
            raise TypeError("row_number must be an integer.")
        if self.row_number < 1 or self.row_number > WEEKLY_SITE_CHECK_ROW_COUNT:
            raise ValueError(
                f"row_number must be between 1 and {WEEKLY_SITE_CHECK_ROW_COUNT}."
            )
        self.values = _normalise_weekly_site_check_values(
            self.values,
            f"values for row {self.row_number}",
        )

    def get_value(self, day_key: str) -> Optional[bool]:
        """Return one cell value from the stored grid."""

        resolved_day_key = _require_text(day_key, "day_key").strip().lower()
        if resolved_day_key not in WEEKLY_SITE_CHECK_DAY_KEYS:
            raise ValueError(
                f"day_key must be one of {', '.join(WEEKLY_SITE_CHECK_DAY_KEYS)}."
            )
        return self.values.get(resolved_day_key)

    def set_value(self, day_key: str, value: Optional[bool]) -> None:
        """Update one cell value on the stored grid."""

        resolved_day_key = _require_text(day_key, "day_key").strip().lower()
        if resolved_day_key not in WEEKLY_SITE_CHECK_DAY_KEYS:
            raise ValueError(
                f"day_key must be one of {', '.join(WEEKLY_SITE_CHECK_DAY_KEYS)}."
            )
        self.values[resolved_day_key] = _normalise_optional_bool(value, "value")

    def to_template_context(self) -> Dict[str, str]:
        """Map one row onto the official tagged placeholders."""

        return {
            f"{day_key}_{self.row_number}": _weekly_checklist_symbol(
                self.values.get(day_key)
            )
            for day_key in WEEKLY_SITE_CHECK_DAY_KEYS
        }

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "WeeklySiteCheckRowState":
        """Rehydrate one weekly site-check row from storage."""

        return cls(
            row_number=int(data["row_number"]),
            values=dict(data.get("values", {})),
        )


@dataclass
class WeeklySiteCheck(BaseDocument):
    """Matrix-backed File 2 checklist aligned to the official tagged template."""

    _register_document_type: ClassVar[bool] = True

    week_commencing: date
    checked_at: datetime
    checked_by: str
    active_day_key: str
    row_states: List[WeeklySiteCheckRowState] = field(default_factory=list)
    daily_initials: Dict[str, str] = field(default_factory=dict)
    daily_time_markers: Dict[str, str] = field(default_factory=dict)
    overall_safe_to_start: bool = False

    document_type: ClassVar[str] = "weekly_site_check"
    document_name: ClassVar[str] = "Weekly Site Check"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_2
    required_template_placeholders: ClassVar[FrozenSet[str]] = (
        WEEKLY_SITE_CHECK_REQUIRED_TEMPLATE_PLACEHOLDERS
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.week_commencing = _coerce_week_commencing(
            self.week_commencing,
            "week_commencing",
        )
        self.checked_at = _coerce_datetime(self.checked_at, "checked_at")
        self.checked_by = _require_text(self.checked_by, "checked_by")
        self.active_day_key = _coerce_weekly_site_check_day_key(self.active_day_key)
        self.row_states = self._normalise_row_states(self.row_states)
        self.daily_initials = _normalise_text_mapping(
            self.daily_initials,
            allowed_keys=SITE_CHECK_WEEKDAY_KEYS,
            field_name="daily_initials",
        )
        self.daily_time_markers = _normalise_text_mapping(
            self.daily_time_markers,
            allowed_keys=SITE_CHECK_WEEKDAY_KEYS,
            field_name="daily_time_markers",
        )
        self.overall_safe_to_start = _require_bool(
            self.overall_safe_to_start,
            "overall_safe_to_start",
        )

    def _normalise_row_states(
        self,
        value: List[WeeklySiteCheckRowState],
    ) -> List[WeeklySiteCheckRowState]:
        """Ensure the matrix contains one row state for each template row."""

        if not isinstance(value, list):
            raise TypeError("row_states must be a list.")

        row_lookup: Dict[int, WeeklySiteCheckRowState] = {}
        for row_state in value:
            resolved_row_state = (
                row_state
                if isinstance(row_state, WeeklySiteCheckRowState)
                else WeeklySiteCheckRowState.from_storage_dict(row_state)
            )
            row_lookup[resolved_row_state.row_number] = resolved_row_state

        for row_number in range(1, WEEKLY_SITE_CHECK_ROW_COUNT + 1):
            row_lookup.setdefault(
                row_number,
                WeeklySiteCheckRowState(row_number=row_number),
            )

        return [row_lookup[row_number] for row_number in range(1, WEEKLY_SITE_CHECK_ROW_COUNT + 1)]

    def get_row_state(self, row_number: int) -> WeeklySiteCheckRowState:
        """Return one matrix row by template number."""

        for row_state in self.row_states:
            if row_state.row_number == row_number:
                return row_state
        raise KeyError(f"Row {row_number} is not present in this weekly site check.")

    def day_values(self, day_key: str) -> List[Optional[bool]]:
        """Return the values for one active day across all template rows."""

        resolved_day_key = (
            _coerce_weekly_site_check_day_key(day_key)
            if day_key in SITE_CHECK_WEEKDAY_KEYS
            else _require_text(day_key, "day_key").strip().lower()
        )
        if resolved_day_key not in WEEKLY_SITE_CHECK_DAY_KEYS:
            raise ValueError(
                f"day_key must be one of {', '.join(WEEKLY_SITE_CHECK_DAY_KEYS)}."
            )
        return [
            row_state.get_value(resolved_day_key)
            for row_state in self.row_states
        ]

    def to_template_context(self) -> Dict[str, str]:
        """Map the full 31x8 grid plus sign-off values into template tags."""

        context = super().to_template_context()
        context["week_commencing"] = self.week_commencing.strftime("%d/%m/%Y")
        context["checked_at"] = self.checked_at.strftime("%d/%m/%Y %H:%M")

        for row_state in self.row_states:
            context.update(row_state.to_template_context())

        for day_key in SITE_CHECK_WEEKDAY_KEYS:
            context[f"initials_{day_key}"] = self.daily_initials.get(day_key, "")
            context[f"time_{day_key}"] = self.daily_time_markers.get(day_key, "")
        return context

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "WeeklySiteCheck":
        """Rehydrate a weekly site-check document from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["week_commencing"] = _coerce_week_commencing(
            payload.get("week_commencing", payload["created_at"]),
            "week_commencing",
        )
        payload["checked_at"] = _coerce_datetime(payload["checked_at"], "checked_at")
        payload["checked_by"] = (
            _normalise_optional_text(payload.get("checked_by"), "checked_by")
            or "Ceri Edwards"
        )
        payload["active_day_key"] = (
            _normalise_optional_text(payload.get("active_day_key"), "active_day_key")
            or "mon"
        )
        payload["row_states"] = [
            WeeklySiteCheckRowState.from_storage_dict(row_state)
            for row_state in payload.get("row_states", [])
        ]
        payload["daily_initials"] = dict(payload.get("daily_initials", {}))
        payload["daily_time_markers"] = dict(payload.get("daily_time_markers", {}))
        return cls(**payload)


@dataclass
class SiteCheckItem:
    """One line item from the File 2 daily/weekly site check sheet."""

    check_name: str
    frequency: str
    passed: bool = False
    notes: str = ""
    day_results: Dict[str, Optional[bool]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.check_name = _require_text(self.check_name, "check_name")
        self.frequency = _require_text(self.frequency, "frequency")
        self.passed = _require_bool(self.passed, "passed")
        self.notes = _normalise_optional_text(self.notes, "notes")
        self.day_results = _normalise_day_results(self.day_results)
        populated_results = [
            result for result in self.day_results.values() if result is not None
        ]
        if populated_results:
            self.passed = all(populated_results)

    def to_template_context(
        self,
        index: int,
        *,
        fallback_day_key: Optional[str] = None,
    ) -> Dict[str, str]:
        """Expose indexed placeholders for template rows."""

        context = {
            f"site_check_{index}_name": self.check_name,
            f"site_check_{index}_passed": _stringify_for_template(self.passed),
            f"site_check_{index}_frequency": self.frequency,
            f"site_check_{index}_notes": self.notes,
        }
        resolved_day_results = dict(self.day_results)
        if fallback_day_key and all(
            value is None for value in resolved_day_results.values()
        ):
            resolved_day_results[fallback_day_key] = self.passed
        for day_key in SITE_CHECK_WEEKDAY_KEYS:
            context[f"site_check_{index}_{day_key}"] = _tick_cross_symbol(
                resolved_day_results.get(day_key)
            )
        return context

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "SiteCheckItem":
        """Rehydrate a site-check item from storage."""

        return cls(
            check_name=data["check_name"],
            passed=bool(data.get("passed", False)),
            frequency=data["frequency"],
            notes=data.get("notes", ""),
            day_results=data.get("day_results", {}),
        )


@dataclass
class SiteCheckRegister(BaseDocument):
    """Timestamped File 2 tick sheet for the start of shift."""

    _register_document_type: ClassVar[bool] = True

    week_commencing: date
    checked_at: datetime
    checked_by: str
    check_items: List[SiteCheckItem] = field(default_factory=list)
    overall_safe_to_start: bool = False

    document_type: ClassVar[str] = "site_check_register"
    document_name: ClassVar[str] = "Daily/Weekly Site Checks"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_2
    required_template_placeholders: ClassVar[FrozenSet[str]] = (
        SITE_CHECK_REQUIRED_TEMPLATE_PLACEHOLDERS
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.week_commencing = _coerce_week_commencing(
            self.week_commencing,
            "week_commencing",
        )
        self.checked_at = _coerce_datetime(self.checked_at, "checked_at")
        self.checked_by = _require_text(self.checked_by, "checked_by")
        self.check_items = [
            item if isinstance(item, SiteCheckItem) else SiteCheckItem.from_storage_dict(item)
            for item in self.check_items
        ]
        self.overall_safe_to_start = _require_bool(
            self.overall_safe_to_start,
            "overall_safe_to_start",
        )

    def to_template_context(self) -> Dict[str, str]:
        """Flatten the site-check sheet into template placeholders."""

        context = super().to_template_context()
        context["week_commencing"] = self.week_commencing.strftime("%d/%m/%Y")
        context["checked_at"] = self.checked_at.strftime("%d/%m/%Y %H:%M")
        context["overall_safe_to_start"] = _tick_cross_symbol(
            self.overall_safe_to_start
        )
        summary_lines = []
        checked_day_key = SITE_CHECK_WEEKDAY_KEYS[self.checked_at.weekday()]
        for index in range(1, SITE_CHECK_TEMPLATE_ROW_COUNT + 1):
            context.setdefault(f"site_check_{index}_name", "")
            context.setdefault(f"site_check_{index}_passed", "")
            context.setdefault(f"site_check_{index}_frequency", "")
            context.setdefault(f"site_check_{index}_notes", "")
            for day_key in SITE_CHECK_WEEKDAY_KEYS:
                context.setdefault(f"site_check_{index}_{day_key}", "")
        for index, item in enumerate(self.check_items, start=1):
            context.update(
                item.to_template_context(
                    index,
                    fallback_day_key=checked_day_key,
                )
            )
            summary_lines.append(
                (
                    f"{item.frequency} | {item.check_name} | "
                    + " ".join(
                        f"{SITE_CHECK_WEEKDAY_LABELS[day_key]} "
                        f"{context[f'site_check_{index}_{day_key}'] or '-'}"
                        for day_key in SITE_CHECK_WEEKDAY_KEYS
                    )
                    + (f" | {item.notes}" if item.notes else "")
                )
            )
        context["site_check_count"] = str(len(self.check_items))
        context["site_checks"] = "\n".join(summary_lines)
        return context

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "SiteCheckRegister":
        """Rehydrate a site-check sheet from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["week_commencing"] = _coerce_week_commencing(
            payload.get("week_commencing", payload["created_at"]),
            "week_commencing",
        )
        payload["checked_at"] = _coerce_datetime(payload["checked_at"], "checked_at")
        payload["check_items"] = [
            SiteCheckItem.from_storage_dict(item)
            for item in payload.get("check_items", [])
        ]
        return cls(**payload)


@dataclass
class RAMSDocument(BaseDocument):
    """File 3 RAMS document for contractor method statements."""

    _register_document_type: ClassVar[bool] = True

    contractor_name: str
    activity_description: str
    approval_date: date
    reference: str = ""
    version: str = ""
    manufacturer: str = ""
    review_date: Optional[date] = None
    assessor_name: str = "Ceri Edwards"
    manager_name: str = "Ceri Edwards"
    manager_position: str = "Project Manager"

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
        self.reference = (
            _normalise_optional_text(self.reference, "reference") or self.doc_id
        )
        self.version = _normalise_optional_text(self.version, "version") or "1.0"
        self.manufacturer = _normalise_optional_text(
            self.manufacturer,
            "manufacturer",
        )
        self.review_date = (
            self.approval_date
            if self.review_date is None
            else _coerce_date(self.review_date, "review_date")
        )
        self.assessor_name = (
            _normalise_optional_text(self.assessor_name, "assessor_name")
            or "Ceri Edwards"
        )
        self.manager_name = (
            _normalise_optional_text(self.manager_name, "manager_name")
            or "Ceri Edwards"
        )
        self.manager_position = (
            _normalise_optional_text(self.manager_position, "manager_position")
            or "Project Manager"
        )

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

        return {
            "contractor_name": self.contractor_name,
            "reference_number": self.reference,
        }

    def to_template_context(self) -> Dict[str, str]:
        """Flatten RAMS data into template placeholders."""

        context = super().to_template_context()
        context.update(
            {
                "reference": self.reference,
                "version": self.version,
                "manufacturer": self.manufacturer,
                "review_date": _stringify_for_template(self.review_date),
                "assessor_name": self.assessor_name,
                "manager_name": self.manager_name,
                "manager_position": self.manager_position,
            }
        )
        return context

    def as_safety_asset(self) -> "SafetyAsset":
        """Return the unified safety inventory projection for this RAMS document."""

        return SafetyAsset.from_rams(self)

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "RAMSDocument":
        """Rehydrate RAMS data from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["approval_date"] = _coerce_date(payload["approval_date"], "approval_date")
        if payload.get("review_date"):
            payload["review_date"] = _coerce_date(payload["review_date"], "review_date")
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
    reference: str = ""
    version: str = ""
    manufacturer: str = ""
    review_date: Optional[date] = None
    supplier_name: str = ""
    intended_use: str = ""
    assessor_name: str = "Ceri Edwards"
    manager_name: str = "Ceri Edwards"
    manager_position: str = "Project Manager"

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
        self.reference = (
            _normalise_optional_text(self.reference, "reference") or self.doc_id
        )
        self.version = _normalise_optional_text(self.version, "version") or "1.0"
        self.supplier_name = _normalise_optional_text(
            self.supplier_name,
            "supplier_name",
        )
        self.manufacturer = (
            _normalise_optional_text(self.manufacturer, "manufacturer")
            or self.supplier_name
        )
        self.review_date = (
            self.created_at.date()
            if self.review_date is None
            else _coerce_date(self.review_date, "review_date")
        )
        self.intended_use = _normalise_optional_text(self.intended_use, "intended_use")
        self.assessor_name = (
            _normalise_optional_text(self.assessor_name, "assessor_name")
            or "Ceri Edwards"
        )
        self.manager_name = (
            _normalise_optional_text(self.manager_name, "manager_name")
            or "Ceri Edwards"
        )
        self.manager_position = (
            _normalise_optional_text(self.manager_position, "manager_position")
            or "Project Manager"
        )

    def to_template_context(self) -> Dict[str, str]:
        """Flatten COSHH data into template placeholders."""

        context = super().to_template_context()
        context.update(
            {
                "reference": self.reference,
                "version": self.version,
                "manufacturer": self.manufacturer,
                "review_date": _stringify_for_template(self.review_date),
                "supplier_name": self.supplier_name,
                "intended_use": self.intended_use,
                "assessor_name": self.assessor_name,
                "manager_name": self.manager_name,
                "manager_position": self.manager_position,
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

        return {
            "contractor_name": self.contractor_name,
            "reference_number": self.reference,
        }

    def as_safety_asset(self) -> "SafetyAsset":
        """Return the unified safety inventory projection for this COSHH document."""

        return SafetyAsset.from_coshh(self)

    @classmethod
    def from_storage_dict(cls, data: Mapping[str, Any]) -> "COSHHDocument":
        """Rehydrate COSHH data from storage."""

        payload = cls._deserialize_base_fields(data)
        payload["hazard_pictograms"] = list(payload.get("hazard_pictograms", []))
        payload["ppe_required"] = list(payload.get("ppe_required", []))
        if payload.get("review_date"):
            payload["review_date"] = _coerce_date(payload["review_date"], "review_date")
        return cls(**payload)


@dataclass(frozen=True)
class SafetyAsset:
    """Unified File 3 safety row derived from either RAMS or COSHH."""

    asset_type: str
    reference: str
    version: str = ""
    manufacturer: str = ""
    review_date: Optional[date] = None
    title: str = ""
    company: str = ""
    status: str = ""
    document_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "asset_type", _require_text(self.asset_type, "asset_type"))
        object.__setattr__(self, "reference", _require_text(self.reference, "reference"))
        object.__setattr__(
            self,
            "version",
            _normalise_optional_text(self.version, "version"),
        )
        object.__setattr__(
            self,
            "manufacturer",
            _normalise_optional_text(self.manufacturer, "manufacturer"),
        )
        object.__setattr__(
            self,
            "title",
            _normalise_optional_text(self.title, "title"),
        )
        object.__setattr__(
            self,
            "company",
            _normalise_optional_text(self.company, "company"),
        )
        object.__setattr__(
            self,
            "status",
            _normalise_optional_text(self.status, "status"),
        )
        object.__setattr__(
            self,
            "document_id",
            _normalise_optional_text(self.document_id, "document_id"),
        )
        if self.review_date is not None:
            object.__setattr__(
                self,
                "review_date",
                _coerce_date(self.review_date, "review_date"),
            )

    @classmethod
    def from_rams(cls, document: "RAMSDocument") -> "SafetyAsset":
        """Project one RAMS document into the shared File 3 safety inventory shape."""

        return cls(
            asset_type="RAMS",
            reference=document.reference,
            version=document.version,
            manufacturer=document.manufacturer,
            review_date=document.review_date,
            title=document.activity_description,
            company=document.contractor_name,
            status=document.status.label,
            document_id=document.doc_id,
        )

    @classmethod
    def from_coshh(cls, document: "COSHHDocument") -> "SafetyAsset":
        """Project one COSHH document into the shared File 3 safety inventory shape."""

        return cls(
            asset_type="COSHH",
            reference=document.reference,
            version=document.version,
            manufacturer=document.manufacturer,
            review_date=document.review_date,
            title=document.substance_name,
            company=document.contractor_name,
            status=document.status.label,
            document_id=document.doc_id,
        )


@dataclass
class InductionDocument(BaseDocument):
    """File 3 site induction record linked to a RAMS document."""

    _register_document_type: ClassVar[bool] = True

    contractor_name: str
    individual_name: str
    linked_rams_doc_id: str = ""
    home_address: str = ""
    contact_number: str = ""
    occupation: str = ""
    emergency_contact: str = ""
    emergency_tel: str = ""
    medical: str = ""
    cscs_number: str = ""
    first_aider: bool = False
    fire_warden: bool = False
    supervisor: bool = False
    smsts: bool = False
    signature_image_path: str = ""
    completed_document_path: str = ""

    document_type: ClassVar[str] = "induction"
    document_name: ClassVar[str] = "Site Induction Log"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_3
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "site_name",
            "date",
            "full_name",
            "company",
            "home_address",
            "contact_number",
            "occupation",
            "emergency_contact",
            "emergency_tel",
            "medical",
            "cscs_no",
            "first_aider",
            "fire_warden",
            "supervisor",
            "smsts",
            "inductor_name_date",
            "inductor_title",
            "signature_image",
        }
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        self.contractor_name = _require_text(self.contractor_name, "contractor_name")
        self.individual_name = _require_text(self.individual_name, "individual_name")
        self.linked_rams_doc_id = _normalise_optional_text(
            self.linked_rams_doc_id,
            "linked_rams_doc_id",
        )
        self.home_address = _normalise_optional_text(self.home_address, "home_address")
        self.contact_number = _normalise_optional_text(
            self.contact_number,
            "contact_number",
        )
        self.occupation = _normalise_optional_text(self.occupation, "occupation")
        self.emergency_contact = _normalise_optional_text(
            self.emergency_contact,
            "emergency_contact",
        )
        self.emergency_tel = _normalise_optional_text(
            self.emergency_tel,
            "emergency_tel",
        )
        self.medical = _normalise_optional_text(self.medical, "medical")
        self.cscs_number = _normalise_optional_text(self.cscs_number, "cscs_number")
        self.first_aider = _require_bool(self.first_aider, "first_aider")
        self.fire_warden = _require_bool(self.fire_warden, "fire_warden")
        self.supervisor = _require_bool(self.supervisor, "supervisor")
        self.smsts = _require_bool(self.smsts, "smsts")
        self.signature_image_path = _normalise_optional_text(
            self.signature_image_path,
            "signature_image_path",
        )
        self.completed_document_path = _normalise_optional_text(
            self.completed_document_path,
            "completed_document_path",
        )

    def get_repository_metadata(self) -> Dict[str, str]:
        """Expose contractor and linkage metadata for repository search."""

        metadata = {"contractor_name": self.contractor_name}
        if self.linked_rams_doc_id:
            metadata["linked_document_id"] = self.linked_rams_doc_id
        return metadata

    def to_template_context(self) -> Dict[str, str]:
        """Flatten induction data into the tagged UHSF16.01 placeholders."""

        context = super().to_template_context()
        role_ticks = {
            "first_aider": self.first_aider,
            "fire_warden": self.fire_warden,
            "supervisor": self.supervisor,
            "smsts": self.smsts,
        }
        context.update(
            {
                "full_name": self.individual_name,
                "individual_name": self.individual_name,
                "company": self.contractor_name,
                "contractor_name": self.contractor_name,
                "home_address": self.home_address,
                "contact_number": self.contact_number,
                "contact_tel": self.contact_number,
                "company_name": self.contractor_name,
                "occupation": self.occupation,
                "emergency_contact": self.emergency_contact,
                "emergency_tel": self.emergency_tel,
                "medical": self.medical,
                "cscs": self.cscs_number,
                "cscs_no": self.cscs_number,
                "cscs_number": self.cscs_number,
                "linked_rams_doc_id": self.linked_rams_doc_id,
                "signature_image_path": self.signature_image_path,
                "completed_document_path": self.completed_document_path,
            }
        )
        for key, enabled in role_ticks.items():
            context[key] = "✔" if enabled else ""
            context[f"{key}_yes"] = "✔" if enabled else ""
            context[f"{key}_no"] = "" if enabled else "✔"
        return context

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
    vehicle_registration: str = ""
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
        self.vehicle_registration = _normalise_optional_text(
            self.vehicle_registration,
            "vehicle_registration",
        )
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

    def to_template_context(self) -> Dict[str, str]:
        """Expose WTN aliases used by the File 1 waste register template."""

        context = super().to_template_context()
        context["reg_no"] = self.vehicle_registration
        return context

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
        payload["vehicle_registration"] = payload.get("vehicle_registration", "")
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
    ladder_stabilisation_confirmed: bool = True
    inspection_records: List[LadderInspectionRecord] = field(default_factory=list)
    worker_name: str = ""
    worker_company: str = ""
    briefing_name: str = ""
    manager_name: str = ""
    manager_position: str = ""
    issued_date: Optional[date] = None

    document_type: ClassVar[str] = "ladder_permit"
    document_name: ClassVar[str] = "UHSF21.09 Step Ladders Permit"
    file_group: ClassVar[FileGroup] = FileGroup.FILE_4
    required_template_placeholders: ClassVar[FrozenSet[str]] = frozenset(
        {
            "company_name",
            "contractor_name",
            "date_issued",
            "insp_comments",
            "insp_date",
            "insp_feet",
            "insp_name",
            "insp_ok",
            "insp_rungs",
            "insp_stiles",
            "job_number",
            "ladder_id",
            "manager_name",
            "manager_signature",
            "permit_number",
            "q10_no",
            "q10_yes",
            "q11_no",
            "q11_yes",
            "q1_no",
            "q1_yes",
            "q2_no",
            "q2_yes",
            "q3_no",
            "q3_yes",
            "q4_no",
            "q4_yes",
            "q5_no",
            "q5_yes",
            "q6_no",
            "q6_yes",
            "q7_no",
            "q7_yes",
            "q8_no",
            "q8_yes",
            "q9_no",
            "q9_yes",
            "site_name",
            "supervisor_name",
            "task_description",
        }
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
        self.ladder_stabilisation_confirmed = _require_bool(
            self.ladder_stabilisation_confirmed,
            "ladder_stabilisation_confirmed",
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
        self.worker_name = _normalise_optional_text(self.worker_name, "worker_name")
        self.worker_company = _normalise_optional_text(
            self.worker_company,
            "worker_company",
        )
        self.briefing_name = _normalise_optional_text(self.briefing_name, "briefing_name")
        self.manager_name = _normalise_optional_text(self.manager_name, "manager_name")
        self.manager_position = _normalise_optional_text(
            self.manager_position,
            "manager_position",
        )
        if self.issued_date is None:
            self.issued_date = self.valid_from_date
        else:
            self.issued_date = _coerce_date(self.issued_date, "issued_date")

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
        inspection_record = self.inspection_records[0] if self.inspection_records else None
        operative_name = _format_person_name(self.worker_name)
        resolved_manager_name = self.manager_name or self.competent_supervisor_name
        resolved_briefing_name = self.briefing_name or resolved_manager_name
        resolved_manager_position = self.manager_position or "Project Manager"
        precaution_answers = {
            1: self.safer_alternative_eliminated,
            2: self.task_specific_rams_prepared_and_approved,
            3: self.personnel_briefed_and_understand_task,
            4: self.competent_supervisor_appointed,
            5: self.operatives_suitably_trained,
            6: self.ladder_length_suitable,
            7: self.conforms_to_bs_class_a,
            8: self.three_points_of_contact_maintained,
            9: self.harness_worn_and_secured_above_head_height,
            10: self.ladder_stabilisation_confirmed,
            11: self.equipment_inspected_for_defects,
        }
        context.update(
            {
                "status_label": self.status.label,
                "document_name": self.document_name,
                "file_group": self.file_group.value,
                "ladder_stabilisation_method_label": self.ladder_stabilisation_method.label,
                "inspection_record_count": str(len(self.inspection_records)),
                "worker_name": operative_name,
                "operative_name": operative_name,
                "op_name": operative_name,
                "company": self.worker_company,
                "company_name": self.worker_company,
                "contractor_name": operative_name,
                "issue_date": _stringify_for_template(self.issued_date),
                "date_issued": _stringify_for_template(self.issued_date),
                "today_date": _stringify_for_template(self.issued_date),
                "job_number": self.project_number,
                "task_description": self.description_of_work,
                "supervisor_name": self.competent_supervisor_name,
                "briefing_name": resolved_briefing_name,
                "issue_name": resolved_briefing_name,
                "auth_name": resolved_manager_name,
                "issue_position": resolved_manager_position,
                "auth_position": resolved_manager_position,
                "ladder_id": self.location_of_work,
                "manager_name": resolved_manager_name,
                "manager_position": resolved_manager_position,
                "manager_signature": "",
                "insp_date": (
                    _stringify_for_template(inspection_record.inspection_date)
                    if inspection_record is not None
                    else ""
                ),
                "insp_name": (
                    self.competent_supervisor_name
                    if self.competent_supervisor_name
                    else (
                        inspection_record.inspected_by
                        if inspection_record is not None
                        else ""
                    )
                ),
                "insp_rungs": (
                    "✔" if inspection_record is not None and inspection_record.rungs_ok else
                    ("✘" if inspection_record is not None else "")
                ),
                "insp_stiles": (
                    "✔" if inspection_record is not None and inspection_record.stiles_ok else
                    ("✘" if inspection_record is not None else "")
                ),
                "insp_feet": (
                    "✔" if inspection_record is not None and inspection_record.feet_ok else
                    ("✘" if inspection_record is not None else "")
                ),
                "insp_comments": (
                    inspection_record.comments_or_action_taken
                    if inspection_record is not None
                    else ""
                ),
                "insp_ok": (
                    "✔" if inspection_record is not None and inspection_record.ok_to_use else
                    ("✘" if inspection_record is not None else "")
                ),
            }
        )
        for question_number, answer in precaution_answers.items():
            context[f"q{question_number}_yes"] = "✔" if answer else ""
            context[f"q{question_number}_no"] = "" if answer else "✔"

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
        if payload.get("issued_date") is not None:
            payload["issued_date"] = _coerce_date(payload["issued_date"], "issued_date")
        payload["inspection_records"] = [
            LadderInspectionRecord.from_storage_dict(record)
            for record in payload.get("inspection_records", [])
        ]
        return cls(**payload)


Permit = PermitDocument
