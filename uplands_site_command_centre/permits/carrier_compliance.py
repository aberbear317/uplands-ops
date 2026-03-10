"""Carrier compliance monitoring and waste gatekeeper helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Sequence, Set

from uplands_site_command_centre.permits.models import (
    CarrierComplianceDocument,
    CarrierComplianceDocumentType,
    ComplianceAlertStatus,
    DocumentStatus,
    VerificationStatus,
    WasteRegister,
    WasteTransferNoteDocument,
)
from uplands_site_command_centre.permits.repository import DocumentRepository


@dataclass(frozen=True)
class CarrierComplianceFinding:
    """One compliance finding for a carrier document requirement."""

    carrier_name: str
    carrier_document_type: CarrierComplianceDocumentType
    status: ComplianceAlertStatus
    reference_number: Optional[str]
    expiry_date: Optional[date]
    days_to_expiry: Optional[int]
    reason: str
    blocking: bool


def check_carrier_compliance(
    repository: DocumentRepository,
    *,
    on_date: Optional[date] = None,
    warning_window_days: int = 30,
) -> List[CarrierComplianceFinding]:
    """Scan all known carriers and flag missing, expired, or near-expiry records."""

    if warning_window_days < 0:
        raise ValueError("warning_window_days must be zero or greater.")

    effective_date = on_date or date.today()
    compliance_documents = _load_active_carrier_compliance_documents(repository)
    carrier_names = _collect_carrier_names(repository, compliance_documents)

    findings: List[CarrierComplianceFinding] = []
    for carrier_name in sorted(carrier_names):
        findings.extend(
            _build_findings_for_carrier(
                carrier_name,
                compliance_documents,
                on_date=effective_date,
                warning_window_days=warning_window_days,
            )
        )
    return findings


def evaluate_waste_transfer_note_verification(
    waste_transfer_note: WasteTransferNoteDocument,
    repository: DocumentRepository,
    *,
    on_date: Optional[date] = None,
) -> VerificationStatus:
    """Apply the carrier licence/insurance gatekeeper to one waste transfer note."""

    effective_date = on_date or date.today()
    findings = _build_findings_for_carrier(
        waste_transfer_note.carrier_name,
        _load_active_carrier_compliance_documents(repository),
        on_date=effective_date,
        warning_window_days=30,
    )

    blocking_findings = [finding for finding in findings if finding.blocking]
    if blocking_findings:
        missing_or_expired = ", ".join(
            finding.carrier_document_type.label for finding in blocking_findings
        )
        waste_transfer_note.set_verification_status(
            VerificationStatus.UNVERIFIED,
            f"Carrier compliance missing or expired: {missing_or_expired}.",
        )
        return waste_transfer_note.verification_status

    waste_transfer_note.set_verification_status(
        VerificationStatus.VERIFIED,
        "Carrier licence and insurance are valid.",
    )
    return waste_transfer_note.verification_status


def _collect_carrier_names(
    repository: DocumentRepository,
    compliance_documents: Sequence[CarrierComplianceDocument],
) -> Set[str]:
    """Return every carrier name referenced by compliance or waste records."""

    carrier_names = {document.carrier_name for document in compliance_documents}
    carrier_names.update(
        waste_note.carrier_name
        for waste_note in repository.list_documents(
            document_type=WasteTransferNoteDocument.document_type
        )
        if isinstance(waste_note, WasteTransferNoteDocument)
    )
    for waste_register in repository.list_documents(document_type=WasteRegister.document_type):
        if not isinstance(waste_register, WasteRegister):
            continue
        carrier_names.update(
            waste_note.carrier_name for waste_note in waste_register.waste_transfer_notes
        )
    return carrier_names


def _load_active_carrier_compliance_documents(
    repository: DocumentRepository,
) -> List[CarrierComplianceDocument]:
    """Load active carrier compliance documents from the repository."""

    return [
        document
        for document in repository.list_documents(
            document_type=CarrierComplianceDocument.document_type
        )
        if isinstance(document, CarrierComplianceDocument)
        and document.status == DocumentStatus.ACTIVE
    ]


def _build_findings_for_carrier(
    carrier_name: str,
    compliance_documents: Sequence[CarrierComplianceDocument],
    *,
    on_date: date,
    warning_window_days: int,
) -> List[CarrierComplianceFinding]:
    """Build findings for the required licence and insurance records."""

    findings: List[CarrierComplianceFinding] = []
    selected_documents = _select_current_documents(
        carrier_name,
        compliance_documents,
    )

    for carrier_document_type in CarrierComplianceDocumentType:
        document = selected_documents.get(carrier_document_type)
        if document is None:
            findings.append(
                CarrierComplianceFinding(
                    carrier_name=carrier_name,
                    carrier_document_type=carrier_document_type,
                    status=ComplianceAlertStatus.CRITICAL,
                    reference_number=None,
                    expiry_date=None,
                    days_to_expiry=None,
                    reason="Missing required compliance document.",
                    blocking=True,
                )
            )
            continue

        days_to_expiry = (document.expiry_date - on_date).days
        if days_to_expiry < 0:
            findings.append(
                CarrierComplianceFinding(
                    carrier_name=carrier_name,
                    carrier_document_type=carrier_document_type,
                    status=ComplianceAlertStatus.CRITICAL,
                    reference_number=document.reference_number,
                    expiry_date=document.expiry_date,
                    days_to_expiry=days_to_expiry,
                    reason="Compliance document has expired.",
                    blocking=True,
                )
            )
            continue

        if days_to_expiry <= warning_window_days:
            findings.append(
                CarrierComplianceFinding(
                    carrier_name=carrier_name,
                    carrier_document_type=carrier_document_type,
                    status=ComplianceAlertStatus.CRITICAL,
                    reference_number=document.reference_number,
                    expiry_date=document.expiry_date,
                    days_to_expiry=days_to_expiry,
                    reason="Compliance document is expiring soon.",
                    blocking=False,
                )
            )
            continue

        findings.append(
            CarrierComplianceFinding(
                carrier_name=carrier_name,
                carrier_document_type=carrier_document_type,
                status=ComplianceAlertStatus.OK,
                reference_number=document.reference_number,
                expiry_date=document.expiry_date,
                days_to_expiry=days_to_expiry,
                reason="Compliance document is valid.",
                blocking=False,
            )
        )

    return findings


def _select_current_documents(
    carrier_name: str,
    compliance_documents: Sequence[CarrierComplianceDocument],
) -> Dict[CarrierComplianceDocumentType, CarrierComplianceDocument]:
    """Select the best active record per required document type for a carrier."""

    selected_documents: Dict[
        CarrierComplianceDocumentType,
        CarrierComplianceDocument,
    ] = {}
    matching_documents = [
        document
        for document in compliance_documents
        if document.carrier_name.casefold() == carrier_name.casefold()
    ]

    for carrier_document_type in CarrierComplianceDocumentType:
        typed_documents = [
            document
            for document in matching_documents
            if document.carrier_document_type == carrier_document_type
        ]
        if not typed_documents:
            continue
        selected_documents[carrier_document_type] = max(
            typed_documents,
            key=lambda document: (document.expiry_date, document.created_at),
        )

    return selected_documents
