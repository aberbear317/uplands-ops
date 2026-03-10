"""Unit tests for the Uplands document hierarchy and repository flow."""

import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
import tempfile
import unittest

from docx import Document

import uplands_site_command_centre.config as app_config
from uplands_site_command_centre import file_and_index_all
from uplands_site_command_centre.permits import (
    COSHHDocument,
    CarrierComplianceDocument,
    CarrierComplianceDocumentType,
    COMMON_CONSTRUCTION_EWC_CODES,
    ComplianceAlertStatus,
    DocumentNotFoundError,
    DocumentRepository,
    DocumentStatus,
    FileGroup,
    IncidentLogDocument,
    IncidentType,
    InductionDocument,
    IngestionEngine,
    LadderPermit,
    LadderStabilisationMethod,
    RAMSDocument,
    SiteAttendanceRegister,
    SiteAttendanceRecord,
    TemplateRegistry,
    TemplateManager,
    TemplateValidationError,
    ValidationError,
    VerificationStatus,
    WasteRegister,
    WasteTransferNoteDocument,
    check_carrier_compliance,
)


class LadderPermitTests(unittest.TestCase):
    def build_permit(self) -> LadderPermit:
        return LadderPermit(
            doc_id="LP-001",
            site_name="Uplands - Newport",
            created_at=datetime(2026, 3, 10, 8, 0),
            status=DocumentStatus.DRAFT,
            permit_number="UHSF21.09-001",
            project_name="Newport Retail Fit-Out",
            project_number="UP-24020",
            location_of_work="Plant room access",
            description_of_work="Lamp replacement above service riser.",
            valid_from_date=date(2026, 3, 10),
            valid_from_time=time(8, 30),
            valid_to_date=date(2026, 3, 10),
            valid_to_time=time(17, 0),
            safer_alternative_eliminated=True,
            task_specific_rams_prepared_and_approved=True,
            personnel_briefed_and_understand_task=True,
            competent_supervisor_appointed=True,
            competent_supervisor_name="J. Evans",
            operatives_suitably_trained=True,
            ladder_length_suitable=True,
            conforms_to_bs_class_a=True,
            three_points_of_contact_maintained=True,
            harness_worn_and_secured_above_head_height=False,
            ladder_stabilisation_method=LadderStabilisationMethod.TIED_AT_BOTTOM,
            equipment_inspected_for_defects=True,
        )

    def test_file_path_uses_four_file_structure(self) -> None:
        permit = self.build_permit()

        self.assertEqual(
            permit.get_file_path(),
            Path("File 4") / "uplands-newport" / "ladder_permit" / "LP-001",
        )
        self.assertEqual(permit.file_group, FileGroup.FILE_4)

    def test_add_inspection_record_appends_typed_entry(self) -> None:
        permit = self.build_permit()

        record = permit.add_inspection_record(
            inspection_date=date(2026, 3, 10),
            inspected_by="J. Evans",
            rungs_ok=True,
            stiles_ok=True,
            feet_ok=False,
            comments_or_action_taken="Replace worn foot before use.",
            ok_to_use=False,
        )

        self.assertEqual(len(permit.inspection_records), 1)
        self.assertEqual(record.inspected_by, "J. Evans")
        self.assertFalse(record.ok_to_use)

    def test_supervisor_name_is_required_when_supervisor_is_appointed(self) -> None:
        with self.assertRaises(ValueError):
            LadderPermit(
                doc_id="LP-002",
                site_name="Uplands - Swansea",
                created_at=datetime(2026, 3, 10, 8, 0),
                status=DocumentStatus.DRAFT,
                permit_number="UHSF21.09-002",
                project_name="Swansea Maintenance",
                project_number="UP-24021",
                location_of_work="Rear stair core",
                description_of_work="Visual snagging check.",
                valid_from_date=date(2026, 3, 10),
                valid_from_time=time(8, 30),
                valid_to_date=date(2026, 3, 10),
                valid_to_time=time(17, 0),
                safer_alternative_eliminated=True,
                task_specific_rams_prepared_and_approved=True,
                personnel_briefed_and_understand_task=True,
                competent_supervisor_appointed=True,
                competent_supervisor_name="",
                operatives_suitably_trained=True,
                ladder_length_suitable=True,
                conforms_to_bs_class_a=True,
                three_points_of_contact_maintained=True,
                harness_worn_and_secured_above_head_height=False,
                ladder_stabilisation_method=LadderStabilisationMethod.FOOTED,
                equipment_inspected_for_defects=True,
            )


class IncidentLogDocumentTests(unittest.TestCase):
    def build_incident(self, *, status: DocumentStatus = DocumentStatus.ACTIVE) -> IncidentLogDocument:
        return IncidentLogDocument(
            doc_id="INC-001",
            site_name="Uplands - Newport",
            created_at=datetime(2026, 3, 10, 11, 0),
            status=status,
            incident_type=IncidentType.NEAR_MISS,
            location="North compound access route",
            description="Operative slipped on wet boarding but avoided injury.",
            witness_list=["A. Hughes", "J. Evans"],
        )

    def test_incident_log_template_context_includes_witnesses(self) -> None:
        incident = self.build_incident()

        context = incident.to_template_context()

        self.assertEqual(context["incident_type_label"], "Near Miss")
        self.assertEqual(context["witness_count"], "2")
        self.assertEqual(context["witness_1"], "A. Hughes")
        self.assertIn("A. Hughes\nJ. Evans", context["witness_list"])


class WasteDocumentTests(unittest.TestCase):
    def build_wtn(
        self,
        *,
        doc_id: str = "WTN-001",
        wtn_number: str = "UWTN-001",
        note_date: date = date(2026, 3, 10),
        ewc_code: str = "17 09 04",
        quantity_tonnes: float = 3.25,
        carrier_name: str = "Green Haul Ltd",
    ) -> WasteTransferNoteDocument:
        return WasteTransferNoteDocument(
            doc_id=doc_id,
            site_name="Uplands - Newport",
            created_at=datetime(2026, 3, 10, 14, 0),
            status=DocumentStatus.ACTIVE,
            wtn_number=wtn_number,
            date=note_date,
            waste_description="Mixed construction and demolition waste.",
            ewc_code=ewc_code,
            quantity_tonnes=quantity_tonnes,
            carrier_name=carrier_name,
            destination_facility="Newport Recovery Centre",
        )

    def test_wtn_uses_file_1_path_and_metadata(self) -> None:
        wtn = self.build_wtn()

        self.assertEqual(wtn.file_group, FileGroup.FILE_1)
        self.assertEqual(
            wtn.get_file_path(),
            Path("File 1") / "uplands-newport" / "waste_transfer_note" / "WTN-001",
        )
        self.assertEqual(
            wtn.get_repository_metadata(),
            {
                "wtn_number": "UWTN-001",
                "reference_number": "UWTN-001",
                "carrier_name": "Green Haul Ltd",
            },
        )

    def test_wtn_validation_rejects_unknown_ewc_code(self) -> None:
        self.assertIn("17 09 04", COMMON_CONSTRUCTION_EWC_CODES)

        with self.assertRaises(ValidationError):
            self.build_wtn(ewc_code="99 99 99")

    def test_waste_register_summarises_monthly_tonnage(self) -> None:
        register = WasteRegister(
            doc_id="WR-001",
            site_name="Uplands - Newport",
            created_at=datetime(2026, 3, 10, 15, 0),
            status=DocumentStatus.ACTIVE,
            waste_transfer_notes=[
                self.build_wtn(
                    doc_id="WTN-001",
                    wtn_number="UWTN-001",
                    note_date=date(2026, 3, 2),
                    ewc_code="17 09 04",
                    quantity_tonnes=3.25,
                ),
                self.build_wtn(
                    doc_id="WTN-002",
                    wtn_number="UWTN-002",
                    note_date=date(2026, 3, 18),
                    ewc_code="17 02 01",
                    quantity_tonnes=1.75,
                ),
                self.build_wtn(
                    doc_id="WTN-003",
                    wtn_number="UWTN-003",
                    note_date=date(2026, 2, 28),
                    ewc_code="17 09 04",
                    quantity_tonnes=2.0,
                ),
            ],
        )

        summary = register.get_monthly_tonnage_summary(month=3, year=2026)

        self.assertEqual(summary["note_count"], 2)
        self.assertEqual(summary["total_tonnage"], 5.0)
        self.assertEqual(summary["by_ewc_code"], {"17 02 01": 1.75, "17 09 04": 3.25})


class CarrierComplianceDocumentTests(unittest.TestCase):
    def build_compliance_document(
        self,
        *,
        doc_id: str = "CCD-001",
        carrier_name: str = "Abucs",
        carrier_document_type: CarrierComplianceDocumentType = CarrierComplianceDocumentType.LICENCE,
        reference_number: str = "WCL-001",
        expiry_date: date = date(2026, 5, 1),
    ) -> CarrierComplianceDocument:
        return CarrierComplianceDocument(
            doc_id=doc_id,
            site_name="Uplands - Newport",
            created_at=datetime(2026, 3, 10, 10, 0),
            status=DocumentStatus.ACTIVE,
            carrier_name=carrier_name,
            carrier_document_type=carrier_document_type,
            reference_number=reference_number,
            expiry_date=expiry_date,
        )

    def test_carrier_compliance_document_uses_file_1_and_reference_index(self) -> None:
        compliance_document = self.build_compliance_document()

        self.assertEqual(compliance_document.file_group, FileGroup.FILE_1)
        self.assertEqual(
            compliance_document.get_repository_metadata(),
            {"carrier_name": "Abucs", "reference_number": "WCL-001"},
        )
        self.assertEqual(
            compliance_document.to_template_context()["document_type"],
            "Licence",
        )

    def test_check_carrier_compliance_flags_expiring_and_missing_documents(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            repository.save(
                self.build_compliance_document(
                    doc_id="CCD-010",
                    carrier_name="Abucs",
                    carrier_document_type=CarrierComplianceDocumentType.LICENCE,
                    reference_number="WCL-010",
                    expiry_date=date(2026, 4, 1),
                )
            )
            repository.save(
                WasteTransferNoteDocument(
                    doc_id="WTN-CARRIER-1",
                    site_name="Uplands - Newport",
                    created_at=datetime(2026, 3, 10, 11, 0),
                    status=DocumentStatus.ACTIVE,
                    wtn_number="UWTN-CARRIER-1",
                    date=date(2026, 3, 10),
                    waste_description="Mixed construction waste.",
                    ewc_code="17 09 04",
                    quantity_tonnes=2.5,
                    carrier_name="Abucs",
                    destination_facility="Newport Recovery Centre",
                )
            )

            findings = check_carrier_compliance(repository, on_date=date(2026, 3, 10))
            findings_by_type = {
                finding.carrier_document_type: finding for finding in findings
            }

            self.assertEqual(len(findings), 2)
            self.assertEqual(
                findings_by_type[CarrierComplianceDocumentType.LICENCE].status,
                ComplianceAlertStatus.CRITICAL,
            )
            self.assertFalse(
                findings_by_type[CarrierComplianceDocumentType.LICENCE].blocking
            )
            self.assertEqual(
                findings_by_type[CarrierComplianceDocumentType.INSURANCE].status,
                ComplianceAlertStatus.CRITICAL,
            )
            self.assertTrue(
                findings_by_type[CarrierComplianceDocumentType.INSURANCE].blocking
            )


class File3DocumentTests(unittest.TestCase):
    def build_rams(self) -> RAMSDocument:
        return RAMSDocument(
            doc_id="RAMS-001",
            site_name="Uplands - Bristol",
            created_at=datetime(2026, 3, 10, 9, 0),
            status=DocumentStatus.ACTIVE,
            contractor_name="Acme Interiors",
            activity_description="Partition wall installation and ceiling fixings.",
            approval_date=date(2025, 3, 1),
        )

    def build_coshh(self) -> COSHHDocument:
        return COSHHDocument(
            doc_id="COSHH-001",
            site_name="Uplands - Bristol",
            created_at=datetime(2026, 3, 10, 9, 30),
            status=DocumentStatus.ACTIVE,
            contractor_name="Acme Interiors",
            substance_name="Solvent Adhesive",
            hazard_pictograms=["Flammable", "Irritant"],
            ppe_required=["Gloves", "Eye Protection"],
            emergency_first_aid="Move to fresh air and flush eyes with clean water.",
        )

    def build_induction(self) -> InductionDocument:
        return InductionDocument(
            doc_id="IND-001",
            site_name="Uplands - Bristol",
            created_at=datetime(2026, 3, 10, 10, 0),
            status=DocumentStatus.ACTIVE,
            contractor_name="Acme Interiors",
            individual_name="S. Carter",
            linked_rams_doc_id="RAMS-001",
        )

    def test_rams_expiry_defaults_to_twelve_month_window(self) -> None:
        rams = self.build_rams()

        self.assertTrue(rams.has_expired(on_date=date(2026, 3, 10)))
        self.assertFalse(
            rams.has_expired(
                on_date=date(2026, 3, 10),
                max_age=timedelta(days=400),
            )
        )

    def test_coshh_template_context_flattens_lists(self) -> None:
        coshh = self.build_coshh()

        context = coshh.to_template_context()

        self.assertEqual(context["substance_name"], "Solvent Adhesive")
        self.assertEqual(context["hazard_pictogram_1"], "Flammable")
        self.assertEqual(context["ppe_required_2"], "Eye Protection")
        self.assertIn("Gloves\nEye Protection", context["ppe_required"])

    def test_induction_document_keeps_rams_link(self) -> None:
        induction = self.build_induction()

        self.assertEqual(induction.linked_rams_doc_id, "RAMS-001")
        self.assertEqual(induction.file_group, FileGroup.FILE_3)


class SiteAttendanceRecordTests(unittest.TestCase):
    def test_site_attendance_record_maps_json_fields(self) -> None:
        record = SiteAttendanceRecord.from_json_row(
            {
                "date": "2026-03-10",
                "company": "Acme Interiors",
                "workerName": "S. Carter",
                "timeIn": "07:30",
                "timeOut": "16:00",
                "totalHours": 8.5,
            }
        )

        self.assertEqual(record.workerName, "S. Carter")
        self.assertEqual(record.totalHours, 8.5)

    def test_site_attendance_record_accepts_kpi_export_date_format(self) -> None:
        record = SiteAttendanceRecord.from_json_row(
            {
                "date": "10/03/2026",
                "company": "Acme Interiors",
                "workerName": "S. Carter",
                "timeIn": "07:30",
                "timeOut": "16:00",
                "totalHours": "8.5",
            }
        )

        self.assertEqual(record.date, date(2026, 3, 10))
        self.assertEqual(record.totalHours, 8.5)


class DocumentRepositoryTests(unittest.TestCase):
    def build_permit(self, *, status: DocumentStatus = DocumentStatus.ACTIVE) -> LadderPermit:
        permit = LadderPermit(
            doc_id="LP-003",
            site_name="Uplands - Caerphilly",
            created_at=datetime(2026, 3, 10, 9, 15),
            status=status,
            permit_number="UHSF21.09-003",
            project_name="Caerphilly Upgrade",
            project_number="UP-24022",
            location_of_work="Warehouse loading area",
            description_of_work="Camera alignment at mezzanine edge.",
            valid_from_date=date(2026, 3, 10),
            valid_from_time=time(9, 30),
            valid_to_date=date(2026, 3, 10),
            valid_to_time=time(15, 0),
            safer_alternative_eliminated=True,
            task_specific_rams_prepared_and_approved=True,
            personnel_briefed_and_understand_task=True,
            competent_supervisor_appointed=True,
            competent_supervisor_name="L. Morgan",
            operatives_suitably_trained=True,
            ladder_length_suitable=True,
            conforms_to_bs_class_a=True,
            three_points_of_contact_maintained=True,
            harness_worn_and_secured_above_head_height=False,
            ladder_stabilisation_method=LadderStabilisationMethod.FOOTED,
            equipment_inspected_for_defects=True,
        )
        permit.add_inspection_record(
            inspection_date=date(2026, 3, 10),
            inspected_by="L. Morgan",
            rungs_ok=True,
            stiles_ok=True,
            feet_ok=True,
            comments_or_action_taken="Serviceable.",
        )
        return permit

    def build_incident(
        self,
        *,
        doc_id: str = "INC-002",
        site_name: str = "Uplands - Caerphilly",
        status: DocumentStatus = DocumentStatus.ACTIVE,
    ) -> IncidentLogDocument:
        return IncidentLogDocument(
            doc_id=doc_id,
            site_name=site_name,
            created_at=datetime(2026, 3, 10, 12, 0),
            status=status,
            incident_type=IncidentType.PROPERTY_DAMAGE,
            location="Delivery bay shutter",
            description="Forklift clipped shutter guide rail.",
            witness_list=["L. Morgan"],
        )

    def build_rams(
        self,
        *,
        doc_id: str = "RAMS-100",
        contractor_name: str = "Acme Interiors",
    ) -> RAMSDocument:
        return RAMSDocument(
            doc_id=doc_id,
            site_name="Uplands - Caerphilly",
            created_at=datetime(2026, 3, 10, 7, 30),
            status=DocumentStatus.ACTIVE,
            contractor_name=contractor_name,
            activity_description="Dry-lining and suspended ceiling installation.",
            approval_date=date(2026, 2, 28),
        )

    def build_coshh(
        self,
        *,
        doc_id: str = "COSHH-100",
        contractor_name: str = "Acme Interiors",
    ) -> COSHHDocument:
        return COSHHDocument(
            doc_id=doc_id,
            site_name="Uplands - Caerphilly",
            created_at=datetime(2026, 3, 10, 8, 15),
            status=DocumentStatus.ACTIVE,
            contractor_name=contractor_name,
            substance_name="Foam Cleaner",
            hazard_pictograms=["Compressed Gas"],
            ppe_required=["Gloves"],
            emergency_first_aid="Rinse affected skin and seek medical advice if irritation persists.",
        )

    def build_induction(
        self,
        *,
        doc_id: str = "IND-100",
        contractor_name: str = "Acme Interiors",
        linked_rams_doc_id: str = "RAMS-100",
    ) -> InductionDocument:
        return InductionDocument(
            doc_id=doc_id,
            site_name="Uplands - Caerphilly",
            created_at=datetime(2026, 3, 10, 8, 45),
            status=DocumentStatus.ACTIVE,
            contractor_name=contractor_name,
            individual_name="P. Lewis",
            linked_rams_doc_id=linked_rams_doc_id,
        )

    def build_wtn(
        self,
        *,
        doc_id: str = "WTN-100",
        wtn_number: str = "UWTN-100",
        carrier_name: str = "Green Haul Ltd",
    ) -> WasteTransferNoteDocument:
        return WasteTransferNoteDocument(
            doc_id=doc_id,
            site_name="Uplands - Caerphilly",
            created_at=datetime(2026, 3, 10, 11, 15),
            status=DocumentStatus.ACTIVE,
            wtn_number=wtn_number,
            date=date(2026, 3, 10),
            waste_description="Mixed construction and demolition waste.",
            ewc_code="17 09 04",
            quantity_tonnes=4.5,
            carrier_name=carrier_name,
            destination_facility="Caerphilly Recycling Hub",
        )

    def build_carrier_compliance_document(
        self,
        *,
        doc_id: str = "CCD-100",
        carrier_name: str = "Abucs",
        carrier_document_type: CarrierComplianceDocumentType = CarrierComplianceDocumentType.LICENCE,
        reference_number: str = "REF-100",
        expiry_date: date = date(2026, 6, 30),
    ) -> CarrierComplianceDocument:
        return CarrierComplianceDocument(
            doc_id=doc_id,
            site_name="Uplands - Caerphilly",
            created_at=datetime(2026, 3, 10, 9, 0),
            status=DocumentStatus.ACTIVE,
            carrier_name=carrier_name,
            carrier_document_type=carrier_document_type,
            reference_number=reference_number,
            expiry_date=expiry_date,
        )

    def test_round_trip_with_sqlite_persists_inspection_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "documents.sqlite3"
            repository = DocumentRepository(database_path)
            repository.create_schema()

            permit = self.build_permit()
            repository.save(permit)
            loaded = repository.get("LP-003")

            self.assertIsInstance(loaded, LadderPermit)
            self.assertEqual(len(loaded.inspection_records), 1)
            self.assertEqual(
                loaded.inspection_records[0].comments_or_action_taken,
                "Serviceable.",
            )

    def test_repository_auto_discovers_incident_log_document(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            incident = self.build_incident()
            repository.save(incident)
            loaded = repository.get(incident.doc_id)

            self.assertIsInstance(loaded, IncidentLogDocument)
            self.assertEqual(loaded.file_group, FileGroup.FILE_1)
            self.assertEqual(loaded.document_type, "incident_log")

    def test_site_compliance_summary_groups_by_file_group(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            active_incident = self.build_incident(doc_id="INC-100", status=DocumentStatus.ACTIVE)
            draft_incident = self.build_incident(doc_id="INC-101", status=DocumentStatus.DRAFT)
            active_permit = self.build_permit(status=DocumentStatus.ACTIVE)

            repository.save(active_incident)
            repository.save(draft_incident)
            repository.save(active_permit)

            summary = repository.get_site_compliance_summary("Uplands - Caerphilly")

            self.assertEqual(summary["files"]["File 1"]["count"], 1)
            self.assertEqual(summary["files"]["File 1"]["total_count"], 2)
            self.assertEqual(
                summary["files"]["File 1"]["status_counts"],
                {"draft": 1, "active": 1, "archived": 0},
            )
            self.assertEqual(summary["files"]["File 4"]["count"], 1)
            self.assertEqual(
                summary["files"]["File 4"]["active_documents"][0]["document_type"],
                "ladder_permit",
            )

    def test_get_raises_for_missing_document(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            with self.assertRaises(DocumentNotFoundError):
                repository.get("missing")

    def test_search_by_contractor_name_returns_all_linked_file3_documents(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            rams = self.build_rams()
            coshh = self.build_coshh()
            induction = self.build_induction()
            other_rams = self.build_rams(
                doc_id="RAMS-101",
                contractor_name="Different Contractor Ltd",
            )

            repository.save(rams)
            repository.save(coshh)
            repository.save(induction)
            repository.save(other_rams)

            results = repository.search_by_contractor_name("acme interiors")

            self.assertEqual(len(results), 3)
            self.assertEqual(
                {document.document_type for document in results},
                {"rams", "coshh", "induction"},
            )
            self.assertEqual(
                {getattr(document, "contractor_name", "") for document in results},
                {"Acme Interiors"},
            )

    def test_search_by_wtn_number_and_carrier_name_returns_waste_transfer_notes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            waste_note = self.build_wtn()
            other_waste_note = self.build_wtn(
                doc_id="WTN-101",
                wtn_number="UWTN-101",
                carrier_name="Other Carrier Ltd",
            )

            repository.save(waste_note)
            repository.save(other_waste_note)

            by_number = repository.search_by_wtn_number("uwtn-100")
            by_carrier = repository.search_by_carrier_name("green haul ltd")

            self.assertEqual(len(by_number), 1)
            self.assertIsInstance(by_number[0], WasteTransferNoteDocument)
            self.assertEqual(by_number[0].wtn_number, "UWTN-100")

            self.assertEqual(len(by_carrier), 1)
            self.assertIsInstance(by_carrier[0], WasteTransferNoteDocument)
            self.assertEqual(by_carrier[0].carrier_name, "Green Haul Ltd")

    def test_waste_transfer_note_is_marked_unverified_when_carrier_docs_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            waste_note = self.build_wtn(carrier_name="Abucs")
            repository.save(waste_note)

            loaded = repository.get(waste_note.doc_id)

            self.assertIsInstance(loaded, WasteTransferNoteDocument)
            self.assertEqual(loaded.verification_status, VerificationStatus.UNVERIFIED)
            self.assertIn("missing or expired", loaded.verification_notes.lower())

    def test_waste_transfer_note_is_verified_when_carrier_docs_are_current(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            repository.save(
                self.build_carrier_compliance_document(
                    doc_id="CCD-200",
                    carrier_name="Abucs",
                    carrier_document_type=CarrierComplianceDocumentType.LICENCE,
                    reference_number="WCL-200",
                    expiry_date=date(2026, 12, 31),
                )
            )
            repository.save(
                self.build_carrier_compliance_document(
                    doc_id="CCD-201",
                    carrier_name="Abucs",
                    carrier_document_type=CarrierComplianceDocumentType.INSURANCE,
                    reference_number="LI-201",
                    expiry_date=date(2026, 12, 31),
                )
            )

            waste_note = self.build_wtn(carrier_name="Abucs")
            repository.save(waste_note)

            loaded = repository.get(waste_note.doc_id)

            self.assertIsInstance(loaded, WasteTransferNoteDocument)
            self.assertEqual(loaded.verification_status, VerificationStatus.VERIFIED)
            self.assertIn("valid", loaded.verification_notes.lower())

    def test_saving_carrier_docs_rechecks_existing_waste_transfer_notes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()

            waste_note = self.build_wtn(carrier_name="Abucs")
            repository.save(waste_note)
            self.assertEqual(
                repository.get(waste_note.doc_id).verification_status,
                VerificationStatus.UNVERIFIED,
            )

            repository.save(
                self.build_carrier_compliance_document(
                    doc_id="CCD-300",
                    carrier_name="Abucs",
                    carrier_document_type=CarrierComplianceDocumentType.LICENCE,
                    reference_number="WCL-300",
                    expiry_date=date(2026, 12, 31),
                )
            )
            self.assertEqual(
                repository.get(waste_note.doc_id).verification_status,
                VerificationStatus.UNVERIFIED,
            )

            repository.save(
                self.build_carrier_compliance_document(
                    doc_id="CCD-301",
                    carrier_name="Abucs",
                    carrier_document_type=CarrierComplianceDocumentType.INSURANCE,
                    reference_number="LI-301",
                    expiry_date=date(2026, 12, 31),
                )
            )
            self.assertEqual(
                repository.get(waste_note.doc_id).verification_status,
                VerificationStatus.VERIFIED,
            )


class IngestionEngineTests(unittest.TestCase):
    def test_ingestion_engine_creates_file2_register_and_skips_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()
            engine = IngestionEngine(repository)
            json_path = Path(temp_dir) / "attendance.json"

            with json_path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    [
                        {
                            "date": "2026-03-10",
                            "company": "Acme Interiors",
                            "workerName": "S. Carter",
                            "timeIn": "07:30",
                            "timeOut": "16:00",
                            "totalHours": 8.5,
                        },
                        {
                            "date": "2026-03-10",
                            "company": "Acme Interiors",
                            "workerName": "S. Carter",
                            "timeIn": "07:30",
                            "timeOut": "16:00",
                            "totalHours": 8.5,
                        },
                        {
                            "date": "2026-03-10",
                            "company": "Acme Interiors",
                            "workerName": "J. Evans",
                            "timeIn": "08:00",
                            "timeOut": "16:30",
                            "totalHours": 8.0,
                        },
                    ],
                    file_handle,
                )

            register = engine.ingest_site_attendance_json(
                json_path,
                site_name="Uplands - Cardiff",
            )

            self.assertIsInstance(register, SiteAttendanceRegister)
            self.assertEqual(register.file_group, FileGroup.FILE_2)
            self.assertEqual(len(register.attendance_records), 2)

            loaded = repository.get(register.doc_id)
            self.assertIsInstance(loaded, SiteAttendanceRegister)
            self.assertEqual(len(loaded.attendance_records), 2)

            second_pass = engine.ingest_site_attendance_json(
                json_path,
                site_name="Uplands - Cardiff",
            )
            self.assertEqual(len(second_pass.attendance_records), 2)

    def test_ingestion_engine_reads_nested_kpi_export_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()
            engine = IngestionEngine(repository)
            json_path = Path(temp_dir) / "attendance_export.json"

            with json_path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    {
                        "version": 1,
                        "settings": {"siteName": "Uplands - Cardiff"},
                        "extractedRows": {
                            "weekly": [
                                {
                                    "id": "weekly-1",
                                    "date": "10/03/2026",
                                    "company": "Acme Interiors",
                                    "workerName": "S. Carter",
                                    "timeIn": "07:30",
                                    "timeOut": "16:00",
                                    "totalHours": 8.5,
                                    "isSeniorManager": False,
                                }
                            ],
                            "eom": [
                                {
                                    "id": "eom-1",
                                    "date": "10/03/2026",
                                    "company": "Acme Interiors",
                                    "workerName": "S. Carter",
                                    "timeIn": "07:30",
                                    "timeOut": "16:00",
                                    "totalHours": 8.5,
                                    "isSeniorManager": False,
                                },
                                {
                                    "id": "eom-2",
                                    "date": "10/03/2026",
                                    "company": "Acme Interiors",
                                    "workerName": "J. Evans",
                                    "timeIn": "08:00",
                                    "timeOut": "16:30",
                                    "totalHours": 8.0,
                                    "isSeniorManager": False,
                                },
                            ],
                        },
                    },
                    file_handle,
                )

            register = engine.ingest_site_attendance_json(
                json_path,
                site_name="Uplands - Cardiff",
            )

            self.assertEqual(len(register.attendance_records), 2)
            self.assertEqual(
                {record.workerName for record in register.attendance_records},
                {"S. Carter", "J. Evans"},
            )
            self.assertEqual(register.site_name, "Uplands - Cardiff")


class WorkspaceFileIndexingTests(unittest.TestCase):
    def test_file_and_index_all_moves_supported_files_and_indexes_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            inbox = workspace_root / "ingest"
            waste_destination = workspace_root / "FILE_1_Environment" / "Waste_Notes"
            carrier_docs_destination = workspace_root / "FILE_1_Environment" / "Carrier_Docs"
            waste_reports_destination = workspace_root / "FILE_1_Environment" / "Waste_Reports"
            attendance_destination = workspace_root / "FILE_2_Registers" / "Attendance"
            database_path = workspace_root / "documents.sqlite3"
            inbox.mkdir(parents=True, exist_ok=True)

            pdf_path = inbox / "31194.PDF"
            pdf_path.write_bytes(b"%PDF-1.4\n%Uplands\n")

            carrier_pdf_path = inbox / "Abucs_Insurance_2026.pdf"
            carrier_pdf_path.write_bytes(b"%PDF-1.4\n%Carrier\n")

            waste_report_path = inbox / "March_Waste_Report.xlsx"
            waste_report_path.write_bytes(b"PK\x03\x04")

            json_path = inbox / "site-kpi-backup.json"
            with json_path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    {
                        "settings": {"siteName": "Uplands - Cardiff"},
                        "extractedRows": {
                            "weekly": [
                                {
                                    "date": "10/03/2026",
                                    "company": "Acme Interiors",
                                    "workerName": "S. Carter",
                                    "timeIn": "07:30",
                                    "timeOut": "16:00",
                                    "totalHours": 8.5,
                                }
                            ],
                            "eom": [],
                        },
                    },
                    file_handle,
                )

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.INBOX,
                app_config.WASTE_DESTINATION,
                app_config.CARRIER_DOCS_DESTINATION,
                app_config.WASTE_REPORTS_DESTINATION,
                app_config.ATTENDANCE_DESTINATION,
                app_config.DATABASE_PATH,
            )

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.INBOX = inbox
                app_config.WASTE_DESTINATION = waste_destination
                app_config.CARRIER_DOCS_DESTINATION = carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = attendance_destination
                app_config.DATABASE_PATH = database_path

                repository = DocumentRepository(database_path)
                filed_assets = file_and_index_all(repository)
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.INBOX,
                    app_config.WASTE_DESTINATION,
                    app_config.CARRIER_DOCS_DESTINATION,
                    app_config.WASTE_REPORTS_DESTINATION,
                    app_config.ATTENDANCE_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config

            moved_pdf_path = waste_destination / "31194.PDF"
            moved_carrier_pdf_path = carrier_docs_destination / "Abucs_Insurance_2026.pdf"
            moved_waste_report_path = waste_reports_destination / "March_Waste_Report.xlsx"
            moved_json_path = attendance_destination / "site-kpi-backup.json"

            self.assertFalse(pdf_path.exists())
            self.assertFalse(carrier_pdf_path.exists())
            self.assertFalse(waste_report_path.exists())
            self.assertFalse(json_path.exists())
            self.assertTrue(moved_pdf_path.exists())
            self.assertTrue(moved_carrier_pdf_path.exists())
            self.assertTrue(moved_waste_report_path.exists())
            self.assertTrue(moved_json_path.exists())
            self.assertEqual(
                {asset.file_category for asset in filed_assets},
                {
                    "abucs_pdf",
                    "carrier_doc_pdf",
                    "waste_report_excel",
                    "kpi_json",
                },
            )

            indexed_files = repository.list_indexed_files()
            self.assertEqual(len(indexed_files), 4)
            self.assertEqual(
                {record.file_path for record in indexed_files},
                {
                    moved_pdf_path.resolve(),
                    moved_carrier_pdf_path.resolve(),
                    moved_waste_report_path.resolve(),
                    moved_json_path.resolve(),
                },
            )

            attendance_registers = repository.list_documents(
                document_type=SiteAttendanceRegister.document_type
            )
            self.assertEqual(len(attendance_registers), 1)
            self.assertEqual(attendance_registers[0].site_name, "Uplands - Cardiff")
            self.assertEqual(
                repository.list_indexed_files(file_category="kpi_json")[0].related_doc_id,
                attendance_registers[0].doc_id,
            )



class TemplateManagerTests(unittest.TestCase):
    def build_permit(self) -> LadderPermit:
        permit = LadderPermit(
            doc_id="LP-004",
            site_name="Uplands - Bridgend",
            created_at=datetime(2026, 3, 10, 10, 0),
            status=DocumentStatus.DRAFT,
            permit_number="UHSF21.09-004",
            project_name="Bridgend Remedials",
            project_number="UP-24023",
            location_of_work="External stair tower",
            description_of_work="Bracket inspection at first-floor landing.",
            valid_from_date=date(2026, 3, 10),
            valid_from_time=time(10, 30),
            valid_to_date=date(2026, 3, 10),
            valid_to_time=time(16, 0),
            safer_alternative_eliminated=True,
            task_specific_rams_prepared_and_approved=True,
            personnel_briefed_and_understand_task=True,
            competent_supervisor_appointed=True,
            competent_supervisor_name="R. Davies",
            operatives_suitably_trained=True,
            ladder_length_suitable=True,
            conforms_to_bs_class_a=True,
            three_points_of_contact_maintained=False,
            harness_worn_and_secured_above_head_height=True,
            ladder_stabilisation_method=LadderStabilisationMethod.TIED_AT_TOP,
            equipment_inspected_for_defects=True,
        )
        permit.add_inspection_record(
            inspection_date=date(2026, 3, 10),
            inspected_by="R. Davies",
            rungs_ok=True,
            stiles_ok=True,
            feet_ok=True,
            comments_or_action_taken="Ready for use.",
        )
        return permit

    def build_incident(self) -> IncidentLogDocument:
        return IncidentLogDocument(
            doc_id="INC-300",
            site_name="Uplands - Bridgend",
            created_at=datetime(2026, 3, 10, 13, 0),
            status=DocumentStatus.ACTIVE,
            incident_type=IncidentType.ACCIDENT,
            location="Front reception",
            description="Minor hand injury during unloading.",
            witness_list=["R. Davies", "C. Thomas"],
        )

    def test_template_manager_replaces_placeholders_for_ladder_permit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "permit_template.docx"
            output_path = Path(temp_dir) / "permit_filled.docx"
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            document = Document()
            document.add_paragraph("Permit {{permit_number}} at {{site_name}}")
            document.add_paragraph("{{project_name}} / {{project_number}}")
            document.add_paragraph("{{location_of_work}}")
            document.add_paragraph("{{description_of_work}}")
            document.add_paragraph("{{valid_from_date}} {{valid_from_time}}")
            document.add_paragraph("{{valid_to_date}} {{valid_to_time}}")
            document.add_paragraph("{{competent_supervisor_name}}")
            document.add_paragraph("{{safer_alternative_eliminated}}")
            document.add_paragraph("{{task_specific_rams_prepared_and_approved}}")
            document.add_paragraph("{{personnel_briefed_and_understand_task}}")
            document.add_paragraph("{{three_points_of_contact_maintained}}")
            document.add_paragraph("{{ladder_stabilisation_method_label}}")
            document.add_paragraph("{{inspection_register}}")
            table = document.add_table(rows=1, cols=2)
            table.cell(0, 0).text = "{{inspection_1_inspected_by}}"
            table.cell(0, 1).text = "{{inspection_1_ok_to_use}}"
            document.save(template_path)

            try:
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path
                manager = TemplateManager(self.build_permit())
                manager.render(output_path)
            finally:
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            rendered = Document(output_path)
            self.assertIn("UHSF21.09-004", rendered.paragraphs[0].text)
            self.assertEqual(rendered.tables[0].cell(0, 0).text, "R. Davies")
            self.assertEqual(rendered.tables[0].cell(0, 1).text, "Yes")

    def test_template_manager_replaces_placeholders_for_incident_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "incident_template.docx"
            output_path = Path(temp_dir) / "incident_filled.docx"
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            document = Document()
            document.add_paragraph(
                "{{site_name}}"
            )
            document.add_paragraph(
                "{{incident_type_label}} at {{location}} witnessed by {{witness_1}}"
            )
            document.add_paragraph("{{description}}")
            document.add_paragraph("{{witness_list}}")
            document.save(template_path)

            try:
                TemplateRegistry.TEMPLATE_PATHS["incident_log"] = template_path
                manager = TemplateManager(self.build_incident())
                manager.render(output_path)
            finally:
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            rendered = Document(output_path)
            self.assertIn("Uplands - Bridgend", rendered.paragraphs[0].text)
            self.assertIn("Accident at Front reception", rendered.paragraphs[1].text)
            self.assertIn("R. Davies", rendered.paragraphs[3].text)
            self.assertIn("C. Thomas", rendered.paragraphs[3].text)

    def test_template_manager_rejects_template_missing_required_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "invalid_template.docx"
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            document = Document()
            document.add_paragraph("Permit {{permit_number}} only")
            document.save(template_path)

            try:
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path
                manager = TemplateManager(self.build_permit())
                with self.assertRaises(TemplateValidationError):
                    manager.render(Path(temp_dir) / "should_not_exist.docx")
            finally:
                TemplateRegistry.TEMPLATE_PATHS = original_registry


if __name__ == "__main__":
    unittest.main()
