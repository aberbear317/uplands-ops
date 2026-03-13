"""Unit tests for the Uplands document hierarchy and repository flow."""

import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from docx import Document
import fitz
import numpy as np

import uplands_site_command_centre.config as app_config
import uplands_site_command_centre.workspace as workspace_module
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
    PlantAssetDocument,
    RAMSDocument,
    SITE_CHECK_WEEKDAY_KEYS,
    SiteCheckItem,
    SiteCheckRegister,
    SiteAttendanceRegister,
    SiteAttendanceRecord,
    SiteWorker,
    TemplateRegistry,
    TemplateManager,
    TemplateValidationError,
    ValidationError,
    VerificationStatus,
    WeeklySiteCheck,
    WeeklySiteCheckRowState,
    WasteRegister,
    WasteTransferNoteDocument,
    check_carrier_compliance,
)
from uplands_site_command_centre.workspace import (
    build_site_worker_roster,
    check_site_inductions,
    create_ladder_permit_draft,
    create_site_induction_document,
    create_site_check_checklist_draft,
    create_weekly_site_check_checklist_draft,
    extract_expiry_date_from_pdf,
    extract_tonnage_from_ticket,
    generate_site_induction_poster,
    generate_plant_register_document,
    generate_waste_register_document,
    generate_permit_register_document,
    get_site_induction_url,
    get_waste_kpi_sheet_metadata,
    get_valid_template_tags,
    get_weekly_site_check_row_definitions,
    log_uploaded_waste_transfer_note,
    run_workspace_diagnostic,
    smart_scan_waste_transfer_note,
    sync_file_4_permit_records,
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

    def test_ladder_permit_context_maps_tagged_template_fields(self) -> None:
        permit = self.build_permit()
        permit.add_inspection_record(
            inspection_date=date(2026, 3, 10),
            inspected_by="J. Evans",
            rungs_ok=True,
            stiles_ok=False,
            feet_ok=True,
            comments_or_action_taken="Replace foot before reuse.",
            ok_to_use=False,
        )

        context = permit.to_template_context()

        self.assertEqual(context["job_number"], "UP-24020")
        self.assertEqual(context["date_issued"], "2026-03-10")
        self.assertEqual(context["task_description"], "Lamp replacement above service riser.")
        self.assertEqual(context["supervisor_name"], "J. Evans")
        self.assertEqual(context["manager_name"], "J. Evans")
        self.assertEqual(context["ladder_id"], "Plant room access")
        self.assertEqual(context["q1_yes"], "✔")
        self.assertEqual(context["q1_no"], "")
        self.assertEqual(context["q9_yes"], "")
        self.assertEqual(context["q9_no"], "✔")
        self.assertEqual(context["q10_yes"], "✔")
        self.assertEqual(context["q11_yes"], "✔")
        self.assertEqual(context["insp_name"], "J. Evans")
        self.assertEqual(context["insp_rungs"], "✔")
        self.assertEqual(context["insp_stiles"], "✘")
        self.assertEqual(context["insp_ok"], "✘")


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


class SiteCheckRegisterTests(unittest.TestCase):
    def test_site_check_register_uses_file_2_structure(self) -> None:
        register = SiteCheckRegister(
            doc_id="SCR-001",
            site_name="NG Lovedean Substation",
            created_at=datetime(2026, 3, 11, 6, 45),
            status=DocumentStatus.ACTIVE,
            week_commencing=date(2026, 3, 9),
            checked_at=datetime(2026, 3, 11, 6, 45),
            checked_by="Ceri Edwards",
            check_items=[
                SiteCheckItem(
                    check_name="Site access and egress routes are clear.",
                    frequency="Daily",
                    passed=True,
                    day_results={"tue": True},
                )
            ],
            overall_safe_to_start=True,
        )

        self.assertEqual(register.file_group, FileGroup.FILE_2)
        self.assertEqual(
            register.get_file_path(),
            Path("File 2") / "ng-lovedean-substation" / "site_check_register" / "SCR-001",
        )

    def test_site_check_register_round_trips_through_repository(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "documents.sqlite3"
            repository = DocumentRepository(database_path)
            repository.create_schema()

            register = SiteCheckRegister(
                doc_id="SCR-002",
                site_name="NG Lovedean Substation",
                created_at=datetime(2026, 3, 11, 6, 45),
                status=DocumentStatus.ACTIVE,
                week_commencing=date(2026, 3, 9),
                checked_at=datetime(2026, 3, 11, 6, 45),
                checked_by="Ceri Edwards",
                check_items=[
                    SiteCheckItem(
                        check_name="Fire points, extinguishers, and emergency routes are in place.",
                        frequency="Daily",
                        passed=True,
                        day_results={"mon": True, "tue": True},
                    ),
                    SiteCheckItem(
                        check_name="Plant condition and lifting/inspection records have been reviewed.",
                        frequency="Weekly",
                        passed=False,
                        day_results={"tue": False},
                    ),
                ],
                overall_safe_to_start=False,
            )

            repository.save(register)
            loaded = repository.get("SCR-002")

            self.assertIsInstance(loaded, SiteCheckRegister)
            self.assertEqual(loaded.checked_by, "Ceri Edwards")
            self.assertEqual(len(loaded.check_items), 2)
            self.assertFalse(loaded.overall_safe_to_start)
            self.assertEqual(loaded.week_commencing, date(2026, 3, 9))
            self.assertTrue(loaded.check_items[0].day_results["mon"])
            self.assertFalse(loaded.check_items[1].day_results["tue"])

    def test_site_check_register_exposes_weekly_template_context(self) -> None:
        register = SiteCheckRegister(
            doc_id="SCR-003",
            site_name="NG Lovedean Substation",
            created_at=datetime(2026, 3, 13, 6, 45),
            status=DocumentStatus.ACTIVE,
            week_commencing=date(2026, 3, 9),
            checked_at=datetime(2026, 3, 13, 6, 45),
            checked_by="Ceri Edwards",
            check_items=[
                SiteCheckItem(
                    check_name="Site access and egress routes are clear.",
                    frequency="Daily",
                    passed=True,
                    day_results={"mon": True, "tue": False, "wed": True},
                )
            ],
            overall_safe_to_start=True,
        )

        context = register.to_template_context()

        self.assertEqual(context["week_commencing"], "09/03/2026")
        self.assertEqual(context["site_check_1_mon"], "✓")
        self.assertEqual(context["site_check_1_tue"], "✗")
        self.assertEqual(context["site_check_1_wed"], "✓")
        self.assertEqual(context["site_check_1_sun"], "")


class SiteCheckChecklistGenerationTests(unittest.TestCase):
    def test_create_site_check_checklist_draft_renders_docx_template(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "workspace"
            output_directory = workspace_root / "FILE_2_Output"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "UHSF19.1 Daily-Weekly Checklist.docx"

            template_document = Document()
            template_document.add_paragraph("Week commencing {{week_commencing}}")
            template_document.add_paragraph("Site {{site_name}}")
            template_document.add_paragraph("Checked by {{checked_by}}")
            template_document.add_paragraph("Checked at {{checked_at}}")
            table = template_document.add_table(
                rows=1 + 7,
                cols=1 + len(SITE_CHECK_WEEKDAY_KEYS),
            )
            table.cell(0, 0).text = "Check"
            for day_offset, day_key in enumerate(SITE_CHECK_WEEKDAY_KEYS, start=1):
                table.cell(0, day_offset).text = day_key.title()
            for row_index in range(1, 8):
                table.cell(row_index, 0).text = f"{{{{site_check_{row_index}_name}}}}"
                for day_offset, day_key in enumerate(SITE_CHECK_WEEKDAY_KEYS, start=1):
                    table.cell(row_index, day_offset).text = (
                        f"{{{{site_check_{row_index}_{day_key}}}}}"
                    )
            template_document.save(template_path)

            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)
            original_output_dir = app_config.FILE_2_OUTPUT_DIR
            try:
                TemplateRegistry.TEMPLATE_PATHS["site_check_register"] = template_path
                app_config.FILE_2_OUTPUT_DIR = output_directory

                repository = DocumentRepository(database_path)
                repository.create_schema()
                register = SiteCheckRegister(
                    doc_id="SCR-PRINT-001",
                    site_name="NG Lovedean Substation",
                    created_at=datetime(2026, 3, 11, 6, 45),
                    status=DocumentStatus.ACTIVE,
                    week_commencing=date(2026, 3, 9),
                    checked_at=datetime(2026, 3, 11, 6, 45),
                    checked_by="Ceri Edwards",
                    check_items=[
                        SiteCheckItem(
                            check_name="Site access and egress routes are clear.",
                            frequency="Daily",
                            passed=True,
                            day_results={"mon": True, "tue": True},
                        ),
                        SiteCheckItem(
                            check_name="Housekeeping standards are acceptable across the workface.",
                            frequency="Daily",
                            passed=False,
                            day_results={"mon": True, "tue": False},
                        ),
                    ],
                    overall_safe_to_start=False,
                )
                repository.save(register)

                result = create_site_check_checklist_draft(
                    repository,
                    register=register,
                )
            finally:
                TemplateRegistry.TEMPLATE_PATHS = original_registry
                app_config.FILE_2_OUTPUT_DIR = original_output_dir

            self.assertTrue(result.output_path.exists())
            self.assertEqual(result.output_path.parent, output_directory)

            rendered_document = Document(result.output_path)
            self.assertIn("Week commencing 09/03/2026", rendered_document.paragraphs[0].text)
            self.assertIn("Site NG Lovedean Substation", rendered_document.paragraphs[1].text)
            self.assertEqual(rendered_document.tables[0].cell(1, 1).text, "✓")
            self.assertEqual(rendered_document.tables[0].cell(2, 2).text, "✗")

            indexed_files = repository.list_indexed_files(related_doc_id=register.doc_id)
            self.assertEqual(len(indexed_files), 1)
            self.assertEqual(indexed_files[0].file_group, FileGroup.FILE_2)


class WeeklySiteCheckTests(unittest.TestCase):
    def test_weekly_site_check_context_maps_matrix_and_signoff_fields(self) -> None:
        weekly_site_check = WeeklySiteCheck(
            doc_id="WSC-20260309",
            site_name="NG Lovedean Substation",
            created_at=datetime(2026, 3, 11, 6, 45),
            status=DocumentStatus.ACTIVE,
            week_commencing=date(2026, 3, 9),
            checked_at=datetime(2026, 3, 11, 6, 45),
            checked_by="Ceri Edwards",
            active_day_key="tue",
            row_states=[
                WeeklySiteCheckRowState(
                    row_number=1,
                    values={"mon": True, "tue": False, "weekly": True},
                ),
                WeeklySiteCheckRowState(
                    row_number=31,
                    values={"tue": True},
                ),
            ],
            daily_initials={"tue": "CE"},
            daily_time_markers={"tue": "AM"},
            overall_safe_to_start=False,
        )

        context = weekly_site_check.to_template_context()

        self.assertEqual(context["week_commencing"], "09/03/2026")
        self.assertEqual(context["mon_1"], "✔")
        self.assertEqual(context["tue_1"], "✘")
        self.assertEqual(context["weekly_1"], "✔")
        self.assertEqual(context["tue_31"], "✔")
        self.assertEqual(context["initials_tue"], "CE")
        self.assertEqual(context["time_tue"], "AM")

    def test_weekly_site_check_template_rows_match_official_template(self) -> None:
        get_weekly_site_check_row_definitions.cache_clear()
        row_definitions = get_weekly_site_check_row_definitions()

        self.assertEqual(len(row_definitions), 31)
        self.assertEqual(row_definitions[0].section, "Information Displayed")
        self.assertIn("H&S law poster", row_definitions[0].prompt)
        self.assertEqual(row_definitions[-1].section, "Environment")

    def test_weekly_site_check_valid_tags_follow_pruned_template(self) -> None:
        get_valid_template_tags.cache_clear()
        valid_tags = get_valid_template_tags()

        self.assertIn("week_commencing", valid_tags)
        self.assertIn("checked_by", valid_tags)
        self.assertIn("mon_1", valid_tags)
        self.assertIn("tue_1", valid_tags)
        self.assertIn("weekly_1", valid_tags)
        self.assertIn("mon_2", valid_tags)
        self.assertIn("weekly_2", valid_tags)
        self.assertIn("initials_wed", valid_tags)

    def test_weekly_site_check_from_storage_backfills_blank_checked_by(self) -> None:
        loaded = WeeklySiteCheck.from_storage_dict(
            {
                "doc_id": "WSC-LEGACY-001",
                "site_name": "NG Lovedean Substation",
                "created_at": "2026-03-13T04:25:09",
                "status": "active",
                "week_commencing": "2026-03-09",
                "checked_at": "2026-03-13T04:25:09",
                "checked_by": "",
                "active_day_key": "thu",
                "row_states": [],
                "daily_initials": {"thu": "CE"},
                "daily_time_markers": {"thu": "AM"},
                "overall_safe_to_start": False,
            }
        )

        self.assertEqual(loaded.checked_by, "Ceri Edwards")
        self.assertEqual(loaded.active_day_key, "thu")

    def test_create_weekly_site_check_checklist_uses_official_tagged_template(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_directory = Path(temp_dir) / "outputs" / "FILE_2_Checklists"
            database_path = Path(temp_dir) / "documents.sqlite3"
            original_output_dir = app_config.FILE_2_CHECKLIST_OUTPUT_DIR
            try:
                app_config.FILE_2_CHECKLIST_OUTPUT_DIR = output_directory
                repository = DocumentRepository(database_path)
                repository.create_schema()

                weekly_site_check = WeeklySiteCheck(
                    doc_id="WSC-20260309",
                    site_name="NG Lovedean Substation",
                    created_at=datetime(2026, 3, 11, 6, 45),
                    status=DocumentStatus.ACTIVE,
                    week_commencing=date(2026, 3, 9),
                    checked_at=datetime(2026, 3, 11, 6, 45),
                    checked_by="Ceri Edwards",
                    active_day_key="tue",
                    row_states=[
                        WeeklySiteCheckRowState(
                            row_number=1,
                            values={"mon": True, "tue": True, "weekly": True},
                        ),
                        WeeklySiteCheckRowState(
                            row_number=2,
                            values={"tue": False},
                        ),
                    ],
                    daily_initials={"mon": "CE", "tue": "CE", "wed": "CE"},
                    daily_time_markers={"tue": "AM", "wed": "PM"},
                    overall_safe_to_start=False,
                )
                repository.save(weekly_site_check)

                generated = create_weekly_site_check_checklist_draft(
                    repository,
                    weekly_site_check=weekly_site_check,
                )
            finally:
                app_config.FILE_2_CHECKLIST_OUTPUT_DIR = original_output_dir

            self.assertTrue(generated.output_path.exists())
            self.assertEqual(generated.output_path.parent, output_directory)

            rendered_document = Document(generated.output_path)
            rendered_table = rendered_document.tables[0]
            self.assertIn("NG Lovedean Substation", rendered_table.cell(0, 1).text)
            self.assertIn("09/03/2026", rendered_table.cell(0, 1).text)
            self.assertEqual(rendered_table.cell(1, 10).text, "✔")
            self.assertEqual(rendered_table.cell(2, 4).text, "✘")
            self.assertEqual(rendered_table.cell(33, 5).text.strip(), "CE")
            self.assertEqual(rendered_table.cell(34, 5).text.strip(), "PM")

            indexed_files = repository.list_indexed_files(
                related_doc_id=weekly_site_check.doc_id
            )
            self.assertEqual(len(indexed_files), 1)
            self.assertEqual(indexed_files[0].file_group, FileGroup.FILE_2)


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

    def test_delete_document_and_files_removes_induction_and_linked_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            repository = DocumentRepository(temp_path / "documents.sqlite3")
            repository.create_schema()
            signature_path = temp_path / "signature.png"
            completed_doc_path = temp_path / "induction.docx"
            signature_path.write_bytes(b"sig")
            completed_doc_path.write_bytes(b"doc")

            induction = InductionDocument(
                doc_id="IND-DEL-001",
                site_name="Uplands - Caerphilly",
                created_at=datetime(2026, 3, 10, 8, 45),
                status=DocumentStatus.ACTIVE,
                contractor_name="Acme Interiors",
                individual_name="P. Lewis",
                signature_image_path=str(signature_path),
                completed_document_path=str(completed_doc_path),
            )
            repository.save(induction)
            repository.index_file(
                file_name=signature_path.name,
                file_path=signature_path,
                file_category="induction_signature_png",
                file_group=FileGroup.FILE_3,
                site_name=induction.site_name,
                related_doc_id=induction.doc_id,
            )
            repository.index_file(
                file_name=completed_doc_path.name,
                file_path=completed_doc_path,
                file_category="completed_induction_docx",
                file_group=FileGroup.FILE_3,
                site_name=induction.site_name,
                related_doc_id=induction.doc_id,
            )

            deleted_paths = repository.delete_document_and_files(induction.doc_id)

            self.assertEqual(
                {path.resolve() for path in deleted_paths},
                {signature_path.resolve(), completed_doc_path.resolve()},
            )
            self.assertFalse(signature_path.exists())
            self.assertFalse(completed_doc_path.exists())
            self.assertEqual(
                repository.list_documents(document_type=InductionDocument.document_type),
                [],
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
    def test_ingestion_engine_creates_file2_register_and_aggregates_split_shifts(self) -> None:
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
                            "timeOut": "10:30",
                            "totalHours": 3.0,
                        },
                        {
                            "date": "2026-03-10",
                            "company": "Acme Interiors",
                            "workerName": "S. Carter",
                            "timeIn": "12:00",
                            "timeOut": "15:18",
                            "totalHours": 3.3,
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
            s_carter = next(
                record
                for record in register.attendance_records
                if record.workerName == "S. Carter"
            )
            self.assertEqual(s_carter.timeIn.strftime("%H:%M"), "07:30")
            self.assertEqual(s_carter.timeOut.strftime("%H:%M"), "15:18")
            self.assertAlmostEqual(s_carter.totalHours, 6.3)

            loaded = repository.get(register.doc_id)
            self.assertIsInstance(loaded, SiteAttendanceRegister)
            self.assertEqual(len(loaded.attendance_records), 2)
            loaded_s_carter = next(
                record
                for record in loaded.attendance_records
                if record.workerName == "S. Carter"
            )
            self.assertAlmostEqual(loaded_s_carter.totalHours, 6.3)

            second_pass = engine.ingest_site_attendance_json(
                json_path,
                site_name="Uplands - Cardiff",
            )
            self.assertEqual(len(second_pass.attendance_records), 2)
            second_pass_s_carter = next(
                record
                for record in second_pass.attendance_records
                if record.workerName == "S. Carter"
            )
            self.assertAlmostEqual(second_pass_s_carter.totalHours, 6.3)

    def test_ingestion_engine_deduplicates_identical_rows_before_aggregation(self) -> None:
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
                    ],
                    file_handle,
                )

            register = engine.ingest_site_attendance_json(
                json_path,
                site_name="Uplands - Cardiff",
            )

            self.assertEqual(len(register.attendance_records), 1)
            self.assertAlmostEqual(register.attendance_records[0].totalHours, 8.5)

    def test_ingestion_engine_reimport_updates_existing_day_total_for_split_shift_correction(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = DocumentRepository(Path(temp_dir) / "documents.sqlite3")
            repository.create_schema()
            engine = IngestionEngine(repository)
            json_path = Path(temp_dir) / "attendance.json"

            with json_path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    [
                        {
                            "date": "2026-03-04",
                            "company": "Acme Interiors",
                            "workerName": "C Ward",
                            "timeIn": "07:30",
                            "timeOut": "10:30",
                            "totalHours": 3.1,
                        }
                    ],
                    file_handle,
                )

            first_register = engine.ingest_site_attendance_json(
                json_path,
                site_name="Uplands - Cardiff",
            )
            self.assertAlmostEqual(first_register.attendance_records[0].totalHours, 3.1)

            with json_path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    [
                        {
                            "date": "2026-03-04",
                            "company": "Acme Interiors",
                            "workerName": "C Ward",
                            "timeIn": "07:30",
                            "timeOut": "10:30",
                            "totalHours": 3.1,
                        },
                        {
                            "date": "2026-03-04",
                            "company": "Acme Interiors",
                            "workerName": "C Ward",
                            "timeIn": "11:45",
                            "timeOut": "14:57",
                            "totalHours": 3.2,
                        },
                    ],
                    file_handle,
                )

            corrected_register = engine.ingest_site_attendance_json(
                json_path,
                site_name="Uplands - Cardiff",
            )

            self.assertEqual(len(corrected_register.attendance_records), 1)
            self.assertAlmostEqual(corrected_register.attendance_records[0].totalHours, 6.3)
            self.assertEqual(
                corrected_register.attendance_records[0].timeOut.strftime("%H:%M"),
                "14:57",
            )

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
            s_carter = next(
                record
                for record in register.attendance_records
                if record.workerName == "S. Carter"
            )
            self.assertAlmostEqual(s_carter.totalHours, 8.5)
            self.assertEqual(register.site_name, "Uplands - Cardiff")


class SiteWorkerRosterTests(unittest.TestCase):
    def test_build_site_worker_roster_reads_all_active_kpi_arrays(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "site-kpi-backup-1.json"
            with json_path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    {
                        "settings": {"siteName": "NG Lovedean Substation"},
                        "extractedRows": {
                            "weekly": [
                                {
                                    "date": "03/03/2026",
                                    "company": "Acme Interiors",
                                    "workerName": "S. Carter",
                                }
                            ],
                            "liveNow": [
                                {
                                    "date": "05/03/2026",
                                    "company": "Acme Interiors",
                                    "workerName": "S. Carter",
                                },
                                {
                                    "date": "04/03/2026",
                                    "company": "Beacon Civils",
                                    "workerName": "J. Evans",
                                },
                            ],
                        },
                    },
                    file_handle,
                )

            roster = build_site_worker_roster(
                site_name="NG Lovedean Substation",
                source_paths=[json_path],
            )

            self.assertEqual(len(roster), 2)
            s_carter = next(worker for worker in roster if worker.worker_name == "S. Carter")
            self.assertEqual(s_carter.company, "Acme Interiors")
            self.assertEqual(s_carter.last_on_site_date, date(2026, 3, 5))
            self.assertEqual(s_carter.induction_status, "Verified (Paper Record)")

    def test_build_site_worker_roster_filters_other_sites(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lovedean_json_path = Path(temp_dir) / "site-kpi-backup-lovedean.json"
            with lovedean_json_path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    {
                        "settings": {"siteName": "NG Lovedean Substation"},
                        "extractedRows": {
                            "weekly": [
                                {
                                    "date": "03/03/2026",
                                    "company": "Acme Interiors",
                                    "workerName": "S. Carter",
                                }
                            ]
                        },
                    },
                    file_handle,
                )

            other_json_path = Path(temp_dir) / "site-kpi-backup-other.json"
            with other_json_path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    {
                        "settings": {"siteName": "Another Project"},
                        "extractedRows": {
                            "weekly": [
                                {
                                    "date": "03/03/2026",
                                    "company": "Other Co",
                                    "workerName": "A. Other",
                                }
                            ]
                        },
                    },
                    file_handle,
                )

            roster = build_site_worker_roster(
                site_name="NG Lovedean Substation",
                source_paths=[lovedean_json_path, other_json_path],
            )

            self.assertEqual(len(roster), 1)
            self.assertEqual(roster[0].worker_name, "S. Carter")


class WorkspaceFileIndexingTests(unittest.TestCase):
    def _build_ladder_tagged_template(self, template_path: Path) -> None:
        template_document = Document()
        template_document.add_paragraph("Permit {{permit_number}}")
        template_document.add_paragraph("{{company_name}}")
        template_document.add_paragraph("{{contractor_name}}")
        template_document.add_paragraph("{{job_number}}")
        template_document.add_paragraph("{{date_issued}}")
        template_document.add_paragraph("{{site_name}}")
        template_document.add_paragraph("{{task_description}}")
        template_document.add_paragraph("{{supervisor_name}}")
        for question_number in range(1, 12):
            template_document.add_paragraph(
                f"Q{question_number} {{{{q{question_number}_yes}}}} {{{{q{question_number}_no}}}}"
            )
        template_document.add_paragraph("{{manager_name}}")
        template_document.add_paragraph("{{manager_signature}}")
        template_document.add_paragraph("{{ladder_id}}")
        table = template_document.add_table(rows=1, cols=7)
        table.cell(0, 0).text = "{{insp_date}}"
        table.cell(0, 1).text = "{{insp_name}}"
        table.cell(0, 2).text = "{{insp_rungs}}"
        table.cell(0, 3).text = "{{insp_stiles}}"
        table.cell(0, 4).text = "{{insp_feet}}"
        table.cell(0, 5).text = "{{insp_comments}}"
        table.cell(0, 6).text = "{{insp_ok}}"
        template_document.save(template_path)

    def _build_ladder_management_sections_template(self, template_path: Path) -> None:
        template_document = Document()
        header_table = template_document.add_table(rows=5, cols=5)
        header_table.cell(0, 1).text = "{{permit_number}}"
        header_table.cell(0, 4).text = "{{site_name}}"
        header_table.cell(1, 1).text = "{{job_number}}"
        header_table.cell(1, 4).text = "{{ladder_id}}"
        header_table.cell(2, 1).text = "{{task_description}}"
        header_table.cell(3, 1).text = "{{date_issued}}"

        template_document.add_paragraph("{{company_name}}")
        template_document.add_paragraph("{{contractor_name}}")
        template_document.add_paragraph("{{supervisor_name}}")
        for question_number in range(1, 12):
            template_document.add_paragraph(
                f"Q{question_number} {{{{q{question_number}_yes}}}} {{{{q{question_number}_no}}}}"
            )
        template_document.add_paragraph("{{manager_name}}")
        template_document.add_paragraph("{{manager_signature}}")
        template_document.add_paragraph("{{ladder_id}}")

        inspection_table = template_document.add_table(rows=1, cols=7)
        inspection_table.cell(0, 0).text = "{{insp_date}}"
        inspection_table.cell(0, 1).text = "{{insp_name}}"
        inspection_table.cell(0, 2).text = "{{insp_rungs}}"
        inspection_table.cell(0, 3).text = "{{insp_stiles}}"
        inspection_table.cell(0, 4).text = "{{insp_feet}}"
        inspection_table.cell(0, 5).text = "{{insp_comments}}"
        inspection_table.cell(0, 6).text = "{{insp_ok}}"

        for _ in range(2):
            template_document.add_table(rows=1, cols=1)

        acceptance_table = template_document.add_table(rows=8, cols=4)
        acceptance_table.cell(1, 0).text = "Name: {{manager_name}}"
        acceptance_table.cell(1, 1).text = "Sign: {{manager_signature}}"
        acceptance_table.cell(1, 2).text = "Date: (dd/mm/yyyy) {{date_issued}}"
        acceptance_table.cell(1, 3).text = "Position:"
        acceptance_table.cell(3, 0).text = "Name: {{contractor_name}}"
        acceptance_table.cell(3, 2).text = "Date: (dd/mm/yyyy) {{date_issued}}"
        acceptance_table.cell(3, 3).text = "Company: {{company_name}}"
        acceptance_table.cell(7, 0).text = "Name:"
        acceptance_table.cell(7, 1).text = "Sign:"
        acceptance_table.cell(7, 2).text = "Date: (dd/mm/yyyy)"
        acceptance_table.cell(7, 3).text = "Position:"

        template_document.save(template_path)

    def _build_permit_register_template(self, template_path: Path) -> None:
        template_document = Document()
        template_document.add_paragraph("{{site_name}}")
        template_document.add_paragraph("{{job_number}}")
        register_table = template_document.add_table(rows=4, cols=7)
        header_titles = [
            "Ref",
            "Date",
            "Type",
            "Name / Company",
            "Location",
            "Contact",
            "Issued",
        ]
        for index, title in enumerate(header_titles):
            register_table.cell(0, index).text = title

        start_paragraph = register_table.cell(1, 0).paragraphs[0]
        start_paragraph.add_run("{% tr ")
        start_paragraph.add_run("for p in permits ")
        start_paragraph.add_run("%}")

        register_table.cell(2, 0).text = "{{p.ref}}"
        register_table.cell(2, 1).text = "{{p.date}}"
        register_table.cell(2, 2).text = "{{p.type}}"
        register_table.cell(2, 3).text = "{{p.name_company}}"
        register_table.cell(2, 4).text = "{{p.location}}"
        register_table.cell(2, 5).text = "{{p.contact}}"
        register_table.cell(2, 6).text = "{{p.time_issued}} {{p.time_cancelled}}"

        end_paragraph = register_table.cell(3, 0).paragraphs[0]
        end_paragraph.add_run("{% tr ")
        end_paragraph.add_run("endfor ")
        end_paragraph.add_run("%}")
        template_document.save(template_path)

    def _build_site_induction_template(self, template_path: Path) -> None:
        template_document = Document()
        template_document.add_paragraph("{{site_name}}")
        template_document.add_paragraph("{{full_name}}")
        template_document.add_paragraph("{{company}}")
        template_document.add_paragraph("{{home_address}}")
        template_document.add_paragraph("{{contact_number}}")
        template_document.add_paragraph("{{occupation}}")
        template_document.add_paragraph("{{emergency_contact}}")
        template_document.add_paragraph("{{emergency_tel}}")
        template_document.add_paragraph("{{medical}}")
        template_document.add_paragraph("{{cscs_no}}")
        template_document.add_paragraph("{{first_aider}}")
        template_document.add_paragraph("{{fire_warden}}")
        template_document.add_paragraph("{{supervisor}}")
        template_document.add_paragraph("{{smsts}}")
        template_document.add_paragraph("{{signature_image}}")
        template_document.save(template_path)

    def test_extract_tonnage_from_ticket_converts_kg_to_tonnes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = Path(temp_dir) / "waste-ticket.pdf"
            document = fitz.open()
            page = document.new_page()
            page.insert_text(
                (72, 72),
                "Net Weight 3520 KG",
            )
            document.save(pdf_path)
            document.close()

            self.assertEqual(
                extract_tonnage_from_ticket(pdf_path),
                3.52,
            )

    def test_check_site_inductions_flags_missing_workers_against_induction_pdfs(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            induction_directory = workspace_root / "FILE_3_Inductions"
            database_path = workspace_root / "documents.sqlite3"
            induction_directory.mkdir(parents=True, exist_ok=True)
            (induction_directory / "S_Carter_Induction.pdf").write_bytes(b"%PDF-1.4\n")

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.INDUCTION_DIR,
                app_config.DATABASE_PATH,
            )

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.INDUCTION_DIR = induction_directory
                app_config.DATABASE_PATH = database_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                repository.save(
                    SiteAttendanceRegister(
                        doc_id="SAR-001",
                        site_name="NG Lovedean Substation",
                        created_at=datetime(2026, 3, 10, 7, 0),
                        status=DocumentStatus.ACTIVE,
                        attendance_records=[
                            SiteAttendanceRecord(
                                date=date(2026, 3, 10),
                                company="Acme Interiors",
                                workerName="S. Carter",
                                timeIn="07:30",
                                timeOut="16:00",
                                totalHours=8.5,
                            ),
                            SiteAttendanceRecord(
                                date=date(2026, 3, 10),
                                company="Acme Interiors",
                                workerName="J. Evans",
                                timeIn="07:30",
                                timeOut="16:00",
                                totalHours=8.5,
                            ),
                        ],
                    )
                )

                result = check_site_inductions(
                    repository,
                    on_date=date(2026, 3, 10),
                    site_name="NG Lovedean Substation",
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.INDUCTION_DIR,
                    app_config.DATABASE_PATH,
                ) = original_config

            self.assertFalse(result.is_compliant)
            self.assertEqual(result.workers_on_site, ["J. Evans", "S. Carter"])
            self.assertEqual(result.inducted_workers, ["S. Carter"])
            self.assertEqual(result.missing_workers, ["J. Evans"])
            self.assertEqual(
                result.matched_files["S. Carter"].name,
                "S_Carter_Induction.pdf",
            )

    def test_create_ladder_permit_draft_renders_and_indexes_without_induction_pdf(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            today = date.today()
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_template.docx"
            self._build_ladder_tagged_template(template_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                attendance_register = SiteAttendanceRegister(
                    doc_id="SAR-PERMIT-001",
                    site_name="NG Lovedean Substation",
                    created_at=datetime.combine(today, time(7, 0)),
                    status=DocumentStatus.ACTIVE,
                    attendance_records=[
                        SiteAttendanceRecord(
                            date=today,
                            company="Abucs",
                            workerName="S. Carter",
                            timeIn="07:30",
                            timeOut="16:00",
                            totalHours=8.5,
                        )
                    ],
                )
                repository.save(attendance_register)

                result = create_ladder_permit_draft(
                    repository,
                    attendance_record=attendance_register.attendance_records[0],
                    description_of_work="Installing CCTV cameras",
                    location_of_work="Transformer bay",
                    supervisor_name="Ceri Edwards",
                    safety_checklist={
                        question_number: question_number != 10
                        for question_number in range(1, 12)
                    },
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=False,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=False,
                    inspection_comments="Replace worn stile before use",
                    site_name=attendance_register.site_name,
                    job_number="JOB-001",
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            self.assertTrue(result.output_path.exists())
            self.assertEqual(result.output_path.parent, permits_directory)
            self.assertEqual(result.permit.worker_name, "S. Carter")
            self.assertEqual(result.permit.worker_company, "Abucs")
            self.assertEqual(result.permit.issued_date, today)
            self.assertEqual(result.permit.project_number, "JOB-001")
            self.assertEqual(result.permit.permit_number, "LADD-001")
            self.assertEqual(result.permit.description_of_work, "Installing CCTV cameras")
            self.assertEqual(result.permit.competent_supervisor_name, "Ceri Edwards")
            self.assertEqual(result.permit.to_template_context()["q10_yes"], "")
            self.assertEqual(result.permit.to_template_context()["q10_no"], "✔")
            self.assertEqual(result.permit.to_template_context()["manager_name"], "Ceri Edwards")
            self.assertEqual(result.permit.to_template_context()["auth_name"], "Ceri Edwards")
            self.assertEqual(result.permit.to_template_context()["issue_name"], "Ceri Edwards")
            self.assertEqual(
                result.permit.to_template_context()["manager_position"],
                "Project Manager",
            )
            self.assertEqual(result.permit.to_template_context()["insp_name"], "Ceri Edwards")
            self.assertEqual(result.permit.to_template_context()["insp_rungs"], "✔")
            self.assertEqual(result.permit.to_template_context()["insp_stiles"], "✘")
            self.assertEqual(result.permit.to_template_context()["insp_feet"], "✔")
            self.assertEqual(result.permit.to_template_context()["insp_ok"], "✘")
            self.assertEqual(
                result.permit.to_template_context()["insp_comments"],
                "Replace worn stile before use",
            )
            self.assertIsNone(result.induction_file)

            saved_permit = repository.get(result.permit.doc_id)
            self.assertIsInstance(saved_permit, LadderPermit)
            indexed_files = repository.list_indexed_files(related_doc_id=result.permit.doc_id)
            self.assertEqual(len(indexed_files), 1)
            self.assertEqual(indexed_files[0].file_group, FileGroup.FILE_4)

    def test_create_ladder_permit_draft_prefills_management_sections_in_output_docx(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            today = date.today()
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_management_template.docx"
            self._build_ladder_management_sections_template(template_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                attendance_record = SiteAttendanceRecord(
                    date=today,
                    company="Abucs",
                    workerName="S. Carter",
                    timeIn="07:30",
                    timeOut="16:00",
                    totalHours=8.5,
                )

                result = create_ladder_permit_draft(
                    repository,
                    attendance_record=attendance_record,
                    description_of_work="Installing CCTV cameras",
                    location_of_work="Transformer bay",
                    supervisor_name="Shift Supervisor",
                    safety_checklist={
                        question_number: True for question_number in range(1, 12)
                    },
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=True,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=True,
                    inspection_comments="No defects found",
                    site_name="NG Lovedean Substation",
                    job_number="JOB-001",
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            rendered = Document(result.output_path)
            header_table = rendered.tables[0]
            acceptance_table = rendered.tables[4]
            self.assertEqual(header_table.rows[3].cells[4].text, result.permit.valid_from_time.strftime("%H:%M"))
            self.assertEqual(
                header_table.rows[4].cells[1].text,
                result.permit.valid_to_date.strftime("%d/%m/%Y"),
            )
            self.assertEqual(
                header_table.rows[4].cells[4].text,
                result.permit.valid_to_time.strftime("%H:%M"),
            )
            self.assertEqual(acceptance_table.rows[1].cells[3].text, "Position: Project Manager")
            self.assertEqual(acceptance_table.rows[5].cells[0].text, "Name: S. Carter")
            self.assertEqual(
                acceptance_table.rows[5].cells[2].text,
                f"Date: (dd/mm/yyyy) {today.strftime('%d/%m/%Y')}",
            )
            self.assertEqual(acceptance_table.rows[5].cells[3].text, "Company: Abucs")
            self.assertEqual(acceptance_table.rows[7].cells[0].text, "Name: Ceri Edwards")
            self.assertEqual(
                acceptance_table.rows[7].cells[2].text,
                f"Date: (dd/mm/yyyy) {today.strftime('%d/%m/%Y')}",
            )
            self.assertEqual(acceptance_table.rows[7].cells[3].text, "Position: Project Manager")

    def test_create_ladder_permit_draft_accepts_latest_non_today_attendance_row(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            attendance_date = date(2026, 3, 4)
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_template.docx"
            self._build_ladder_tagged_template(template_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                attendance_record = SiteAttendanceRecord(
                    date=attendance_date,
                    company="Abucs",
                    workerName="J. Evans",
                    timeIn="07:30",
                    timeOut="16:00",
                    totalHours=8.5,
                )

                result = create_ladder_permit_draft(
                    repository,
                    attendance_record=attendance_record,
                    description_of_work="Fire alarm sensor replacement",
                    location_of_work="Switch room",
                    supervisor_name="Ceri Edwards",
                    safety_checklist={question_number: True for question_number in range(1, 12)},
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=True,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=True,
                    inspection_comments="No defects found",
                    site_name="NG Lovedean Substation",
                    job_number="JOB-002",
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            self.assertEqual(result.permit.valid_from_date, result.permit.issued_date)
            self.assertEqual(
                result.permit.valid_to_datetime - result.permit.valid_from_datetime,
                timedelta(hours=8),
            )
            self.assertTrue(result.output_path.exists())
            self.assertEqual(result.permit.project_number, "JOB-002")

    def test_create_ladder_permit_draft_uses_roster_name_for_operative_context(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            attendance_date = date(2026, 3, 4)
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_template.docx"
            self._build_ladder_tagged_template(template_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                attendance_record = SiteAttendanceRecord(
                    date=attendance_date,
                    company="Abucs",
                    workerName="Abucs Ltd",
                    timeIn="07:30",
                    timeOut="16:00",
                    totalHours=8.5,
                )
                selected_worker = SiteWorker(
                    company="Abucs",
                    worker_name="Sean Michael Carter",
                    last_on_site_date=attendance_date,
                )

                result = create_ladder_permit_draft(
                    repository,
                    attendance_record=attendance_record,
                    site_worker=selected_worker,
                    description_of_work="Fire alarm sensor replacement",
                    location_of_work="Switch room",
                    supervisor_name="Ceri Edwards",
                    safety_checklist={
                        question_number: True for question_number in range(1, 12)
                    },
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=True,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=True,
                    inspection_comments="No defects found",
                    site_name="NG Lovedean Substation",
                    job_number="JOB-002",
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            context = result.permit.to_template_context()
            self.assertEqual(result.permit.worker_name, "Sean Michael Carter")
            self.assertEqual(result.permit.worker_company, "Abucs")
            self.assertEqual(context["company_name"], "Abucs")
            self.assertEqual(context["contractor_name"], "Sean Carter")
            self.assertEqual(context["op_name"], "Sean Carter")

    def test_create_ladder_permit_draft_uses_current_system_time_for_valid_from(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            attendance_date = date(2026, 3, 4)
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_template.docx"
            self._build_ladder_tagged_template(template_path)
            fixed_issue_time = datetime(2026, 3, 11, 8, 30)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                attendance_record = SiteAttendanceRecord(
                    date=attendance_date,
                    company="Abucs",
                    workerName="J. Evans",
                    timeIn="07:30",
                    timeOut="16:00",
                    totalHours=8.5,
                )

                with patch(
                    "uplands_site_command_centre.workspace._current_permit_issue_datetime",
                    return_value=fixed_issue_time,
                ):
                    result = create_ladder_permit_draft(
                        repository,
                        attendance_record=attendance_record,
                        description_of_work="Fire alarm sensor replacement",
                        location_of_work="Switch room",
                        supervisor_name="Ceri Edwards",
                        safety_checklist={
                            question_number: True for question_number in range(1, 12)
                        },
                        inspection_checked_by="Ceri Edwards",
                        inspection_rungs_ok=True,
                        inspection_stiles_ok=True,
                        inspection_feet_ok=True,
                        inspection_ok_to_use=True,
                        inspection_comments="No defects found",
                        site_name="NG Lovedean Substation",
                        job_number="JOB-002",
                    )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            self.assertEqual(result.permit.valid_from_time, time(8, 30))
            self.assertEqual(result.permit.valid_to_time, time(16, 30))
            self.assertEqual(result.permit.issued_date, fixed_issue_time.date())
            self.assertEqual(result.permit.valid_from_date, fixed_issue_time.date())
            self.assertEqual(result.permit.valid_to_date, fixed_issue_time.date())

    def test_create_ladder_permit_draft_rolls_expiry_date_when_eight_hours_crosses_midnight(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            attendance_date = date(2026, 3, 4)
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_template.docx"
            self._build_ladder_tagged_template(template_path)
            fixed_issue_time = datetime(2026, 3, 11, 20, 30)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                attendance_record = SiteAttendanceRecord(
                    date=attendance_date,
                    company="Abucs",
                    workerName="J. Evans",
                    timeIn="07:30",
                    timeOut="16:00",
                    totalHours=8.5,
                )

                with patch(
                    "uplands_site_command_centre.workspace._current_permit_issue_datetime",
                    return_value=fixed_issue_time,
                ):
                    result = create_ladder_permit_draft(
                        repository,
                        attendance_record=attendance_record,
                        description_of_work="Fire alarm sensor replacement",
                        location_of_work="Switch room",
                        supervisor_name="Ceri Edwards",
                        safety_checklist={
                            question_number: True for question_number in range(1, 12)
                        },
                        inspection_checked_by="Ceri Edwards",
                        inspection_rungs_ok=True,
                        inspection_stiles_ok=True,
                        inspection_feet_ok=True,
                        inspection_ok_to_use=True,
                        inspection_comments="No defects found",
                        site_name="NG Lovedean Substation",
                        job_number="JOB-002",
                    )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            self.assertEqual(result.permit.valid_from_time, time(20, 30))
            self.assertEqual(result.permit.valid_to_time, time(4, 30))
            self.assertEqual(result.permit.valid_from_date, date(2026, 3, 11))
            self.assertEqual(result.permit.valid_to_date, date(2026, 3, 12))

    def test_create_ladder_permit_draft_requires_job_number_from_project_setup(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_template.docx"
            self._build_ladder_tagged_template(template_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                attendance_record = SiteAttendanceRecord(
                    date=date.today(),
                    company="Abucs",
                    workerName="S. Carter",
                    timeIn="07:30",
                    timeOut="16:00",
                    totalHours=8.5,
                )

                with self.assertRaises(ValidationError):
                    create_ladder_permit_draft(
                        repository,
                        attendance_record=attendance_record,
                        description_of_work="Installing CCTV cameras",
                        location_of_work="Switch room",
                        supervisor_name="Ceri Edwards",
                        safety_checklist={
                            question_number: True for question_number in range(1, 12)
                        },
                        inspection_checked_by="Ceri Edwards",
                        inspection_rungs_ok=True,
                        inspection_stiles_ok=True,
                        inspection_feet_ok=True,
                        inspection_ok_to_use=True,
                        inspection_comments="No defects found",
                        site_name="NG Lovedean Substation",
                        job_number="",
                    )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

    def test_create_ladder_permit_draft_auto_numbers_sequentially_for_site(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            today = date.today()
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_template.docx"
            self._build_ladder_tagged_template(template_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                first_record = SiteAttendanceRecord(
                    date=today,
                    company="Abucs",
                    workerName="S. Carter",
                    timeIn="07:30",
                    timeOut="16:00",
                    totalHours=8.5,
                )
                second_record = SiteAttendanceRecord(
                    date=today,
                    company="Abucs",
                    workerName="J. Evans",
                    timeIn="08:00",
                    timeOut="16:30",
                    totalHours=8.0,
                )

                first_permit = create_ladder_permit_draft(
                    repository,
                    attendance_record=first_record,
                    description_of_work="Install tray supports",
                    location_of_work="Bay 1",
                    supervisor_name="Ceri Edwards",
                    safety_checklist={question_number: True for question_number in range(1, 12)},
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=True,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=True,
                    inspection_comments="No defects found",
                    site_name="NG Lovedean Substation",
                    job_number="JOB-010",
                )
                second_permit = create_ladder_permit_draft(
                    repository,
                    attendance_record=second_record,
                    description_of_work="Inspect cable ladder",
                    location_of_work="Bay 2",
                    supervisor_name="Ceri Edwards",
                    safety_checklist={question_number: True for question_number in range(1, 12)},
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=True,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=True,
                    inspection_comments="No defects found",
                    site_name="NG Lovedean Substation",
                    job_number="JOB-010",
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            self.assertEqual(first_permit.permit.permit_number, "LADD-001")
            self.assertEqual(second_permit.permit.permit_number, "LADD-002")

    def test_generate_permit_register_document_renders_and_indexes_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            today = date.today()
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            ladder_template_path = Path(temp_dir) / "ladder_template.docx"
            register_template_path = Path(temp_dir) / "permit_register_template.docx"
            self._build_ladder_tagged_template(ladder_template_path)
            self._build_permit_register_template(register_template_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = ladder_template_path
                TemplateRegistry.TEMPLATE_PATHS["permit_register"] = register_template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                first_record = SiteAttendanceRecord(
                    date=today,
                    company="Abucs",
                    workerName="S. Carter",
                    timeIn="07:30",
                    timeOut="16:00",
                    totalHours=8.5,
                )
                second_record = SiteAttendanceRecord(
                    date=today,
                    company="Abucs",
                    workerName="J. Evans",
                    timeIn="08:00",
                    timeOut="16:30",
                    totalHours=8.0,
                )

                create_ladder_permit_draft(
                    repository,
                    attendance_record=first_record,
                    description_of_work="Install tray supports",
                    location_of_work="Bay 1",
                    supervisor_name="Ceri Edwards",
                    safety_checklist={question_number: True for question_number in range(1, 12)},
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=True,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=True,
                    inspection_comments="No defects found",
                    site_name="NG Lovedean Substation",
                    job_number="JOB-777",
                )
                create_ladder_permit_draft(
                    repository,
                    attendance_record=second_record,
                    description_of_work="Inspect cable ladder",
                    location_of_work="Bay 2",
                    supervisor_name="Ceri Edwards",
                    safety_checklist={question_number: True for question_number in range(1, 12)},
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=True,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=True,
                    inspection_comments="No defects found",
                    site_name="NG Lovedean Substation",
                    job_number="JOB-777",
                )

                result = generate_permit_register_document(
                    repository,
                    site_name="NG Lovedean Substation",
                    job_number="JOB-777",
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            self.assertTrue(result.output_path.exists())
            self.assertEqual(result.output_path.parent, permits_directory)
            self.assertEqual(result.permit_count, 2)

            rendered_document = Document(result.output_path)
            rendered_text = "\n".join(
                paragraph.text for paragraph in rendered_document.paragraphs
            )
            table_text = "\n".join(
                cell.text
                for table in rendered_document.tables
                for row in table.rows
                for cell in row.cells
            )
            self.assertIn("NG Lovedean Substation", rendered_text)
            self.assertIn("JOB-777", rendered_text)
            self.assertIn("LADD-001", table_text)
            self.assertIn("LADD-002", table_text)
            self.assertIn("S. Carter | Abucs", table_text)
            self.assertIn("J. Evans | Abucs", table_text)

            indexed_files = repository.list_indexed_files(
                file_category="permit_register_docx"
            )
            self.assertEqual(len(indexed_files), 1)
            self.assertEqual(indexed_files[0].file_group, FileGroup.FILE_4)

    def test_sync_file_4_permit_records_removes_ghost_permits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            today = date.today()
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            permits_directory = workspace_root / "FILE_4_Permits"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "ladder_template.docx"
            self._build_ladder_tagged_template(template_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.PERMITS_DESTINATION,
                app_config.DATABASE_PATH,
            )
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.PERMITS_DESTINATION = permits_directory
                app_config.DATABASE_PATH = database_path
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                generated_permit = create_ladder_permit_draft(
                    repository,
                    attendance_record=SiteAttendanceRecord(
                        date=today,
                        company="Abucs",
                        workerName="S. Carter",
                        timeIn="07:30",
                        timeOut="16:00",
                        totalHours=8.5,
                    ),
                    description_of_work="Installing CCTV cameras",
                    location_of_work="Switch room",
                    supervisor_name="Ceri Edwards",
                    safety_checklist={
                        question_number: True for question_number in range(1, 12)
                    },
                    inspection_checked_by="Ceri Edwards",
                    inspection_rungs_ok=True,
                    inspection_stiles_ok=True,
                    inspection_feet_ok=True,
                    inspection_ok_to_use=True,
                    inspection_comments="No defects found",
                    site_name="NG Lovedean Substation",
                    job_number="JOB-900",
                )
                generated_permit.output_path.unlink()

                sync_result = sync_file_4_permit_records(
                    repository,
                    site_name="NG Lovedean Substation",
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.PERMITS_DESTINATION,
                    app_config.DATABASE_PATH,
                ) = original_config
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            self.assertEqual(sync_result.removed_count, 1)
            self.assertEqual(
                repository.list_documents(
                    document_type=LadderPermit.document_type,
                    site_name="NG Lovedean Substation",
                ),
                [],
            )
            self.assertEqual(
                repository.list_indexed_files(file_category="ladder_permit_docx"),
                [],
            )

    def test_extract_expiry_date_from_pdf_prefers_expiry_phrase_and_ignores_generation_dates(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = Path(temp_dir) / "Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877.pdf"
            document = fitz.open()
            page = document.new_page()
            page.insert_text(
                (72, 72),
                (
                    "Date of registration 13 September 2023\n"
                    "Expiry date of registration (unless revoked) 25 October 2026\n"
                    "This certificate was created on 13 September 2023. "
                    "These details are correct at the time of certificate generation."
                ),
            )
            document.save(pdf_path)
            document.close()

            self.assertEqual(
                extract_expiry_date_from_pdf(pdf_path),
                date(2026, 10, 25),
            )

    def test_extract_expiry_date_from_pdf_uses_filename_when_expiry_is_not_in_visible_text(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = (
                Path(temp_dir)
                / "Liability Insurance - Biffa Ltd Subsidiaries - Expiry 31.03.2026.pdf"
            )
            document = fitz.open()
            page = document.new_page()
            page.insert_text(
                (72, 72),
                (
                    "Attachment to letter dated 17-March-2025\n"
                    "This certificate was created on 17 March 2025."
                ),
            )
            document.save(pdf_path)
            document.close()

            self.assertEqual(
                extract_expiry_date_from_pdf(pdf_path),
                date(2026, 3, 31),
            )

    def test_file_and_index_all_overwrites_stale_carrier_record_using_reference_number(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            inbox = workspace_root / "ingest"
            waste_destination = workspace_root / "FILE_1_Environment" / "Waste_Notes"
            carrier_docs_destination = workspace_root / "FILE_1_Environment" / "Carrier_Docs"
            waste_reports_destination = workspace_root / "FILE_1_Environment" / "Waste_Reports"
            attendance_destination = workspace_root / "FILE_2_Registers" / "Attendance"
            plant_hire_directory = workspace_root / "FILE_2_Registers" / "Plant_Hire_Register"
            database_path = workspace_root / "documents.sqlite3"
            inbox.mkdir(parents=True, exist_ok=True)

            ls_pdf_path = inbox / "Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877.pdf"
            ls_pdf_document = fitz.open()
            ls_pdf_page = ls_pdf_document.new_page()
            ls_pdf_page.insert_text(
                (72, 72),
                (
                    "Name of registered carrier L&S WASTE MANAGEMENT LIMITED\n"
                    "Date of registration 13 September 2023\n"
                    "Expiry date of registration (unless revoked) 25 October 2026\n"
                    "This certificate was created on 13 September 2023. "
                    "These details are correct at the time of certificate generation."
                ),
            )
            ls_pdf_document.save(ls_pdf_path)
            ls_pdf_document.close()

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.INBOX,
                app_config.WASTE_DESTINATION,
                app_config.CARRIER_DOCS_DESTINATION,
                app_config.WASTE_REPORTS_DESTINATION,
                app_config.ATTENDANCE_DESTINATION,
                app_config.PLANT_HIRE_REGISTER_DIR,
                app_config.INDUCTION_DIR,
                app_config.RAMS_DESTINATION,
                app_config.COSHH_DESTINATION,
                app_config.FILE_3_OUTPUT_DIR,
                app_config.DATABASE_PATH,
            )

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.INBOX = inbox
                app_config.WASTE_DESTINATION = waste_destination
                app_config.CARRIER_DOCS_DESTINATION = carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = attendance_destination
                app_config.PLANT_HIRE_REGISTER_DIR = plant_hire_directory
                app_config.INDUCTION_DIR = workspace_root / "FILE_3_Inductions"
                app_config.DATABASE_PATH = database_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                repository.save(
                    CarrierComplianceDocument(
                        doc_id="CCD-carriers-exp-cbdu198877-licence",
                        site_name="NG Lovedean Substation",
                        created_at=datetime(2026, 3, 10, 8, 0),
                        status=DocumentStatus.ACTIVE,
                        carrier_name="Carriers Exp . . Cbdu198877",
                        carrier_document_type=CarrierComplianceDocumentType.LICENCE,
                        reference_number="Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877",
                        expiry_date=date(2023, 9, 13),
                    )
                )

                filed_assets = file_and_index_all(repository)
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.INBOX,
                    app_config.WASTE_DESTINATION,
                    app_config.CARRIER_DOCS_DESTINATION,
                    app_config.WASTE_REPORTS_DESTINATION,
                    app_config.ATTENDANCE_DESTINATION,
                    app_config.PLANT_HIRE_REGISTER_DIR,
                    app_config.INDUCTION_DIR,
                    app_config.RAMS_DESTINATION,
                    app_config.COSHH_DESTINATION,
                    app_config.FILE_3_OUTPUT_DIR,
                    app_config.DATABASE_PATH,
                ) = original_config

            carrier_documents = repository.list_documents(
                document_type=CarrierComplianceDocument.document_type
            )
            self.assertEqual(len(carrier_documents), 1)
            self.assertEqual(carrier_documents[0].doc_id, "CCD-carriers-exp-cbdu198877-licence")
            self.assertEqual(carrier_documents[0].carrier_name, "Abucs")
            self.assertEqual(
                carrier_documents[0].carrier_document_type,
                CarrierComplianceDocumentType.LICENCE,
            )
            self.assertEqual(carrier_documents[0].expiry_date, date(2026, 10, 25))
            self.assertEqual(len(filed_assets), 1)

    def test_file_and_index_all_archives_duplicate_active_record_for_same_reference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            inbox = workspace_root / "ingest"
            waste_destination = workspace_root / "FILE_1_Environment" / "Waste_Notes"
            carrier_docs_destination = workspace_root / "FILE_1_Environment" / "Carrier_Docs"
            waste_reports_destination = workspace_root / "FILE_1_Environment" / "Waste_Reports"
            attendance_destination = workspace_root / "FILE_2_Registers" / "Attendance"
            plant_hire_directory = workspace_root / "FILE_2_Registers" / "Plant_Hire_Register"
            database_path = workspace_root / "documents.sqlite3"
            inbox.mkdir(parents=True, exist_ok=True)

            ls_pdf_path = inbox / "Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877.pdf"
            ls_pdf_document = fitz.open()
            ls_pdf_page = ls_pdf_document.new_page()
            ls_pdf_page.insert_text(
                (72, 72),
                (
                    "Name of registered carrier L&S WASTE MANAGEMENT LIMITED\n"
                    "Date of registration 13 September 2023\n"
                    "Expiry date of registration (unless revoked) 25 October 2026\n"
                    "This certificate was created on 13 September 2023. "
                    "These details are correct at the time of certificate generation."
                ),
            )
            ls_pdf_document.save(ls_pdf_path)
            ls_pdf_document.close()

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.INBOX,
                app_config.WASTE_DESTINATION,
                app_config.CARRIER_DOCS_DESTINATION,
                app_config.WASTE_REPORTS_DESTINATION,
                app_config.ATTENDANCE_DESTINATION,
                app_config.PLANT_HIRE_REGISTER_DIR,
                app_config.INDUCTION_DIR,
                app_config.RAMS_DESTINATION,
                app_config.COSHH_DESTINATION,
                app_config.FILE_3_OUTPUT_DIR,
                app_config.DATABASE_PATH,
            )

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.INBOX = inbox
                app_config.WASTE_DESTINATION = waste_destination
                app_config.CARRIER_DOCS_DESTINATION = carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = attendance_destination
                app_config.PLANT_HIRE_REGISTER_DIR = plant_hire_directory
                app_config.INDUCTION_DIR = workspace_root / "FILE_3_Inductions"
                app_config.RAMS_DESTINATION = workspace_root / "FILE_3_Safety" / "RAMS"
                app_config.COSHH_DESTINATION = workspace_root / "FILE_3_Safety" / "COSHH"
                app_config.FILE_3_OUTPUT_DIR = workspace_root / "FILE_3_Safety" / "Registers"
                app_config.DATABASE_PATH = database_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                repository.save(
                    CarrierComplianceDocument(
                        doc_id="CCD-abucs-licence",
                        site_name="NG Lovedean Substation",
                        created_at=datetime(2026, 3, 10, 9, 0),
                        status=DocumentStatus.ACTIVE,
                        carrier_name="Abucs",
                        carrier_document_type=CarrierComplianceDocumentType.LICENCE,
                        reference_number="Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877",
                        expiry_date=date(2026, 10, 25),
                    )
                )
                repository.save(
                    CarrierComplianceDocument(
                        doc_id="CCD-carriers-exp-cbdu198877-licence",
                        site_name="NG Lovedean Substation",
                        created_at=datetime(2026, 3, 10, 8, 0),
                        status=DocumentStatus.ACTIVE,
                        carrier_name="Carriers Exp . . Cbdu198877",
                        carrier_document_type=CarrierComplianceDocumentType.LICENCE,
                        reference_number="Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877",
                        expiry_date=date(2023, 9, 13),
                    )
                )

                file_and_index_all(repository)
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.INBOX,
                    app_config.WASTE_DESTINATION,
                    app_config.CARRIER_DOCS_DESTINATION,
                    app_config.WASTE_REPORTS_DESTINATION,
                    app_config.ATTENDANCE_DESTINATION,
                    app_config.PLANT_HIRE_REGISTER_DIR,
                    app_config.INDUCTION_DIR,
                    app_config.RAMS_DESTINATION,
                    app_config.COSHH_DESTINATION,
                    app_config.FILE_3_OUTPUT_DIR,
                    app_config.DATABASE_PATH,
                ) = original_config

            carrier_documents = sorted(
                repository.list_documents(
                    document_type=CarrierComplianceDocument.document_type
                ),
                key=lambda document: document.doc_id,
            )
            self.assertEqual(len(carrier_documents), 2)
            self.assertEqual(carrier_documents[0].doc_id, "CCD-abucs-licence")
            self.assertEqual(carrier_documents[0].status, DocumentStatus.ACTIVE)
            self.assertEqual(carrier_documents[1].doc_id, "CCD-carriers-exp-cbdu198877-licence")
            self.assertEqual(carrier_documents[1].status, DocumentStatus.ARCHIVED)

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
            waste_pdf_document = fitz.open()
            waste_pdf_page = waste_pdf_document.new_page()
            waste_pdf_page.insert_text(
                (72, 72),
                (
                    "Ticket No.\n"
                    "10/03/2026\n"
                    "31194\n"
                    "National Grid Broadway Lane Lovedean Waterlooville\n"
                    "Waste Type:Mixed Construction\n"
                    "17 09 04\n"
                    "Weight\n"
                    "3.52 Tonnes\n"
                    "DUTY OF CARE WASTE TRANSFER NOTE / INVOICE\n"
                ),
            )
            waste_pdf_document.save(pdf_path)
            waste_pdf_document.close()

            biffa_pdf_path = (
                inbox
                / "Liability Insurance   - EL 20m PL  PI  10m  -  Biffa Ltd  Subsidiaries - Expiry 31.03.2026.pdf"
            )
            biffa_pdf_document = fitz.open()
            biffa_pdf_page = biffa_pdf_document.new_page()
            biffa_pdf_page.insert_text(
                (72, 72),
                (
                    "Attachment to letter dated 17-March-2025\n"
                    "This certificate was created on 17 March 2025."
                ),
            )
            biffa_pdf_document.save(biffa_pdf_path)
            biffa_pdf_document.close()

            ls_pdf_path = inbox / "Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877.pdf"
            ls_pdf_document = fitz.open()
            ls_pdf_page = ls_pdf_document.new_page()
            ls_pdf_page.insert_text(
                (72, 72),
                (
                    "Name of registered carrier L&S WASTE MANAGEMENT LIMITED\n"
                    "Date of registration 13 September 2023\n"
                    "Expiry date of registration (unless revoked) 25 October 2026\n"
                    "This certificate was created on 13 September 2023. "
                    "These details are correct at the time of certificate generation."
                ),
            )
            ls_pdf_document.save(ls_pdf_path)
            ls_pdf_document.close()

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
                app_config.PLANT_HIRE_REGISTER_DIR,
                app_config.INDUCTION_DIR,
                app_config.RAMS_DESTINATION,
                app_config.COSHH_DESTINATION,
                app_config.FILE_3_OUTPUT_DIR,
                app_config.DATABASE_PATH,
            )

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.INBOX = inbox
                app_config.WASTE_DESTINATION = waste_destination
                app_config.CARRIER_DOCS_DESTINATION = carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = attendance_destination
                app_config.PLANT_HIRE_REGISTER_DIR = workspace_root / "FILE_2_Registers" / "Plant_Hire_Register"
                app_config.INDUCTION_DIR = workspace_root / "FILE_3_Inductions"
                app_config.RAMS_DESTINATION = workspace_root / "FILE_3_Safety" / "RAMS"
                app_config.COSHH_DESTINATION = workspace_root / "FILE_3_Safety" / "COSHH"
                app_config.FILE_3_OUTPUT_DIR = workspace_root / "FILE_3_Safety" / "Registers"
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
                    app_config.PLANT_HIRE_REGISTER_DIR,
                    app_config.INDUCTION_DIR,
                    app_config.RAMS_DESTINATION,
                    app_config.COSHH_DESTINATION,
                    app_config.FILE_3_OUTPUT_DIR,
                    app_config.DATABASE_PATH,
                ) = original_config

            moved_pdf_path = waste_destination / "31194.PDF"
            moved_biffa_pdf_path = (
                carrier_docs_destination
                / "Liability Insurance   - EL 20m PL  PI  10m  -  Biffa Ltd  Subsidiaries - Expiry 31.03.2026.pdf"
            )
            moved_ls_pdf_path = (
                carrier_docs_destination
                / "Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877.pdf"
            )
            moved_waste_report_path = waste_reports_destination / "March_Waste_Report.xlsx"
            moved_json_path = attendance_destination / "site-kpi-backup.json"

            self.assertFalse(pdf_path.exists())
            self.assertFalse(biffa_pdf_path.exists())
            self.assertFalse(ls_pdf_path.exists())
            self.assertFalse(waste_report_path.exists())
            self.assertFalse(json_path.exists())
            self.assertTrue(moved_pdf_path.exists())
            self.assertTrue(moved_biffa_pdf_path.exists())
            self.assertTrue(moved_ls_pdf_path.exists())
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
            self.assertEqual(len(indexed_files), 5)
            self.assertEqual(
                {record.file_path for record in indexed_files},
                {
                    moved_pdf_path.resolve(),
                    moved_biffa_pdf_path.resolve(),
                    moved_ls_pdf_path.resolve(),
                    moved_waste_report_path.resolve(),
                    moved_json_path.resolve(),
                },
            )
            carrier_documents = repository.list_documents(
                document_type=CarrierComplianceDocument.document_type
            )
            self.assertEqual(len(carrier_documents), 2)
            documents_by_type = {
                document.carrier_document_type: document
                for document in carrier_documents
                if isinstance(document, CarrierComplianceDocument)
            }
            self.assertEqual(
                set(documents_by_type.keys()),
                {
                    CarrierComplianceDocumentType.INSURANCE,
                    CarrierComplianceDocumentType.LICENCE,
                },
            )
            self.assertEqual(
                documents_by_type[CarrierComplianceDocumentType.INSURANCE].carrier_name,
                "Abucs",
            )
            self.assertEqual(
                documents_by_type[CarrierComplianceDocumentType.INSURANCE].expiry_date,
                date(2026, 3, 31),
            )
            self.assertEqual(
                documents_by_type[CarrierComplianceDocumentType.LICENCE].carrier_name,
                "Abucs",
            )
            self.assertEqual(
                documents_by_type[CarrierComplianceDocumentType.LICENCE].expiry_date,
                date(2026, 10, 25),
            )
            self.assertEqual(
                documents_by_type[
                    CarrierComplianceDocumentType.INSURANCE
                ].reference_number,
                "Liability Insurance   - EL 20m PL  PI  10m  -  Biffa Ltd  Subsidiaries - Expiry 31.03.2026",
            )
            self.assertEqual(
                documents_by_type[
                    CarrierComplianceDocumentType.LICENCE
                ].reference_number,
                "Waste-Carriers-Certificate-Exp-25.10.-2026-CBDU198877",
            )
            carrier_indexed_files = repository.list_indexed_files(
                file_category="carrier_doc_pdf"
            )
            self.assertEqual(len(carrier_indexed_files), 2)
            self.assertEqual(
                {record.related_doc_id for record in carrier_indexed_files},
                {
                    documents_by_type[CarrierComplianceDocumentType.INSURANCE].doc_id,
                    documents_by_type[CarrierComplianceDocumentType.LICENCE].doc_id,
                },
            )

            carrier_assets = [
                asset for asset in filed_assets if asset.file_category == "carrier_doc_pdf"
            ]
            self.assertEqual(len(carrier_assets), 2)
            captured_by_type = {
                asset.auto_captured_document_type: asset
                for asset in carrier_assets
            }
            self.assertEqual(
                captured_by_type[
                    CarrierComplianceDocumentType.INSURANCE
                ].auto_captured_expiry_date,
                date(2026, 3, 31),
            )
            self.assertEqual(
                captured_by_type[
                    CarrierComplianceDocumentType.INSURANCE
                ].auto_captured_carrier_name,
                "Abucs",
            )
            self.assertEqual(
                captured_by_type[
                    CarrierComplianceDocumentType.LICENCE
                ].auto_captured_expiry_date,
                date(2026, 10, 25),
            )
            self.assertEqual(
                captured_by_type[
                    CarrierComplianceDocumentType.LICENCE
                ].auto_captured_carrier_name,
                "Abucs",
            )

            waste_transfer_notes = repository.list_documents(
                document_type=WasteTransferNoteDocument.document_type
            )
            self.assertEqual(len(waste_transfer_notes), 1)
            self.assertEqual(waste_transfer_notes[0].wtn_number, "31194")
            self.assertEqual(waste_transfer_notes[0].quantity_tonnes, 3.52)
            self.assertEqual(waste_transfer_notes[0].date, date(2026, 3, 10))
            self.assertEqual(waste_transfer_notes[0].ewc_code, "17 09 04")
            self.assertEqual(waste_transfer_notes[0].carrier_name, "Abucs")
            self.assertEqual(waste_transfer_notes[0].status, DocumentStatus.ACTIVE)
            self.assertEqual(
                repository.list_indexed_files(file_category="abucs_pdf")[0].related_doc_id,
                waste_transfer_notes[0].doc_id,
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
            worker_name="R. Davies",
            worker_company="Uplands",
            issued_date=date(2026, 3, 10),
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

    def _build_ladder_tagged_template(self, template_path: Path) -> None:
        document = Document()
        document.add_paragraph("Permit {{permit_number}} at {{site_name}}")
        document.add_paragraph("{{company_name}} / {{contractor_name}}")
        document.add_paragraph("{{op_name}}")
        document.add_paragraph("{{job_number}}")
        document.add_paragraph("{{date_issued}}")
        document.add_paragraph("{{task_description}}")
        document.add_paragraph("{{supervisor_name}}")
        for question_number in range(1, 12):
            document.add_paragraph(
                f"Q{question_number} {{{{q{question_number}_yes}}}} / {{{{q{question_number}_no}}}}"
            )
        document.add_paragraph("{{manager_name}}")
        document.add_paragraph("{{manager_signature}}")
        document.add_paragraph("{{ladder_id}}")
        table = document.add_table(rows=1, cols=7)
        table.cell(0, 0).text = "{{insp_date}}"
        table.cell(0, 1).text = "{{insp_name}}"
        table.cell(0, 2).text = "{{insp_rungs}}"
        table.cell(0, 3).text = "{{insp_stiles}}"
        table.cell(0, 4).text = "{{insp_feet}}"
        table.cell(0, 5).text = "{{insp_comments}}"
        table.cell(0, 6).text = "{{insp_ok}}"
        document.save(template_path)

    def test_template_manager_replaces_placeholders_for_ladder_permit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / "permit_template.docx"
            output_path = Path(temp_dir) / "permit_filled.docx"
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)

            self._build_ladder_tagged_template(template_path)

            try:
                TemplateRegistry.TEMPLATE_PATHS["ladder_permit"] = template_path
                manager = TemplateManager(self.build_permit())
                manager.render(output_path)
            finally:
                TemplateRegistry.TEMPLATE_PATHS = original_registry

            rendered = Document(output_path)
            self.assertIn("UHSF21.09-004", rendered.paragraphs[0].text)
            self.assertEqual(rendered.paragraphs[1].text, "Uplands / R. Davies")
            self.assertEqual(rendered.paragraphs[2].text, "R. Davies")
            self.assertIn("UP-24023", rendered.paragraphs[3].text)
            self.assertEqual(rendered.tables[0].cell(0, 1).text, "R. Davies")
            self.assertEqual(rendered.tables[0].cell(0, 6).text, "✔")

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


class PlantRegisterAutomationTests(unittest.TestCase):
    def _build_hss_order_confirmation_pdf(
        self,
        pdf_path: Path,
        *,
        order_ref: str = "H-3YXFCBWH",
        purchase_order: str = "81888",
        description: str = "DUST EXTRACTOR M CLASS 110V",
        start_date: str = "26/01/2026",
        end_date: str = "01/02/2026",
        quantity: int = 1,
    ) -> None:
        document = fitz.open()
        page = document.new_page()
        page.insert_text(
            (72, 72),
            "\n".join(
                [
                    "Order Confirmation",
                    "Product",
                    "1",
                    "UPLANDS RETAIL LTD",
                    "UP0037",
                    purchase_order,
                    order_ref,
                    "07970335636",
                    "ceri.edwards@uplandsretail.co.uk",
                    "Ceri Edwards",
                    "07970335636",
                    "PO8 0SJ",
                    "Waterlooville",
                    "National Grid",
                    "Sub Station",
                    description,
                    "52538",
                    f"{start_date} {end_date}*4dw",
                    "£25.00",
                    str(quantity),
                    "delivery",
                    "0161 749 4090",
                ]
            ),
        )
        document.save(pdf_path)
        document.close()

    def _build_plant_register_template(self, template_path: Path) -> None:
        document = Document()
        table = document.add_table(rows=2, cols=9)
        headers = [
            "Hire Number",
            "Plant description",
            "Hire Company",
            "Telephone number",
            "On Hire Date",
            "Who Hired",
            "Serial Number",
            "Last or Next LOLER Inspection",
            "Plant Certificate in H&S File 2",
        ]
        for column_index, header in enumerate(headers):
            table.cell(0, column_index).text = header
        table.cell(1, 0).text = "{% tr for p in plant_assets %}{{p.hire_num}}"
        table.cell(1, 1).text = "{{p.description}}"
        table.cell(1, 2).text = "{{p.company}}"
        table.cell(1, 3).text = "{{p.phone}}"
        table.cell(1, 4).text = "{{p.on_hire}}"
        table.cell(1, 5).text = "{{p.hired_by}}"
        table.cell(1, 6).text = "{{p.serial}}"
        table.cell(1, 7).text = "{{p.inspection}}"
        table.cell(1, 8).text = "{{p.in_file}}{% tr endfor %}"
        document.save(template_path)

    def _build_site_induction_template(self, template_path: Path) -> None:
        document = Document()
        document.add_paragraph("{{site_name}}")
        document.add_paragraph("{{date}}")
        document.add_paragraph("{{full_name}}")
        document.add_paragraph("{{company}}")
        document.add_paragraph("{{home_address}}")
        document.add_paragraph("{{contact_number}}")
        document.add_paragraph("{{occupation}}")
        document.add_paragraph("{{emergency_contact}}")
        document.add_paragraph("{{emergency_tel}}")
        document.add_paragraph("{{medical}}")
        document.add_paragraph("{{cscs_no}}")
        document.add_paragraph("{{first_aider}}")
        document.add_paragraph("{{fire_warden}}")
        document.add_paragraph("{{supervisor}}")
        document.add_paragraph("{{smsts}}")
        document.add_paragraph("{{inductor_name_date}}")
        document.add_paragraph("{{inductor_title}}")
        document.add_paragraph("{{signature_image}}")
        document.save(template_path)

    def test_file_and_index_all_creates_pending_plant_asset_from_hss_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            waste_destination = workspace_root / "FILE_1_Environment" / "Waste_Notes"
            carrier_docs_destination = workspace_root / "FILE_1_Environment" / "Carrier_Docs"
            waste_reports_destination = workspace_root / "FILE_1_Environment" / "Waste_Reports"
            attendance_destination = workspace_root / "FILE_2_Registers" / "Attendance"
            plant_hire_directory = workspace_root / "FILE_2_Registers" / "Plant_Hire_Register"
            induction_directory = workspace_root / "FILE_3_Inductions"
            inbox = workspace_root / "ingest"
            database_path = workspace_root / "documents.sqlite3"

            for directory in (
                waste_destination,
                carrier_docs_destination,
                waste_reports_destination,
                attendance_destination,
                plant_hire_directory,
                induction_directory,
                inbox,
            ):
                directory.mkdir(parents=True, exist_ok=True)

            (workspace_root / "project_setup.json").write_text(
                json.dumps(
                    {
                        "current_site_name": "NG Lovedean Substation",
                        "job_number": "JOB-4471",
                        "site_address": "Broadway Lane",
                        "client_name": "National Grid",
                    }
                ),
                encoding="utf-8",
            )

            contract_path = inbox / "Contract-H-3YXFCBWH.pdf"
            self._build_hss_order_confirmation_pdf(contract_path)

            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.INBOX,
                app_config.WASTE_DESTINATION,
                app_config.CARRIER_DOCS_DESTINATION,
                app_config.WASTE_REPORTS_DESTINATION,
                app_config.ATTENDANCE_DESTINATION,
                app_config.PLANT_HIRE_REGISTER_DIR,
                app_config.INDUCTION_DIR,
                app_config.DATABASE_PATH,
            )

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.INBOX = inbox
                app_config.WASTE_DESTINATION = waste_destination
                app_config.CARRIER_DOCS_DESTINATION = carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = attendance_destination
                app_config.PLANT_HIRE_REGISTER_DIR = plant_hire_directory
                app_config.INDUCTION_DIR = induction_directory
                app_config.DATABASE_PATH = database_path

                repository = DocumentRepository(database_path)
                file_and_index_all(repository)
                plant_assets = repository.list_documents(
                    document_type=PlantAssetDocument.document_type,
                )
            finally:
                (
                    app_config.BASE_DATA_DIR,
                    app_config.INBOX,
                    app_config.WASTE_DESTINATION,
                    app_config.CARRIER_DOCS_DESTINATION,
                    app_config.WASTE_REPORTS_DESTINATION,
                    app_config.ATTENDANCE_DESTINATION,
                    app_config.PLANT_HIRE_REGISTER_DIR,
                    app_config.INDUCTION_DIR,
                    app_config.DATABASE_PATH,
                ) = original_config

            self.assertEqual(len(plant_assets), 1)
            plant_asset = plant_assets[0]
            self.assertEqual(plant_asset.hire_num, "JOB-4471-01")
            self.assertEqual(plant_asset.description, "DUST EXTRACTOR M CLASS 110V")
            self.assertEqual(plant_asset.company, "HSS")
            self.assertEqual(plant_asset.phone, "0161 749 4090")
            self.assertEqual(plant_asset.on_hire, date(2026, 1, 26))
            self.assertEqual(plant_asset.source_reference, "H-3YXFCBWH")
            self.assertEqual(plant_asset.purchase_order, "81888")
            self.assertEqual(plant_asset.status, DocumentStatus.DRAFT)
            self.assertEqual(plant_asset.serial, "")
            self.assertEqual(plant_asset.inspection, "Pending serial / LOLER details")

    def test_generate_plant_register_document_renders_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_directory = Path(temp_dir) / "Plant_Hire_Register"
            database_path = Path(temp_dir) / "documents.sqlite3"
            template_path = Path(temp_dir) / "plant-register-template.docx"
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)
            original_plant_hire_directory = app_config.PLANT_HIRE_REGISTER_DIR
            repository = DocumentRepository(database_path)
            repository.create_schema()
            self._build_plant_register_template(template_path)

            repository.save(
                PlantAssetDocument(
                    doc_id="PLANT-001",
                    site_name="NG Lovedean Substation",
                    created_at=datetime(2026, 3, 12, 8, 0),
                    status=DocumentStatus.ACTIVE,
                    hire_num="JOB-4471-01",
                    description="DUST EXTRACTOR M CLASS 110V",
                    company="HSS",
                    phone="0161 749 4090",
                    on_hire=date(2026, 1, 26),
                    hired_by="TDE",
                    serial="SER-001",
                    inspection=(
                        "Exam 01/03/2026 | Next due 14/03/2026 | "
                        "Asset ID 001 | Report 123"
                    ),
                    source_reference="H-3YXFCBWH",
                    purchase_order="81888",
                )
            )

            try:
                TemplateRegistry.TEMPLATE_PATHS["plant_register"] = template_path
                app_config.PLANT_HIRE_REGISTER_DIR = output_directory
                generated = generate_plant_register_document(
                    repository,
                    site_name="NG Lovedean Substation",
                )
            finally:
                TemplateRegistry.TEMPLATE_PATHS = original_registry
                app_config.PLANT_HIRE_REGISTER_DIR = original_plant_hire_directory

            self.assertTrue(generated.output_path.exists())
            self.assertEqual(generated.asset_count, 1)

            rendered = Document(generated.output_path)
            self.assertEqual(rendered.tables[0].cell(1, 0).text, "JOB-4471-01")
            self.assertEqual(
                rendered.tables[0].cell(1, 1).text,
                "DUST EXTRACTOR M CLASS 110V",
            )
            self.assertEqual(rendered.tables[0].cell(1, 8).text, "Yes")

    def test_create_site_induction_document_renders_signature_and_logs_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            signatures_directory = workspace_root / "FILE_3_Safety" / "Signatures"
            completed_directory = workspace_root / "FILE_3_Safety" / "Completed_Inductions"
            database_path = workspace_root / "documents.sqlite3"
            template_path = Path(temp_dir) / "site_induction_template.docx"
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)
            original_config = (
                app_config.BASE_DATA_DIR,
                app_config.FILE_3_SIGNATURES_DIR,
                app_config.FILE_3_COMPLETED_INDUCTIONS_DIR,
                app_config.DATABASE_PATH,
            )

            self._build_site_induction_template(template_path)

            try:
                TemplateRegistry.TEMPLATE_PATHS["site_induction"] = template_path
                app_config.BASE_DATA_DIR = workspace_root
                app_config.FILE_3_SIGNATURES_DIR = signatures_directory
                app_config.FILE_3_COMPLETED_INDUCTIONS_DIR = completed_directory
                app_config.DATABASE_PATH = database_path

                repository = DocumentRepository(database_path)
                repository.create_schema()
                signature_image_data = np.full((200, 420, 4), 255, dtype=np.uint8)
                signature_image_data[92:98, 40:220, :3] = 0

                generated_document = create_site_induction_document(
                    repository,
                    site_name="NG Lovedean Substation",
                    full_name="Sean Carter",
                    home_address="1 Test Street",
                    contact_number="07123 456789",
                    company="A. Archer Electrical",
                    occupation="Electrician",
                    emergency_contact="Jane Carter",
                    emergency_tel="07999 888777",
                    medical="None declared",
                    cscs_number="CSCS-1234",
                    first_aider=True,
                    fire_warden=False,
                    supervisor=True,
                    smsts=False,
                    signature_image_data=signature_image_data,
                )
            finally:
                TemplateRegistry.TEMPLATE_PATHS = original_registry
                (
                    app_config.BASE_DATA_DIR,
                    app_config.FILE_3_SIGNATURES_DIR,
                    app_config.FILE_3_COMPLETED_INDUCTIONS_DIR,
                    app_config.DATABASE_PATH,
                ) = original_config

            self.assertTrue(generated_document.signature_path.exists())
            self.assertTrue(generated_document.output_path.exists())
            self.assertEqual(generated_document.signature_path.parent, signatures_directory)
            self.assertEqual(generated_document.output_path.parent, completed_directory)

            persisted_documents = repository.list_documents(
                document_type=InductionDocument.document_type,
                site_name="NG Lovedean Substation",
            )
            self.assertEqual(len(persisted_documents), 1)
            induction_document = persisted_documents[0]
            self.assertIsInstance(induction_document, InductionDocument)
            self.assertEqual(induction_document.individual_name, "Sean Carter")
            self.assertEqual(induction_document.contractor_name, "A. Archer Electrical")
            self.assertTrue(induction_document.first_aider)
            self.assertTrue(induction_document.supervisor)
            self.assertEqual(
                Path(induction_document.completed_document_path),
                generated_document.output_path,
            )

            rendered_document = Document(generated_document.output_path)
            rendered_text = "\n".join(
                paragraph.text for paragraph in rendered_document.paragraphs
            )
            self.assertIn("Sean Carter", rendered_text)
            self.assertIn("A. Archer Electrical", rendered_text)
            self.assertIn("1 Test Street", rendered_text)
            self.assertIn("Ceri Edwards", rendered_text)
            self.assertIn("Site Manager", rendered_text)
            self.assertIn(generated_document.induction_document.created_at.strftime("%d/%m/%Y"), rendered_text)

    def test_plant_asset_document_parses_inspection_due_date(self) -> None:
        plant_asset = PlantAssetDocument(
            doc_id="PLANT-002",
            site_name="NG Lovedean Substation",
            created_at=datetime(2026, 3, 12, 8, 0),
            status=DocumentStatus.ACTIVE,
            hire_num="JOB-4471-02",
            description="Rechargeable Worklight LED",
            company="HSS",
            phone="0161 749 4090",
            on_hire=date(2026, 3, 12),
            hired_by="TDE",
            serial="SER-002",
            inspection="Exam 12/03/2026 | Next due 18/03/2026 | Asset 52 | Report 900",
        )

        self.assertEqual(plant_asset.inspection_due_date(), date(2026, 3, 18))
        self.assertTrue(
            plant_asset.inspection_requires_attention(on_date=date(2026, 3, 12))
        )


class WasteRegisterAutomationTests(unittest.TestCase):
    def _build_waste_kpi_workbook(self, workbook_path: Path) -> None:
        import xlwt

        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet("Sheet1")
        sheet.write(6, 0, "Customer")
        sheet.write(6, 1, "Uplands")
        sheet.write(10, 0, "Project Number")
        sheet.write(10, 1, "JOB-4471")
        sheet.write(12, 1, "Project Name & Address")
        sheet.write(12, 3, "National Grid, Broadway Lane, Waterlooville, Hampshire, PO8 0SJ")
        sheet.write(16, 0, "Person responsible for waste management on site (incl. Job title)")
        sheet.write(16, 4, "Ceri Edwards")
        workbook.save(str(workbook_path))

    def _build_waste_register_template(self, template_path: Path) -> None:
        document = Document()
        document.add_paragraph("Client {{client_name}}")
        document.add_paragraph("Site {{site_address}}")
        document.add_paragraph("Manager {{manager_name}}")
        table = document.add_table(rows=2, cols=4)
        table.cell(0, 0).text = "Carrier"
        table.cell(0, 1).text = "Date"
        table.cell(0, 2).text = "Description"
        table.cell(0, 3).text = "Reg"
        table.cell(1, 0).text = "{% tr for w in waste_entries %}{{w.carrier}}"
        table.cell(1, 1).text = "{{w.date}}"
        table.cell(1, 2).text = "{{w.description}}"
        table.cell(1, 3).text = "{{w.reg_no}}{% tr endfor %}"
        document.save(template_path)

    def test_get_waste_kpi_sheet_metadata_reads_header_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            waste_reports_directory = Path(temp_dir) / "Waste_Reports"
            waste_reports_directory.mkdir(parents=True, exist_ok=True)
            workbook_path = waste_reports_directory / "PO8 0SJ - MAR 2026.xls"
            original_waste_reports_destination = app_config.WASTE_REPORTS_DESTINATION
            self._build_waste_kpi_workbook(workbook_path)

            try:
                app_config.WASTE_REPORTS_DESTINATION = waste_reports_directory
                metadata = get_waste_kpi_sheet_metadata(
                    site_name="NG Lovedean Substation",
                    site_address="Broadway Lane, Waterlooville, Hampshire, PO8 0SJ",
                    fallback_project_number="",
                )
            finally:
                app_config.WASTE_REPORTS_DESTINATION = original_waste_reports_destination

            self.assertEqual(metadata.client_name, "Uplands")
            self.assertEqual(metadata.project_number, "JOB-4471")
            self.assertIn("Broadway Lane", metadata.site_address)
            self.assertEqual(metadata.manager_name, "Ceri Edwards")

    def test_smart_scan_waste_transfer_note_extracts_pdf_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "documents.sqlite3"
            pdf_path = Path(temp_dir) / "31194.pdf"
            repository = DocumentRepository(database_path)
            repository.create_schema()

            pdf_document = fitz.open()
            pdf_page = pdf_document.new_page()
            pdf_page.insert_text(
                (72, 72),
                "\n".join(
                    [
                        "Carrier: L&S Waste Management Limited",
                        "Vehicle Reg: AB12 CDE",
                        "Waste Type: Mixed Construction Payment Type",
                        "Net Weight: 2400 kg",
                        "Date: 11/03/2026",
                        "17 09 04",
                    ]
                ),
            )
            pdf_document.save(pdf_path)
            pdf_document.close()

            scanned = smart_scan_waste_transfer_note(
                repository,
                source_path=pdf_path,
            )

            self.assertEqual(scanned.carrier_name, "Abucs")
            self.assertEqual(scanned.wtn_number, "31194")
            self.assertEqual(scanned.vehicle_registration, "AB12 CDE")
            self.assertEqual(scanned.waste_description, "Mixed Construction")
            self.assertEqual(scanned.quantity_tonnes, 2.4)
            self.assertEqual(scanned.ewc_code, "17 09 04")

    def test_file_and_index_all_backfills_weightless_abacus_ticket_into_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            waste_destination = workspace_root / "FILE_1_Environment" / "Waste_Notes"
            carrier_docs_destination = workspace_root / "FILE_1_Environment" / "Carrier_Docs"
            waste_reports_destination = workspace_root / "FILE_1_Environment" / "Waste_Reports"
            attendance_destination = workspace_root / "FILE_2_Registers" / "Attendance"
            induction_directory = workspace_root / "FILE_3_Inductions"
            inbox = workspace_root / "ingest"
            database_path = workspace_root / "documents.sqlite3"

            for directory in (
                waste_destination,
                carrier_docs_destination,
                waste_reports_destination,
                attendance_destination,
                induction_directory,
                inbox,
            ):
                directory.mkdir(parents=True, exist_ok=True)

            pdf_path = waste_destination / "30649.PDF"
            document = fitz.open()
            page = document.new_page()
            page.insert_text(
                (72, 72),
                "\n".join(
                    [
                        "ABACUS BRISTOL LTD",
                        "Ticket No.",
                        "19/01/2026",
                        "30649",
                        "Vehicle",
                        "Waste Type:Mixed Construction",
                        "17 09 04",
                        "Weight",
                    ]
                ),
            )
            document.save(pdf_path)
            document.close()

            original_base_data_dir = app_config.BASE_DATA_DIR
            original_inbox = app_config.INBOX
            original_waste_destination = app_config.WASTE_DESTINATION
            original_carrier_docs_destination = app_config.CARRIER_DOCS_DESTINATION
            original_waste_reports_destination = app_config.WASTE_REPORTS_DESTINATION
            original_attendance_destination = app_config.ATTENDANCE_DESTINATION
            original_plant_hire_directory = app_config.PLANT_HIRE_REGISTER_DIR
            original_induction_directory = app_config.INDUCTION_DIR
            original_database_path = app_config.DATABASE_PATH

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.INBOX = inbox
                app_config.WASTE_DESTINATION = waste_destination
                app_config.CARRIER_DOCS_DESTINATION = carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = attendance_destination
                app_config.PLANT_HIRE_REGISTER_DIR = workspace_root / "FILE_2_Registers" / "Plant_Hire_Register"
                app_config.INDUCTION_DIR = induction_directory
                app_config.DATABASE_PATH = database_path

                repository = DocumentRepository(database_path)
                file_and_index_all(repository)
                waste_transfer_notes = repository.list_documents(
                    document_type=WasteTransferNoteDocument.document_type,
                )
            finally:
                app_config.BASE_DATA_DIR = original_base_data_dir
                app_config.INBOX = original_inbox
                app_config.WASTE_DESTINATION = original_waste_destination
                app_config.CARRIER_DOCS_DESTINATION = original_carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = original_waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = original_attendance_destination
                app_config.PLANT_HIRE_REGISTER_DIR = original_plant_hire_directory
                app_config.INDUCTION_DIR = original_induction_directory
                app_config.DATABASE_PATH = original_database_path

            self.assertEqual(len(waste_transfer_notes), 1)
            self.assertEqual(waste_transfer_notes[0].wtn_number, "30649")
            self.assertEqual(waste_transfer_notes[0].carrier_name, "Abucs")
            self.assertEqual(waste_transfer_notes[0].vehicle_registration, "")
            self.assertEqual(waste_transfer_notes[0].quantity_tonnes, 0.0)

    def test_log_uploaded_waste_transfer_note_moves_file_and_updates_register(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            waste_destination = workspace_root / "FILE_1_Environment" / "Waste_Notes"
            database_path = workspace_root / "documents.sqlite3"
            source_path = Path(temp_dir) / "scan.pdf"
            source_path.write_bytes(b"%PDF-1.4")
            original_waste_destination = app_config.WASTE_DESTINATION
            repository = DocumentRepository(database_path)

            try:
                app_config.WASTE_DESTINATION = waste_destination
                logged = log_uploaded_waste_transfer_note(
                    repository,
                    upload_path=source_path,
                    original_filename="scan.pdf",
                    site_name="NG Lovedean Substation",
                    carrier_name="Abucs",
                    vehicle_registration="AB12 CDE",
                    waste_description="Mixed Construction",
                    ticket_date=date(2026, 3, 11),
                    quantity_tonnes=2.4,
                    ewc_code="17 09 04",
                    wtn_number="31194",
                )
            finally:
                app_config.WASTE_DESTINATION = original_waste_destination

            self.assertTrue(logged.stored_file_path.exists())
            self.assertFalse(source_path.exists())
            self.assertEqual(logged.waste_transfer_note.vehicle_registration, "AB12 CDE")
            self.assertIsNotNone(logged.register_document)
            self.assertEqual(len(logged.register_document.waste_transfer_notes), 1)

    def test_generate_waste_register_document_renders_and_indexes_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_directory = Path(temp_dir) / "output"
            database_path = Path(temp_dir) / "documents.sqlite3"
            template_path = Path(temp_dir) / "waste-register-template.docx"
            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)
            original_file_1_output_dir = app_config.FILE_1_OUTPUT_DIR
            repository = DocumentRepository(database_path)
            repository.create_schema()
            self._build_waste_register_template(template_path)

            repository.save(
                WasteTransferNoteDocument(
                    doc_id="WTN-31194",
                    site_name="NG Lovedean Substation",
                    created_at=datetime(2026, 3, 11, 8, 0),
                    status=DocumentStatus.ACTIVE,
                    wtn_number="31194",
                    date=date(2026, 3, 11),
                    waste_description="Mixed Construction",
                    ewc_code="17 09 04",
                    quantity_tonnes=2.4,
                    carrier_name="Abucs",
                    destination_facility="Not captured from ticket PDF",
                    vehicle_registration="AB12 CDE",
                )
            )

            try:
                TemplateRegistry.TEMPLATE_PATHS["waste_register"] = template_path
                app_config.FILE_1_OUTPUT_DIR = output_directory
                generated = generate_waste_register_document(
                    repository,
                    site_name="NG Lovedean Substation",
                    client_name="Uplands",
                    site_address="National Grid, Broadway Lane, Waterlooville, Hampshire, PO8 0SJ",
                    manager_name="Ceri Edwards",
                )
            finally:
                TemplateRegistry.TEMPLATE_PATHS = original_registry
                app_config.FILE_1_OUTPUT_DIR = original_file_1_output_dir

            self.assertTrue(generated.output_path.exists())
            self.assertEqual(generated.row_count, 1)

            rendered = Document(generated.output_path)
            self.assertIn("Client Uplands", rendered.paragraphs[0].text)
            self.assertEqual(rendered.tables[0].cell(1, 0).text, "Abucs")
            self.assertEqual(rendered.tables[0].cell(1, 3).text, "AB12 CDE / 31194")


class SafetyScannerAutomationTests(unittest.TestCase):
    def _build_word_document(self, document_path: Path, lines: list[str]) -> None:
        document = Document()
        for line in lines:
            document.add_paragraph(line)
        document.save(document_path)

    def test_file_and_index_all_imports_file_3_word_documents(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "Uplands_Workspace"
            inbox = workspace_root / "ingest"
            waste_destination = workspace_root / "FILE_1_Environment" / "Waste_Notes"
            carrier_docs_destination = workspace_root / "FILE_1_Environment" / "Carrier_Docs"
            waste_reports_destination = workspace_root / "FILE_1_Environment" / "Waste_Reports"
            attendance_destination = workspace_root / "FILE_2_Registers" / "Attendance"
            plant_hire_destination = workspace_root / "FILE_2_Registers" / "Plant_Hire_Register"
            induction_directory = workspace_root / "FILE_3_Inductions"
            rams_destination = workspace_root / "FILE_3_Safety" / "RAMS"
            coshh_destination = workspace_root / "FILE_3_Safety" / "COSHH"
            file_3_output_directory = workspace_root / "FILE_3_Safety" / "Registers"
            database_path = workspace_root / "documents.sqlite3"

            for directory in (
                inbox,
                waste_destination,
                carrier_docs_destination,
                waste_reports_destination,
                attendance_destination,
                plant_hire_destination,
                induction_directory,
                rams_destination,
                coshh_destination,
                file_3_output_directory,
            ):
                directory.mkdir(parents=True, exist_ok=True)

            (workspace_root / "project_setup.json").write_text(
                json.dumps(
                    {
                        "current_site_name": "NG Lovedean Substation",
                        "job_number": "81888",
                        "site_address": "Broadway Lane, Waterlooville, Hampshire, PO8 0SJ",
                        "client_name": "National Grid",
                    }
                ),
                encoding="utf-8",
            )

            rams_docx_path = inbox / "TDE Method Statement.docx"
            coshh_docx_path = inbox / "CT1 COSHH Assessment.docx"
            self._build_word_document(
                rams_docx_path,
                [
                    "Risk Assessment and Method Statement",
                    "Reference: RAMS-47",
                    "Version: 3.1",
                    "Activity Description: Cable tray installation in switch room",
                    "Contractor: TDE",
                    "Review Date: 12/03/2026",
                ],
            )
            self._build_word_document(
                coshh_docx_path,
                [
                    "COSHH Assessment",
                    "Substance Name: CT1 Sealant",
                    "Supplier: C-Tec",
                    "Reference: COSHH-12",
                    "Version: 2.0",
                    "Intended Use: General sealing works",
                    "Review Date: 12/03/2026",
                ],
            )

            original_base_data_dir = app_config.BASE_DATA_DIR
            original_inbox = app_config.INBOX
            original_waste_destination = app_config.WASTE_DESTINATION
            original_carrier_docs_destination = app_config.CARRIER_DOCS_DESTINATION
            original_waste_reports_destination = app_config.WASTE_REPORTS_DESTINATION
            original_attendance_destination = app_config.ATTENDANCE_DESTINATION
            original_plant_hire_directory = app_config.PLANT_HIRE_REGISTER_DIR
            original_induction_directory = app_config.INDUCTION_DIR
            original_rams_destination = app_config.RAMS_DESTINATION
            original_coshh_destination = app_config.COSHH_DESTINATION
            original_file_3_output_dir = app_config.FILE_3_OUTPUT_DIR
            original_database_path = app_config.DATABASE_PATH

            try:
                app_config.BASE_DATA_DIR = workspace_root
                app_config.INBOX = inbox
                app_config.WASTE_DESTINATION = waste_destination
                app_config.CARRIER_DOCS_DESTINATION = carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = attendance_destination
                app_config.PLANT_HIRE_REGISTER_DIR = plant_hire_destination
                app_config.INDUCTION_DIR = induction_directory
                app_config.RAMS_DESTINATION = rams_destination
                app_config.COSHH_DESTINATION = coshh_destination
                app_config.FILE_3_OUTPUT_DIR = file_3_output_directory
                app_config.DATABASE_PATH = database_path

                repository = DocumentRepository(database_path)
                filed_assets = file_and_index_all(repository)
                rams_documents = repository.list_documents(
                    document_type=RAMSDocument.document_type,
                )
                coshh_documents = repository.list_documents(
                    document_type=COSHHDocument.document_type,
                )
            finally:
                app_config.BASE_DATA_DIR = original_base_data_dir
                app_config.INBOX = original_inbox
                app_config.WASTE_DESTINATION = original_waste_destination
                app_config.CARRIER_DOCS_DESTINATION = original_carrier_docs_destination
                app_config.WASTE_REPORTS_DESTINATION = original_waste_reports_destination
                app_config.ATTENDANCE_DESTINATION = original_attendance_destination
                app_config.PLANT_HIRE_REGISTER_DIR = original_plant_hire_directory
                app_config.INDUCTION_DIR = original_induction_directory
                app_config.RAMS_DESTINATION = original_rams_destination
                app_config.COSHH_DESTINATION = original_coshh_destination
                app_config.FILE_3_OUTPUT_DIR = original_file_3_output_dir
                app_config.DATABASE_PATH = original_database_path

            self.assertEqual(len(rams_documents), 1)
            self.assertEqual(rams_documents[0].reference, "RAMS-47")
            self.assertEqual(rams_documents[0].version, "3.1")
            self.assertIn("Cable tray installation", rams_documents[0].activity_description)
            self.assertEqual(len(coshh_documents), 1)
            self.assertEqual(coshh_documents[0].reference, "COSHH-12")
            self.assertEqual(coshh_documents[0].version, "2.0")
            self.assertEqual(coshh_documents[0].substance_name, "CT1 Sealant")
            self.assertEqual(coshh_documents[0].manufacturer, "C-Tec")
            self.assertTrue((rams_destination / rams_docx_path.name).exists())
            self.assertTrue((coshh_destination / coshh_docx_path.name).exists())
            filed_categories = {asset.file_category for asset in filed_assets}
            self.assertIn("rams_docx", filed_categories)
            self.assertIn("coshh_docx", filed_categories)

    def test_safe_extract_word_text_reads_legacy_doc_via_conversion(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            converted_docx_path = Path(temp_dir) / "converted.docx"
            legacy_doc_path = Path(temp_dir) / "legacy.doc"
            legacy_doc_path.write_bytes(b"legacy")
            self._build_word_document(
                converted_docx_path,
                [
                    "COSHH Assessment",
                    "Reference: COSHH-21",
                    "Version: 4.0",
                ],
            )

            def _fake_convert(source_path: Path, destination_path: Path) -> None:
                self.assertEqual(source_path, legacy_doc_path)
                destination_path.write_bytes(converted_docx_path.read_bytes())

            with patch.object(
                workspace_module,
                "_convert_legacy_word_document_to_docx",
                side_effect=_fake_convert,
            ):
                extracted_text = workspace_module._safe_extract_word_text(legacy_doc_path)

            self.assertIn("COSHH Assessment", extracted_text)
            self.assertIn("Reference: COSHH-21", extracted_text)
            self.assertIn("Version: 4.0", extracted_text)

    def test_file_3_filename_first_parser_ignores_blacklisted_site_terms(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "documents.sqlite3"
            repository = DocumentRepository(database_path)
            repository.create_schema()
            source_path = Path(
                "NG-National Grid-Lovedean-(Bluecord)-RAMS-CCTV Camera Install-Rev 3.docx"
            )
            noisy_text = (
                "and people at risk "
                "and people at risk "
                "and people at risk "
                "and people at risk"
            )

            contractor_name = workspace_module._guess_file_3_contractor_name(
                repository,
                site_name="NG Lovedean Substation",
                pdf_text=noisy_text,
                source_path=source_path,
            )
            activity_description = workspace_module._extract_rams_activity_description(
                noisy_text,
                source_path,
            )
            version = workspace_module._extract_safety_version(
                noisy_text,
                source_path=source_path,
            )

            self.assertEqual(contractor_name, "Bluecord")
            self.assertEqual(activity_description, "CCTV Camera Install")
            self.assertEqual(version, "3")
            self.assertNotIn(contractor_name.casefold(), {"ng", "national grid", "lovedean"})

    def test_file_3_version_fallback_rejects_reviewed_text_noise(self) -> None:
        version = workspace_module._extract_safety_version(
            "Reviewed by Ceri Edwards\nReview Date: 12/03/2026\nVersion: 2a",
        )
        noisy_version = workspace_module._extract_safety_version(
            "Reviewed by Ceri Edwards\nReview Date: 12/03/2026",
        )

        self.assertEqual(version, "2a")
        self.assertEqual(noisy_version, "1.0")

    def test_file_3_text_fallback_rejects_blacklisted_contractor_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "documents.sqlite3"
            repository = DocumentRepository(database_path)
            repository.create_schema()

            contractor_name = workspace_module._guess_file_3_contractor_name(
                repository,
                site_name="NG Lovedean Substation",
                pdf_text="Client: National Grid\nSite: Lovedean Substation\nReviewed by NG",
                source_path=Path("RAMS_Method_Statement.docx"),
            )

            self.assertEqual(contractor_name, "Site Contractor")

    def test_file_3_anchor_labels_override_filename_parser(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "documents.sqlite3"
            repository = DocumentRepository(database_path)
            repository.create_schema()
            source_path = Path("NG-Lovedean-UHSF20.1-Review-Form-Rev-iewed.docx")
            anchored_text = (
                "COMPANY NAME:\nUplands\n"
                "RAMS TITLE:\nSwitch Room Cable Tray Install\n"
                "RAMS VERSION:\n03"
            )

            contractor_name = workspace_module._guess_file_3_contractor_name(
                repository,
                site_name="NG Lovedean Substation",
                pdf_text=anchored_text,
                source_path=source_path,
            )
            activity_description = workspace_module._extract_rams_activity_description(
                anchored_text,
                source_path,
            )
            version = workspace_module._extract_safety_version(
                anchored_text,
                source_path=source_path,
            )

            self.assertEqual(contractor_name, "Uplands")
            self.assertEqual(activity_description, "Switch Room Cable Tray Install")
            self.assertEqual(version, "03")

    def test_is_rams_safety_source_allows_review_forms_with_rams_anchors(self) -> None:
        self.assertFalse(
            workspace_module._is_rams_safety_source(
                Path("UHSF20.1 Review Form - Bluecord RAMS.docx"),
                "Risk Assessment and Method Statement",
            )
        )
        self.assertTrue(
            workspace_module._is_rams_safety_source(
                Path("UHSF20.1 Review Form - Bluecord RAMS.docx"),
                "RAMS TITLE: Cable Tray Install\nRisk Assessment and Method Statement",
            )
        )
        self.assertFalse(
            workspace_module._is_rams_safety_source(
                Path("Bluecord Review Form.docx"),
                "Review Form only",
            )
        )
        self.assertTrue(
            workspace_module._is_rams_safety_source(
                Path("Bluecord RAMS Rev 2.docx"),
                "Risk Assessment and Method Statement",
            )
        )


class WorkspaceDoctorTests(unittest.TestCase):
    def test_run_workspace_diagnostic_reports_healthy_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            workspace_root = project_root / "Uplands_Workspace"
            templates_dir = project_root / "templates"
            file_2_output_dir = workspace_root / "FILE_2_Output"
            signatures_dir = workspace_root / "FILE_3_Inductions" / "Signatures"
            completed_dir = workspace_root / "FILE_3_Inductions" / "Completed_Inductions"
            database_path = workspace_root / "documents.sqlite3"
            template_path = templates_dir / "UHSF16.01_Template.docx"

            for directory in (
                workspace_root,
                templates_dir,
                file_2_output_dir,
                signatures_dir,
                completed_dir,
            ):
                directory.mkdir(parents=True, exist_ok=True)
            database_path.touch()
            template_path.touch()

            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)
            with patch.object(app_config, "PROJECT_ROOT", project_root), patch.object(
                app_config, "BASE_DATA_DIR", workspace_root
            ), patch.object(app_config, "FILE_2_OUTPUT_DIR", file_2_output_dir), patch.object(
                app_config, "FILE_3_SIGNATURES_DIR", signatures_dir
            ), patch.object(
                app_config,
                "FILE_3_COMPLETED_INDUCTIONS_DIR",
                completed_dir,
            ), patch.object(
                app_config, "DATABASE_PATH", database_path
            ), patch.object(
                TemplateRegistry, "PROJECT_ROOT", project_root
            ):
                TemplateRegistry.TEMPLATE_PATHS = {
                    "site_induction": Path("templates/UHSF16.01_Template.docx")
                }
                try:
                    diagnostic_checks = run_workspace_diagnostic()
                finally:
                    TemplateRegistry.TEMPLATE_PATHS = original_registry

            self.assertTrue(diagnostic_checks)
            self.assertTrue(all(check.exists for check in diagnostic_checks))

    def test_run_workspace_diagnostic_reports_missing_template(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            project_root = Path(temp_dir)
            workspace_root = project_root / "Uplands_Workspace"
            templates_dir = project_root / "templates"
            file_2_output_dir = workspace_root / "FILE_2_Output"
            signatures_dir = workspace_root / "FILE_3_Inductions" / "Signatures"
            completed_dir = workspace_root / "FILE_3_Inductions" / "Completed_Inductions"
            database_path = workspace_root / "documents.sqlite3"

            for directory in (
                workspace_root,
                templates_dir,
                file_2_output_dir,
                signatures_dir,
                completed_dir,
            ):
                directory.mkdir(parents=True, exist_ok=True)
            database_path.touch()

            original_registry = dict(TemplateRegistry.TEMPLATE_PATHS)
            with patch.object(app_config, "PROJECT_ROOT", project_root), patch.object(
                app_config, "BASE_DATA_DIR", workspace_root
            ), patch.object(app_config, "FILE_2_OUTPUT_DIR", file_2_output_dir), patch.object(
                app_config, "FILE_3_SIGNATURES_DIR", signatures_dir
            ), patch.object(
                app_config,
                "FILE_3_COMPLETED_INDUCTIONS_DIR",
                completed_dir,
            ), patch.object(
                app_config, "DATABASE_PATH", database_path
            ), patch.object(
                TemplateRegistry, "PROJECT_ROOT", project_root
            ):
                TemplateRegistry.TEMPLATE_PATHS = {
                    "site_induction": Path("templates/UHSF16.01_Template.docx")
                }
                try:
                    diagnostic_checks = run_workspace_diagnostic()
                finally:
                    TemplateRegistry.TEMPLATE_PATHS = original_registry

            missing_checks = [check for check in diagnostic_checks if not check.exists]
            self.assertEqual(len(missing_checks), 1)
            self.assertEqual(
                missing_checks[0].display_path,
                "templates/UHSF16.01_Template.docx",
            )


class SiteInductionPosterTests(unittest.TestCase):
    def test_get_site_induction_url_uses_local_ip_and_induction_station(self) -> None:
        with patch.object(
            workspace_module,
            "get_local_ip_address",
            return_value="192.168.1.88",
        ):
            self.assertEqual(
                get_site_induction_url(),
                "http://192.168.1.88:8501/?station=induction",
            )

    def test_generate_site_induction_poster_returns_png_assets(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            logo_path = Path(temp_dir) / "uplands-logo.png"
            fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 50, 20), 0).save(str(logo_path))

            with patch.object(
                workspace_module,
                "get_local_ip_address",
                return_value="192.168.1.88",
            ):
                poster = generate_site_induction_poster(
                    site_name="NG Lovedean Substation",
                    logo_path=logo_path,
                )

        self.assertEqual(
            poster.induction_url,
            "http://192.168.1.88:8501/?station=induction",
        )
        self.assertTrue(poster.qr_code_png.startswith(b"\x89PNG\r\n\x1a\n"))
        self.assertTrue(poster.poster_png.startswith(b"\x89PNG\r\n\x1a\n"))


if __name__ == "__main__":
    unittest.main()
