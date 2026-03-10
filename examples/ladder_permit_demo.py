"""End-to-end demo for the UHSF21.09 ladder permit workflow."""

from datetime import date, datetime, time
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from uplands_site_command_centre.permits import (
    DocumentRepository,
    DocumentStatus,
    LadderPermit,
    LadderStabilisationMethod,
    TemplateManager,
    TemplateValidationError,
)


def build_demo_permit() -> LadderPermit:
    """Create a representative ladder permit record for local testing."""

    permit = LadderPermit(
        doc_id="LP-2026-0001",
        site_name="Uplands - Cardiff North",
        created_at=datetime(2026, 3, 10, 9, 30),
        status=DocumentStatus.DRAFT,
        permit_number="UHSF21.09-0001",
        project_name="Cardiff North Retail Refurbishment",
        project_number="UP-24017",
        location_of_work="North elevation access bay",
        description_of_work="Short-duration cable tray inspection using a step ladder.",
        valid_from_date=date(2026, 3, 10),
        valid_from_time=time(10, 0),
        valid_to_date=date(2026, 3, 10),
        valid_to_time=time(18, 0),
        safer_alternative_eliminated=True,
        task_specific_rams_prepared_and_approved=True,
        personnel_briefed_and_understand_task=True,
        competent_supervisor_appointed=True,
        competent_supervisor_name="A. Hughes",
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
        inspected_by="A. Hughes",
        rungs_ok=True,
        stiles_ok=True,
        feet_ok=True,
        comments_or_action_taken="Pre-use inspection complete.",
    )
    return permit


def main() -> None:
    permit = build_demo_permit()

    repository = DocumentRepository("data/documents.sqlite3")
    repository.create_schema()
    repository.save(permit)

    loaded = repository.get(permit.doc_id)
    if not isinstance(loaded, LadderPermit):
        raise TypeError("Expected the stored document to rehydrate as LadderPermit.")

    output_path = Path("output/drafts") / f"{loaded.doc_id}-draft.docx"
    try:
        draft_path = TemplateManager(loaded).render(output_path)
    except TemplateValidationError as exc:
        raise SystemExit(f"Template validation failed: {exc}")

    print(loaded.to_json())
    print()
    print("Saved SQLite record:", loaded.doc_id)
    print("Logical file path:", loaded.get_file_path())
    print("Draft DOCX:", draft_path)


if __name__ == "__main__":
    main()
