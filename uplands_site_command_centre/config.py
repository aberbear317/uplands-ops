"""Workspace path configuration for the Uplands ops application."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASE_DATA_DIR = Path("/Users/ceriedwards/uplands-ops/Uplands_Workspace")
INBOX = BASE_DATA_DIR / "ingest"
WASTE_DESTINATION = BASE_DATA_DIR / "FILE_1_Environment" / "Waste_Notes"
CARRIER_DOCS_DESTINATION = BASE_DATA_DIR / "FILE_1_Environment" / "Carrier_Docs"
WASTE_REPORTS_DESTINATION = BASE_DATA_DIR / "FILE_1_Environment" / "Waste_Reports"
FILE_1_OUTPUT_DIR = BASE_DATA_DIR / "FILE_1_Environment" / "Waste_Register_Output"
ATTENDANCE_DESTINATION = BASE_DATA_DIR / "FILE_2_Registers" / "Attendance"
PLANT_HIRE_REGISTER_DIR = BASE_DATA_DIR / "FILE_2_Registers" / "Plant_Hire_Register"
TOOLBOX_TALK_REGISTER_DIR = BASE_DATA_DIR / "FILE_2_Registers" / "Toolbox_Talk_Register"
FILE_2_OUTPUT_DIR = BASE_DATA_DIR / "FILE_2_Output"
FILE_2_CHECKLIST_OUTPUT_DIR = FILE_2_OUTPUT_DIR / "FILE_2_Checklists"
INDUCTION_DIR = BASE_DATA_DIR / "FILE_3_Inductions"
FILE_3_SAFETY_DIR = INDUCTION_DIR
RAMS_DESTINATION = FILE_3_SAFETY_DIR / "RAMS"
COSHH_DESTINATION = FILE_3_SAFETY_DIR / "COSHH"
FILE_3_SIGNATURES_DIR = FILE_3_SAFETY_DIR / "Signatures"
FILE_3_COMPLETED_INDUCTIONS_DIR = FILE_3_SAFETY_DIR / "Completed_Inductions"
FILE_3_OUTPUT_DIR = FILE_3_SAFETY_DIR / "Registers"
PERMITS_DESTINATION = BASE_DATA_DIR / "FILE_4_Permits"
DATABASE_PATH = BASE_DATA_DIR / "documents.sqlite3"
