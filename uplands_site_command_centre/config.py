"""Workspace path configuration for the Uplands ops application."""

from pathlib import Path


BASE_DATA_DIR = Path("/Users/ceriedwards/uplands-ops/Uplands_Workspace")
INBOX = BASE_DATA_DIR / "ingest"
WASTE_DESTINATION = BASE_DATA_DIR / "FILE_1_Environment" / "Waste_Notes"
CARRIER_DOCS_DESTINATION = BASE_DATA_DIR / "FILE_1_Environment" / "Carrier_Docs"
WASTE_REPORTS_DESTINATION = BASE_DATA_DIR / "FILE_1_Environment" / "Waste_Reports"
ATTENDANCE_DESTINATION = BASE_DATA_DIR / "FILE_2_Registers" / "Attendance"
DATABASE_PATH = BASE_DATA_DIR / "documents.sqlite3"
