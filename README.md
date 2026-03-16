# Site Command Centre

Site Command Centre is a macOS-first Streamlit application for running a live construction site paperwork stack across the standard Uplands File 1-4 structure.

It combines:

- manager-facing operational dashboards
- a locked mobile kiosk for induction and attendance
- Word document generation from tagged templates
- local file storage inside the site workspace
- SQLite-backed typed records
- Cloudflare tunnel access for phones on site

The app is designed to replace fragmented paper handling with a single live control point while still producing printable records for the physical site file.

## What the app covers

### File 1: Environment & Waste

- waste note filing and indexing
- waste register output
- carrier compliance support
- incident and environmental reporting support

### File 2: Registers & Diary

- UHSF19.1 daily and weekly checklist workflow
- UHSF16.09 daily attendance register
- UHSF15.63 daily site diary
- plant register
- toolbox talk register and remote signing
- attendance register printing

### File 3: Contractor Master

- induction records and completed induction packs
- competency card storage
- safety document vault for RAMS, COSHH, review packs, and archive material

### File 4: Permits & Temporary Works

- permit workflows including ladder permit generation
- permit register support
- worker/company resolution from roster, attendance, and induction history

### Live operations

- manager-only broadcast hub
- SMS / Messages launch flow for active personnel on site
- remote toolbox talk distribution and signing
- live fire roll
- compliance radar for expiring competency cards

## Core operating model

The app separates into two modes:

### Manager mode

This is the desktop control surface for the site manager.

It is used for:

- daily administration
- document generation
- attendance oversight
- exports and printing
- project setup
- broadcast and toolbox talk control

### Kiosk mode

This is the mobile gate flow, exposed through the induction tunnel URL.

It is used for:

- daily sign-in
- daily sign-out
- first-time operative induction
- GPS / gate verification

Kiosk mode is intentionally restricted so manager tools and dashboards do not appear when a phone is using the on-site QR workflow.

## Main features

### Attendance and induction

- UHSF16.09 attendance console with live sign-in and sign-out
- UHSF16.01 induction form with signature capture
- first-time-on-site routing from attendance to induction and back again
- stored induction files, signatures, and competency evidence
- one-click induction pack opening for physical filing
- reset tools for saved attendance and saved induction records

### Geo-fence and gate access

- configurable site latitude, longitude, and fence radius
- site location can be set manually, by postcode, or by current device location
- known sites memory for switching between previously used projects
- dedicated top-level GPS helper page for mobile browsers
- session trust and gate verification tracking on attendance entries

### Daily diary

- UHSF15.63 diary station with auto-filled contractor headcount from live attendance
- editable contractor and visitor tables
- incidents, handovers, and comments capture
- voice dictation helper flow for long text fields
- DOCX generation into the File 2 diary output folder

### Broadcasts and toolbox talks

- active audience built from the live fire roll
- broadcast composer with reusable presets and recent site messages
- Apple Messages-first launch flow for SMS drafts
- toolbox talk creation with uploaded source document
- mobile TBT signing workflow for active on-site operatives
- UHSF16.2 register export from collected signatures

### Word template generation

The app uses tagged `.docx` templates stored under [`templates/`](./templates) and fills them with `docxtpl`.

Current generated outputs include:

- waste register
- attendance register
- daily and weekly checklist
- daily site diary
- induction document
- toolbox talk register
- permit outputs

## Project structure

```text
.
|-- app.py
|-- START_UPLANDS.command
|-- gps_server.py
|-- gps/
|   |-- geo-capture.html
|   |-- voice-capture.html
|   `-- uplands-logo.png
|-- static/
|-- templates/
|-- tests/
|-- uplands_site_command_centre/
|   |-- __init__.py
|   |-- config.py
|   |-- workspace.py
|   `-- permits/
|       |-- ingestion_engine.py
|       |-- models.py
|       `-- repository.py
`-- Uplands_Workspace/
```

### Important files

- [`app.py`](./app.py)
  Main Streamlit application. This contains manager UI, kiosk UI, routing, styling, and station renderers.

- [`uplands_site_command_centre/workspace.py`](./uplands_site_command_centre/workspace.py)
  File handling, document generation, poster creation, broadcast helpers, and site/workspace logic.

- [`uplands_site_command_centre/permits/models.py`](./uplands_site_command_centre/permits/models.py)
  Typed document and record models used across the system.

- [`uplands_site_command_centre/permits/repository.py`](./uplands_site_command_centre/permits/repository.py)
  SQLite repository and persistence layer.

- [`uplands_site_command_centre/permits/ingestion_engine.py`](./uplands_site_command_centre/permits/ingestion_engine.py)
  KPI backup ingestion into the attendance register.

- [`gps_server.py`](./gps_server.py)
  Lightweight local helper server for the GPS and voice helper pages. This is important because mobile Safari and microphone / geolocation flows behave better from a top-level helper page than from an iframe-style embedded route.

- [`START_UPLANDS.command`](./START_UPLANDS.command)
  Local launcher that starts the helper server, Cloudflare tunnel, and Streamlit app.

## Data layout

Site data is stored under [`Uplands_Workspace/`](./Uplands_Workspace), which is intentionally ignored from Git.

Key folders are configured in [`uplands_site_command_centre/config.py`](./uplands_site_command_centre/config.py):

- `FILE_1_Environment/`
- `FILE_2_Registers/Attendance/`
- `FILE_2_Registers/Toolbox_Talk_Register/`
- `FILE_2_Output/FILE_2_Checklists/`
- `FILE_2_Output/FILE_2_Daily_Site_Diary/`
- `FILE_3_Inductions/`
- `FILE_3_Inductions/Competency_Cards/`
- `FILE_3_Inductions/Completed_Inductions/`
- `FILE_4_Permits/`
- `documents.sqlite3`

Tracked source code lives in Git. Generated site data does not.

## Requirements

This project currently assumes:

- macOS
- Python 3.9+
- Cloudflare `cloudflared`
- Apple Messages available if using the SMS broadcast launcher

Python packages are listed in [`requirements.txt`](./requirements.txt).

## Local setup

### 1. Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

### 2. Ensure Cloudflare tunnel access is already set up

The launcher expects a named tunnel called:

```bash
uplands-site-induction
```

and a valid Cloudflare config at:

```bash
$HOME/.cloudflared/config.yml
```

If the machine has not been authenticated before:

```bash
cloudflared tunnel login
```

### 3. Start the app

The supported launcher is:

```bash
./START_UPLANDS.command
```

That script:

- kills old `cloudflared` ghosts
- kills any old helper server on port `8502`
- starts [`gps_server.py`](./gps_server.py)
- starts the named Cloudflare tunnel
- launches Streamlit

## Manual run commands

If you need to run the pieces manually:

```bash
python3 gps_server.py
```

```bash
cloudflared tunnel --config "$HOME/.cloudflared/config.yml" run uplands-site-induction
```

```bash
python3 -m streamlit run app.py
```

## Public access

The permanent induction kiosk URL is configured in [`uplands_site_command_centre/config.py`](./uplands_site_command_centre/config.py):

```text
https://uplands-site-induction.omegaleague.win/?station=induction&mode=kiosk
```

This is what the induction poster QR code points to.

## Testing

The project uses `unittest`.

Run the full suite:

```bash
python3 -m unittest tests.test_permits
```

Run one area while working:

```bash
python3 -m unittest tests.test_permits -k site_induction -v
```

```bash
python3 -m unittest tests.test_permits -k attendance -v
```

## Notes on mobile helper pages

Two browser helper pages live under [`gps/`](./gps):

- `geo-capture.html`
- `voice-capture.html`

These pages exist because some mobile browser features are much more reliable from a top-level page than from inside the Streamlit app shell.

They are used for:

- geo-fence capture
- dictation / speech capture

## Operational notes

### Site memory

The app keeps a memory of previously used sites so project setup can switch between known locations instead of retyping details every time.

### KPI backup ingestion

Attendance can be rebuilt from KPI backup JSON files. The importer supports the real-world KPI formats currently seen in backups, including:

- `YYYY-MM-DD`
- `DD/MM/YYYY`
- `YYYY/MM/DD`
- short `DD/MM` dates when the full year is recoverable from the KPI row id

### Safety vault

File 3 currently operates as a document-first contractor and safety vault rather than a fully automated RAMS/COSHH intelligence engine. This is deliberate: for live projects, explicit filed documents are safer than overconfident parsing.

## Known constraints

- The app is currently optimized for macOS operations.
- Some features rely on Apple-side tooling, especially the Messages launch flow.
- Browser GPS behavior varies by Safari / Chrome / Android WebView. The system includes helper pages and fallback paths, but mobile browser quirks still need real-device testing when deploying to a new site setup.
- Generated site data is intentionally not stored in Git.

## Recommended operator workflow

1. Set the active site in `Project Setup`.
2. Confirm geo-fence coordinates and radius.
3. Launch the command centre via [`START_UPLANDS.command`](./START_UPLANDS.command).
4. Verify the tunnel and helper server are live.
5. Use the kiosk QR for attendance and induction.
6. Use manager mode for diary, broadcast, TBT, exports, and pack printing.
7. Print and file the required paper outputs where the physical site file still needs them.

## Development guidance

- Prefer adding typed behavior in `models.py` and `workspace.py` rather than burying business rules inside the UI.
- Preserve the manager / kiosk separation.
- Treat File 3 safety records carefully. For safety-critical data, explicit beats clever.
- Avoid destructive Git operations in the site workspace.

## License / ownership

This repository appears to be a private project workspace for Uplands operational tooling. No open-source license is declared in the repository at the time of writing.
