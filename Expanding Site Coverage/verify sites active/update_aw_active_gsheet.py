"""
update_aw_active_gsheet.py
--------------------------
Reads AW_civic_scraper_sites2026-03-02.xlsx, finds all sites whose
'Date Last Updated' falls in 2026, then sets aw_active = 'yes' for
those rows in the Google Sheet:
  https://docs.google.com/spreadsheets/d/14mfVh7zL-uuzkqBbf2EwgNs07Lj4E9n2WKY727c0CYI

Prerequisites:
    uv pip install gspread google-auth openpyxl
    gcloud auth application-default login
"""

from datetime import datetime
import openpyxl
import gspread
from google.auth import default

# ── Config ─────────────────────────────────────────────────────────────────────
EXCEL_PATH     = "batch_processing/AW_civic_scraper_sites2026-03-02.xlsx"
SPREADSHEET_ID = "14mfVh7zL-uuzkqBbf2EwgNs07Lj4E9n2WKY727c0CYI"
WORKSHEET_NAME = "AW_civic_scraper_sites"
MATCH_COL      = "Name"       # column in Excel used to match rows in the sheet
AW_ACTIVE_COL  = "aw_active"  # column in the sheet to update
NEW_VALUE      = "yes"


# ── Step 1: collect 2026 site names from Excel ─────────────────────────────────
print(f"Reading {EXCEL_PATH} …")
wb = openpyxl.load_workbook(EXCEL_PATH)
ws_xl = wb.active

headers = [cell.value for cell in ws_xl[2]]  # row 1 is a title, row 2 has headers
name_col_idx = headers.index(MATCH_COL)      # 0-based
date_col_idx = headers.index("Date Last Updated")

names_2026 = set()
for row in ws_xl.iter_rows(min_row=3, values_only=True):
    date_val = row[date_col_idx]
    if isinstance(date_val, datetime) and date_val.year == 2026:
        name = row[name_col_idx]
        if name:
            names_2026.add(str(name).strip())

print(f"Found {len(names_2026)} sites with a 2026 'Date Last Updated'.")


# ── Step 2: connect to Google Sheets ──────────────────────────────────────────
print("Connecting to Google Sheets …")
creds, _ = default(scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(creds)

sh = gc.open_by_key(SPREADSHEET_ID)
ws_gs = sh.worksheet(WORKSHEET_NAME)
print(f"Opened worksheet: '{WORKSHEET_NAME}'")


# ── Step 3: read all data and locate columns ───────────────────────────────────
all_values = ws_gs.get_all_values()
gs_headers = all_values[0]

try:
    name_gs_col   = gs_headers.index(MATCH_COL) + 1        # 1-based for Sheets API
    aw_active_col = gs_headers.index(AW_ACTIVE_COL) + 1
except ValueError as e:
    raise RuntimeError(f"Column not found in sheet headers: {e}\nHeaders: {gs_headers}")

print(f"  '{MATCH_COL}' is column {name_gs_col}, '{AW_ACTIVE_COL}' is column {aw_active_col}")


# ── Step 4: build batch update ─────────────────────────────────────────────────
updates = []   # list of (row_number, col_number, value)

for row_idx, row in enumerate(all_values[1:], start=2):  # skip header, 1-based rows
    cell_name = row[name_gs_col - 1].strip() if len(row) >= name_gs_col else ""
    if cell_name in names_2026:
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_idx, aw_active_col),
            "values": [[NEW_VALUE]],
        })

print(f"Rows to update: {len(updates)}")

if not updates:
    print("Nothing to update — check that the Name values match between the Excel and sheet.")
else:
    # Batch update in one API call (much faster than cell-by-cell)
    ws_gs.batch_update(updates, value_input_option="RAW")
    print(f"Done! Set '{AW_ACTIVE_COL}' = '{NEW_VALUE}' for {len(updates)} rows.")
