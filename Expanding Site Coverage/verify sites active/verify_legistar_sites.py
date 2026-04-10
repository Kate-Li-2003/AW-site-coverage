"""
verify_legistar_sites.py
------------------------
Runs live HTTP verification against every site in Copy_of_Legistrar_sites.xlsx
(sheet: 'legistar') and writes TRUE/FALSE to an 'aw_active' column.

The 'endpoint' column contains bare hostnames/paths like:
    parkland.legistar.com/Calendar.aspx
This script prepends 'https://' to build the full URL.

Run:
    uv run --with requests --with openpyxl --with pandas python3 verify_legistar_sites.py
"""

import io
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import openpyxl
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Config ─────────────────────────────────────────────────────────────────────
EXCEL_PATH   = "Copy_of_Legistrar_sites.xlsx"
SHEET_NAME   = "legistar"
MAX_WORKERS  = 30
TIMEOUT      = 12

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LEGISTAR_FINGERPRINTS = ["legistar", "granicus", "calendar.aspx"]


# ── Verification ───────────────────────────────────────────────────────────────

def build_url(endpoint: str) -> str:
    endpoint = endpoint.strip()
    if endpoint.startswith("http"):
        return endpoint
    return f"https://{endpoint}"


def verify_legistar(url: str) -> bool:
    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=TIMEOUT,
            allow_redirects=True, verify=False,
        )
        if resp.status_code != 200:
            return False
        html = resp.text.lower()
        return any(fp in html for fp in LEGISTAR_FINGERPRINTS)
    except Exception:
        return False


def run_verification() -> None:
    print(f"Reading {EXCEL_PATH} (sheet: '{SHEET_NAME}') …")
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

    # Drop rows where endpoint is blank
    df = df[df["endpoint"].notna() & (df["endpoint"].astype(str).str.strip() != "")]
    n = len(df)
    print(f"{n} sites to verify\n")

    results: dict[int, bool] = {}

    def _task(idx: int, endpoint: str) -> tuple[int, bool]:
        url = build_url(str(endpoint))
        return idx, verify_legistar(url)

    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_task, i, row["endpoint"]): i
            for i, row in df.iterrows()
        }
        done = 0
        for fut in as_completed(futures):
            idx, ok = fut.result()
            results[idx] = ok
            done += 1
            if done % 25 == 0 or done == n:
                pct    = done / n * 100
                active = sum(results.values())
                elapsed = time.time() - start
                print(
                    f"  [{done:4d}/{n}]  {pct:5.1f}%  "
                    f"active so far: {active}  "
                    f"elapsed: {elapsed:.0f}s"
                )

    active_count = sum(results.values())
    print(f"\nDone — {active_count}/{n} sites verified active ({time.time()-start:.1f}s)\n")

    _write_results(results)
    print(f"Saved → '{EXCEL_PATH}'\n")


# ── Write aw_active back to the Excel file ─────────────────────────────────────

def _get_sheet_zip_path(excel_path: str, sheet_name: str) -> str:
    with zipfile.ZipFile(excel_path) as z:
        wb_xml   = z.read("xl/workbook.xml").decode("utf-8")
        rels_xml = z.read("xl/_rels/workbook.xml.rels").decode("utf-8")
    m = re.search(
        r'<sheet\b[^>]*\bname="' + re.escape(sheet_name) + r'"[^>]*\br:id="([^"]+)"',
        wb_xml,
    )
    if not m:
        raise RuntimeError(f"Sheet '{sheet_name}' not found in workbook.")
    rid = m.group(1)
    t = re.search(
        r'<Relationship\b[^>]*\bId="' + re.escape(rid) + r'"[^>]*\bTarget="([^"]+)"',
        rels_xml,
    )
    if not t:
        raise RuntimeError(f"Relationship '{rid}' not found.")
    target = t.group(1)
    return f"xl/{target}" if not target.startswith("xl/") else target


def _write_results(verdicts: dict[int, bool]) -> None:
    """
    Adds/updates the 'aw_active' column in the sheet using direct ZIP/XML patching
    (avoids openpyxl failing on pivot-cache XML issues).
    """
    # Read headers to find or determine aw_active column position
    df_hdr = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, nrows=0)
    cols = list(df_hdr.columns)
    if "aw_active" in cols:
        aw_col_idx = cols.index("aw_active") + 1  # 1-based
    else:
        aw_col_idx = len(cols) + 1  # append after last column

    # Convert 1-based index to Excel column letter
    def _col_letter(n: int) -> str:
        letter = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            letter = chr(ord("A") + r) + letter
        return letter

    aw_col = _col_letter(aw_col_idx)
    sheet_key = _get_sheet_zip_path(EXCEL_PATH, SHEET_NAME)

    def make_bool_cell(row_num: int, value: bool) -> str:
        v = "1" if value else "0"
        return f'<c r="{aw_col}{row_num}" s="1" t="b"><v>{v}</v></c>'

    def make_header_cell(row_num: int) -> str:
        # Inline string for the header "aw_active"
        return (
            f'<c r="{aw_col}{row_num}" t="inlineStr">'
            f'<is><t>aw_active</t></is></c>'
        )

    buf = io.BytesIO()
    with zipfile.ZipFile(EXCEL_PATH, "r") as zin, \
         zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)
            if item.filename == sheet_key:
                xml   = data.decode("utf-8")
                parts = []
                pos   = 0
                i_pat = re.compile(
                    rf'<c r="{aw_col}(\d+)"[^>]*>.*?</c>', re.DOTALL
                )
                for row_m in re.finditer(
                    r'(<row[^>]+r="(\d+)"[^>]*>)(.*?)(</row>)', xml, re.DOTALL
                ):
                    row_num  = int(row_m.group(2))
                    row_inner = row_m.group(3)

                    if row_num == 1:
                        # Header row — add/replace aw_active header cell
                        new_cell = make_header_cell(row_num)
                    else:
                        df_idx = row_num - 2  # 0-based pandas index
                        if df_idx not in verdicts:
                            parts.append(xml[pos:row_m.end()])
                            pos = row_m.end()
                            continue
                        new_cell = make_bool_cell(row_num, verdicts[df_idx])

                    if i_pat.search(row_inner):
                        new_inner = i_pat.sub(new_cell, row_inner)
                    else:
                        next_col  = _col_letter(aw_col_idx + 1)
                        insert_at = re.search(
                            r'<c r="[' + next_col + r'-Z]\d+"', row_inner
                        )
                        ip        = insert_at.start() if insert_at else len(row_inner)
                        new_inner = row_inner[:ip] + new_cell + row_inner[ip:]

                    parts.append(xml[pos:row_m.start()])
                    parts.append(row_m.group(1) + new_inner + row_m.group(4))
                    pos = row_m.end()

                parts.append(xml[pos:])
                data = "".join(parts).encode("utf-8")
            zout.writestr(item, data)

    with open(EXCEL_PATH, "wb") as f:
        f.write(buf.getvalue())


if __name__ == "__main__":
    run_verification()
