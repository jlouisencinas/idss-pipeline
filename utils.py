# utils.py
import os
import re
import zipfile
import pdfplumber
import pandas as pd
from decimal import Decimal
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import requests
from datetime import datetime

# --- ZIP & PDF ---
def extract_zip(zip_path, extract_to, password: bytes):
    """Extract password-protected ZIP"""
    with zipfile.ZipFile(zip_path) as zf:
        zf.setpassword(password)
        zf.extractall(extract_to)

def clean_numbers(values):
    """Convert string numbers to Decimal strings"""
    result = []
    for val in values:
        parts = re.split(r"\s+", val) if isinstance(val, str) else [val]
        for part in parts:
            try:
                result.append(str(Decimal(part.replace(",", ""))))
            except:
                continue
    return result

def extract_and_clean_pdf(pdf_path, report_date):
    """Extract table and orphan rows, clean, deduplicate"""
    rows, orphan_rows = [], []

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            for table in page.extract_tables():
                for row in table:
                    if any(cell for cell in row):
                        rows.append([cell.strip() if cell else "" for cell in row])

            lines = page.extract_text().split("\n")[-6:]
            for line in lines:
                parts = line.strip().split()
                if len(parts) < 12 or not re.fullmatch(r"\d{7,}", parts[0]):
                    continue
                num_candidates = [p for p in parts if re.match(r"^-?\d[\d,]*\.?\d*$", p)]
                if len(num_candidates) < 8:
                    continue
                code = parts[0]
                numeric_parts = parts[-10:]
                name = " ".join(parts[1:-10])
                orphan_rows.append([code, name] + clean_numbers(numeric_parts))

    rows.extend(orphan_rows)

    # Cleaning and filtering rows
    cleaned = []
    for row in rows:
        if len(row) < 2 or not row[1].strip():
            continue
        if any(kw in row[1].upper() for kw in ["NAME", "DAILY", "MONTH-TO-DATE", "PRU LIFE UK", "SUMMARY"]):
            continue
        if any(row[1].startswith(prefix) for prefix in ("BM:", "DM:", "UM:", "AM:", "AUM", "RM:")):
            continue

        match = re.match(r"(\d{7,})\s+(.*)", row[1])
        if not match and len(row) > 2 and re.fullmatch(r"\d{7,}", row[0]):
            code, name = row[0], row[1]
        elif match:
            code, name = match.group(1), match.group(2).strip()
        else:
            continue
        if name.endswith("*"):
            continue

        numeric = clean_numbers(row[2:])
        cleaned_row = [code, name] + numeric
        cleaned_row = cleaned_row[:11] + [""] * (11 - len(cleaned_row)) if len(cleaned_row) < 11 else cleaned_row[:11]
        if cleaned_row[-1] == "":
            cleaned_row[-1] = cleaned_row[-2]
        cleaned.append(cleaned_row)

    df = pd.DataFrame(cleaned, columns=[
        "AGENT CODE", "AGENT NAME", "CC", "APE", "MTD CC", "MTD APE",
        "YTD APE", "LAPSES", "NAP", "YTD CC", "NET CC"
    ])
    df[df.columns[2:]] = df[df.columns[2:]].apply(pd.to_numeric, errors='coerce').fillna(0)
    df = df.drop_duplicates(subset=["AGENT CODE", "CC", "APE", "MTD CC", "MTD APE", "YTD APE", "LAPSES", "NAP", "YTD CC", "NET CC"])

    # Sort by longest name and group by agent code
    df["NAME LENGTH"] = df["AGENT NAME"].str.len()
    df = df.sort_values("NAME LENGTH", ascending=False).groupby("AGENT CODE", as_index=False).agg({
        "AGENT NAME": "first",
        "CC": "sum",
        "APE": "sum",
        "MTD CC": "sum",
        "MTD APE": "sum",
        "YTD APE": "sum",
        "LAPSES": "sum",
        "NAP": "sum",
        "YTD CC": "sum",
        "NET CC": "sum"
    })

    df.insert(0, "UNIT", "UNKNOWN")
    df[""] = ""
    df["  "] = ""
    df["REPORT_DATE"] = report_date
    return df[[
        "UNIT", "AGENT CODE", "AGENT NAME", "CC", "APE", "MTD CC",
        "MTD APE", "YTD APE", "LAPSES", "NAP", "YTD CC", "NET CC", "", "  ", "REPORT_DATE"
    ]]

# --- Google Drive Upload ---
def upload_to_drive(local_path, folder_id, secret_path, base_dir):
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(secret_path)
    cred_path = os.path.join(base_dir, "credentials.json")
    gauth.LoadCredentialsFile(cred_path)
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
        gauth.SaveCredentialsFile(cred_path)
    elif gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile(cred_path)
    else:
        gauth.Authorize()

    drive = GoogleDrive(gauth)
    file = drive.CreateFile({
        'title': os.path.basename(local_path),
        'parents': [{'id': folder_id}],
        'mimeType': 'text/csv'
    })
    file.SetContentFile(local_path)
    file.Upload()
    print(f"✅ Uploaded: {file['title']}")

# --- Trigger Apps Script ---
def trigger_app_script(webapp_url):
    try:
        response = requests.post(webapp_url)
        print("Apps Script response:", response.text)
    except Exception as e:
        print("Failed to trigger Apps Script:", e)

# --- File cleanup ---
def cleanup_download_folder(folder_path, keep_filename):
    import shutil
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        if item == keep_filename:
            continue
        if os.path.isfile(item_path):
            os.remove(item_path)
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)
        print(f"Deleted: {item}")

def extract_report_date(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            match = re.search(r"Production Report as of (\d{2}/\d{2}/\d{4})", text)
            if match:
                return datetime.strptime(match.group(1), "%m/%d/%Y").strftime("%B %d, %Y")
    return datetime.today().strftime("%B %d, %Y")