import os
import re
import zipfile
import pdfplumber
import pandas as pd
from datetime import datetime
from decimal import Decimal
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# === CONFIGURATION ===
WORKING_DIR = r"D:\My Files\LKL Reports\IDSS Automation\downloads"
ZIP_PASSWORD = b"5fb85964"
DRIVE_FOLDER_ID = "1O00CGI9zSzsbGK4S_n3bTAehJ-TUcvrX"
base_dir = r"D:\My Files\LKL Reports\IDSS Automation"
CLIENT_SECRET_PATH = os.path.join(base_dir, "client_secrets.json")

# === STEP 1: Extract ZIP ===
def extract_zip(zip_path, extract_to, password):
    with zipfile.ZipFile(zip_path) as zf:
        zf.setpassword(password)
        zf.extractall(extract_to)

# === STEP 2: Extract Report Date ===
def extract_report_date(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            match = re.search(r"Production Report as of (\d{2}/\d{2}/\d{4})", text)
            if match:
                return datetime.strptime(match.group(1), "%m/%d/%Y").strftime("%B %d, %Y")
    return datetime.today().strftime("%B %d, %Y")

# === STEP 3: Clean and Format Data ===
def clean_numbers(values):
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
    rows = []
    orphan_rows = []

    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            for table in page.extract_tables():
                for row in table:
                    if any(cell for cell in row):
                        rows.append([cell.strip() if cell else "" for cell in row])

            text = page.extract_text()
            lines = text.split("\n")[-6:]

            for line in lines:
                if not isinstance(line, str):
                    continue
                parts = line.strip().split()
                if len(parts) < 12:
                    continue
                if not re.fullmatch(r"\d{7,}", parts[0]):
                    continue
                num_candidates = [p for p in parts if re.match(r"^-?\d[\d,]*\.?\d*$", p)]
                if len(num_candidates) < 8:
                    continue

                code = parts[0]
                numeric_parts = parts[-10:]
                name_parts = parts[1:-10]
                name = " ".join(name_parts)
                row = [code, name] + clean_numbers(numeric_parts)
                orphan_rows.append(row)
                print(f"🆗 Captured orphan row from page {i + 1}: {row}")

    rows.extend(orphan_rows)

    cleaned = []
    for row in rows:
        if len(row) < 2 or not row[1].strip():
            continue
        if any(kw in row[1].upper() for kw in ["NAME", "DAILY", "MONTH-TO-DATE", "PRU LIFE UK", "SUMMARY"]):
            continue
        if any(row[1].startswith(prefix) for prefix in ("BM:", "DM:", "UM:", "AM:", "AUM", "RM:")):
            continue
        if row[:3] == ["AGENT CODE", "AGENT NAME", "CC"]:
            continue

        match = re.match(r"(\d{7,})\s+(.*)", row[1])
        if not match:
            if len(row) > 2 and re.fullmatch(r"\d{7,}", row[0]):
                code, name = row[0], row[1]
            else:
                continue
        else:
            code, name = match.group(1), match.group(2).strip()

        if name.endswith("*"):
            continue

        numeric = clean_numbers(row[2:])
        cleaned_row = [code, name] + numeric
        cleaned_row = cleaned_row[:11] + [""] * (11 - len(cleaned_row)) if len(cleaned_row) < 11 else cleaned_row[:11]
        if cleaned_row[-1] == "":
            cleaned_row[-1] = cleaned_row[-2]
        cleaned.append(cleaned_row)

    df = pd.DataFrame(cleaned, columns=[
        "AGENT CODE", "AGENT NAME", "CC", "APE", "MTD CC", "MTD APE", "YTD APE",
        "LAPSES", "NAP", "YTD CC", "NET CC"
    ])
    df[df.columns[2:]] = df[df.columns[2:]].apply(pd.to_numeric, errors='coerce').fillna(0)

    # Remove exact duplicate rows (same agent code AND same numeric values)
    # This prevents doubling when same row is extracted from both table and orphan
    # But keeps rows with same agent code but different values (e.g., 0.0 vs -252,000)
    df = df.drop_duplicates(subset=["AGENT CODE", "CC", "APE", "MTD CC", "MTD APE", "YTD APE", "LAPSES", "NAP", "YTD CC", "NET CC"])

    # Sort by name length to ensure longest names come first
    df["NAME LENGTH"] = df["AGENT NAME"].str.len()
    df = df.sort_values("NAME LENGTH", ascending=False)

    # Group by AGENT CODE only, sum numeric columns, keep first (longest) name
    df = df.groupby("AGENT CODE", as_index=False).agg({
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
    df = df.sort_values("AGENT CODE")
    return df[[
        "UNIT", "AGENT CODE", "AGENT NAME", "CC", "APE", "MTD CC", "MTD APE", "YTD APE",
        "LAPSES", "NAP", "YTD CC", "NET CC", "", "  ", "REPORT_DATE"
    ]]

# === STEP 4: Upload CSV ===
def upload_to_drive(local_path, folder_id, secret_path):
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

# === MAIN ===
if __name__ == "__main__":
    os.chdir(WORKING_DIR)
    all_dfs = []

    zip_files = [f for f in os.listdir(WORKING_DIR) if f.lower().endswith(".zip")]
    zip_files.sort(key=lambda f: os.path.getmtime(os.path.join(WORKING_DIR, f)))

    for zip_file in zip_files:
        zip_path = os.path.join(WORKING_DIR, zip_file)
        temp_extract_dir = os.path.join(WORKING_DIR, f"extracted_{os.path.splitext(zip_file)[0]}")
        os.makedirs(temp_extract_dir, exist_ok=True)

        print(f"🔓 Extracting: {zip_file} → {temp_extract_dir}")
        extract_zip(zip_path, temp_extract_dir, ZIP_PASSWORD)

        pdf_files = [
            f for f in os.listdir(temp_extract_dir)
            if f.lower().endswith("branchproductionreport.pdf") and "_" not in f
        ]

        for pdf_file in pdf_files:
            pdf_path = os.path.join(temp_extract_dir, pdf_file)
            report_date = extract_report_date(pdf_path)
            print(f"📅 Processing: {pdf_file} | Report Date: {report_date}")

            df = extract_and_clean_pdf(pdf_path, report_date)
            all_dfs.append(df)

        os.remove(zip_path)
        print(f"🗑️ Deleted ZIP: {zip_file}")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)

        # Deduplicate by AGENT CODE, keeping the longest name
        final_df["NAME LENGTH"] = final_df["AGENT NAME"].str.len()
        final_df = final_df.sort_values("NAME LENGTH", ascending=False)

        # Group and sum by agent code and report date (not by name to merge duplicates)
        final_df = final_df.groupby(
            ["AGENT CODE", "REPORT_DATE"], as_index=False
        ).agg({
            "AGENT NAME": "first",  # Keep the longest name (already sorted)
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

        # Clean up helper column
        final_df = final_df.drop(columns=["NAME LENGTH"], errors='ignore')

        # Get unique report date (assumes only one)
        unique_dates = final_df["REPORT_DATE"].unique()
        if len(unique_dates) != 1:
            raise ValueError("Expected exactly one REPORT_DATE, but found multiple.")

        report_date_str = unique_dates[0]
        final_df[report_date_str] = ""  # Replace column label with the actual date
        final_df.drop(columns=["REPORT_DATE"], inplace=True)

        # Add static columns
        final_df.insert(0, "UNIT", "UNKNOWN")
        final_df[""] = ""
        final_df["  "] = ""

        # Reorder columns
        final_df = final_df[[
            "UNIT", "AGENT CODE", "AGENT NAME", "CC", "APE", "MTD CC", "MTD APE",
            "YTD APE", "LAPSES", "NAP", "YTD CC", "NET CC", "", "  ", report_date_str
        ]]

        # Save to CSV
        clean_date = datetime.strptime(report_date, "%B %d, %Y").strftime("%Y%m%d")
        consolidated_csv = os.path.join(WORKING_DIR, f"Consolidated_Report_{clean_date}.csv")

        final_df.to_csv(consolidated_csv, index=False)
        print(f"💾 Saved Consolidated CSV: {os.path.basename(consolidated_csv)}")

        upload_to_drive(consolidated_csv, DRIVE_FOLDER_ID, CLIENT_SECRET_PATH)
    else:
        print("❌ No data frames to consolidate.")