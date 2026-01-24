import os
import re
import base64
import ssl
import sys
import time
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# ================= CONFIG =================
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
BRANCH_DIR = r"D:\My Files\LKL Reports\IDSS Automation\downloads"
UNIT_DIR = r"D:\My Files\LKL Reports\IDSS Automation\checker"
os.makedirs(BRANCH_DIR, exist_ok=True)
os.makedirs(UNIT_DIR, exist_ok=True)

BRANCH_REGEX = re.compile(r"^(Branch) Production Reports as of (\d{8})(?: \((.+)\))?$", re.IGNORECASE)
UNIT_REGEX   = re.compile(r"^(Unit) Production Reports as of (\d{8})(?: \((.+)\))?$", re.IGNORECASE)

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ================= AUTH =================
def get_gmail_service():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    creds_path = os.path.join(base_dir, "credentials1.json")
    token_path = os.path.join(base_dir, "token.json")
    creds = None

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as token_file:
            token_file.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)

# ================= FETCH MESSAGES =================
def fetch_latest_idss_messages(service, subject_regex, query_subject, max_messages=30, retries=3):
    for attempt in range(1, retries + 1):
        try:
            results = service.users().messages().list(
                userId="me",
                q=f"subject:{query_subject}",
                maxResults=max_messages
            ).execute()

            messages = results.get("messages", [])
            reports = []

            for msg in messages:
                meta = service.users().messages().get(
                    userId="me",
                    id=msg["id"],
                    format="metadata",
                    metadataHeaders=["Subject"]
                ).execute()

                headers = meta["payload"].get("headers", [])
                subject = next((h["value"] for h in headers if h["name"] == "Subject"), "").strip()
                match = subject_regex.match(subject)
                if not match:
                    continue

                report_type, report_date, branch = match.groups()
                reports.append({"id": msg["id"], "date": report_date})

            if not reports:
                return [], None

            latest_date = max(r["date"] for r in reports)
            latest_ids = [r["id"] for r in reports if r["date"] == latest_date]

            return latest_ids, latest_date

        except HttpError as e:
            logging.warning(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                logging.error("Failed to fetch messages after retries.")
                return [], None

# ================= DOWNLOAD ATTACHMENT =================
def download_attachment(service, msg_id, part, folder, retries=3):
    filename = part.get("filename")
    if not filename.lower().endswith(".zip"):
        return None

    path = os.path.join(folder, filename)
    if os.path.exists(path):
        return None  # already downloaded

    attachment_id = part.get("body", {}).get("attachmentId")
    if not attachment_id:
        return None

    for attempt in range(1, retries + 1):
        try:
            attachment = service.users().messages().attachments().get(
                userId="me",
                messageId=msg_id,
                id=attachment_id
            ).execute()

            # data = base64.urlsafe_b64decode(attachment["data"])
            # with open(path, "wb") as f:
            #     f.write(data)
            # return filename

            data = base64.urlsafe_b64decode(attachment["data"])

            temp_path = path + ".part"
            with open(temp_path, "wb") as f:
                f.write(data)

            os.replace(temp_path, path)  # atomic rename (Windows-safe)
            return filename

        except HttpError as e:
            logging.warning(f"Attempt {attempt} failed for attachment {filename}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                logging.error(f"Failed to download attachment {filename} after retries.")
                return None

# ================= PARALLEL DOWNLOAD =================
def download_zip_attachments_parallel(service, message_ids, folder):
    downloaded = []

    for msg_id in message_ids:
        # Fetch full message **sequentially**
        for attempt in range(1, 4):
            try:
                message = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
                break
            except HttpError as e:
                logging.warning(f"Attempt {attempt} failed for message {msg_id}: {e}")
                time.sleep(2 ** attempt)
            except ssl.SSLError as e:
                logging.warning(f"SSL error for message {msg_id}, retrying: {e}")
                time.sleep(2 ** attempt)
        else:
            logging.error(f"Failed to fetch message {msg_id}, skipping.")
            continue

        parts = message["payload"].get("parts", [])

        # Download attachments in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(download_attachment, service, msg_id, part, folder) for part in parts]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    downloaded.append(result)

    return downloaded


# ================= MAIN =================
def step1_download_latest_idss():
    start = time.perf_counter()
    logging.info("Starting IDSS downloader (Gmail API)...")
    service = get_gmail_service()

    # Branch
    branch_ids, branch_date = fetch_latest_idss_messages(service, BRANCH_REGEX, "Branch Production Reports")
    if branch_ids:
        logging.info(f"Latest Branch IDSS detected: {branch_date}")
        downloaded_branch = download_zip_attachments_parallel(service, branch_ids, BRANCH_DIR)
        if downloaded_branch:
            logging.info(f"Downloaded Branch ZIP(s): {', '.join(downloaded_branch)}")
        else:
            logging.info("No new Branch ZIPs to download (already processed).")
    else:
        logging.info("No Branch IDSS reports found.")

    # Unit
    unit_ids, unit_date = fetch_latest_idss_messages(service, UNIT_REGEX, "Unit Production Reports")
    if unit_ids:
        logging.info(f"Latest Unit IDSS detected: {unit_date}")
        downloaded_unit = download_zip_attachments_parallel(service, unit_ids, UNIT_DIR)
        if downloaded_unit:
            logging.info(f"Downloaded Unit ZIP(s): {', '.join(downloaded_unit)}")
        else:
            logging.info("No new Unit ZIPs to download (already processed).")
    else:
        logging.info("No Unit IDSS reports found.")

    elapsed = time.perf_counter() - start
    logging.info(f"Finished in {elapsed:.2f} seconds")

# ================= ENTRY =================
if __name__ == "__main__":
    step1_download_latest_idss()
