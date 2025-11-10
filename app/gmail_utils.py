import os
import json
import base64
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as date_parser
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import streamlit as st

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

def get_gmail_service():
    """Load Gmail API service using pre-generated OAuth token (works on Streamlit Cloud)."""
    try:
        token_data = json.loads(st.secrets["GMAIL_TOKEN"])
        creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    except Exception:
        # Fallback: try to load from local token.json (for local dev)
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        else:
            raise RuntimeError("❌ No Gmail credentials found in st.secrets or token.json")

    # Refresh token automatically if expired
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return build("gmail", "v1", credentials=creds)

def parse_date_dynamic(date_value):
    if isinstance(date_value, pd.Timestamp):
        return date_value.to_pydatetime()
    if isinstance(date_value, (int, float)):
        return datetime.fromtimestamp(date_value)
    if isinstance(date_value, str):
        try:
            return date_parser.parse(date_value, fuzzy=True)
        except Exception:
            return None
    return None

def get_email_body(payload):
    """Extract plain or HTML text content from Gmail message payload."""
    if "body" in payload and "data" in payload["body"]:
        try:
            data = payload["body"]["data"]
            return base64.urlsafe_b64decode(data).decode("utf-8")
        except Exception:
            pass
    if "parts" in payload:
        for part in payload["parts"]:
            mime_type = part.get("mimeType", "")
            if mime_type == "text/plain":
                data = part["body"].get("data")
                if data:
                    return base64.urlsafe_b64decode(data).decode("utf-8")
            elif mime_type == "text/html":
                data = part["body"].get("data")
                if data:
                    html = base64.urlsafe_b64decode(data).decode("utf-8")
                    return BeautifulSoup(html, "html.parser").get_text()
            else:
                result = get_email_body(part)
                if result.strip():
                    return result
    return "[No text content found]"

def fetch_recent_emails(start_date, end_date):
    """Fetch emails within a date range."""
    service = get_gmail_service()
    query = f"after:{start_date} before:{end_date}"
    results = service.users().messages().list(userId="me", q=query).execute()
    messages = results.get("messages", [])
    email_data = []

    for msg in messages:
        msg_id = msg["id"]
        gmail_url = f"https://mail.google.com/mail/u/0/#inbox/{msg_id}"
        msg_data = service.users().messages().get(userId="me", id=msg_id, format="full").execute()

        headers = msg_data["payload"]["headers"]
        email_info = {"id": msg_id, "gmail_url": gmail_url, "from": None, "subject": None, "date": None, "snippet": None, "attachments": []}

        for header in headers:
            name = header["name"].lower()
            if name == "from":
                email_info["from"] = header["value"]
            elif name == "subject":
                email_info["subject"] = header["value"]
            elif name == "date":
                email_info["date"] = header["value"]

        email_info["snippet"] = get_email_body(msg_data["payload"])[:300]

        # Extract attachments (if any)
        if "parts" in msg_data["payload"]:
            for part in msg_data["payload"]["parts"]:
                body = part.get("body", {})
                if "attachmentId" in body:
                    att_id = body["attachmentId"]
                    attachment = service.users().messages().attachments().get(userId="me", messageId=msg_id, id=att_id).execute()
                    data = attachment.get("data")
                    if data:
                        file_bytes = base64.urlsafe_b64decode(data)
                        sha_hash = hashlib.sha256(file_bytes).hexdigest()
                        email_info["attachments"].append({
                            "filename": part.get("filename", "unknown"),
                            "hash": sha_hash,
                        })
        email_data.append(email_info)

    print(f"✅ Found {len(email_data)} emails between {start_date} - {end_date}")
    return email_data


# from email import parser
# import os
# import base64
# import hashlib
# from bs4 import BeautifulSoup
# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# from datetime import datetime
# from dateutil import parser as date_parser
# import pandas as pd

# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# CREDENTIALS_PATH = os.path.join(BASE_DIR, "credentials.json")
# TOKEN_PATH = os.path.join(BASE_DIR, "token.json")

# SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# def parse_date_dynamic(date_value):
#     if isinstance(date_value, (pd.Timestamp, )):
#         return date_value.to_pydatetime()
#     elif isinstance(date_value, (int, float)):
#         return datetime.fromtimestamp(date_value)
#     elif isinstance(date_value, str):
#         try:
#             return parser.parse(date_value, fuzzy=True)
#         except Exception as e:
#             print(f"⚠️ Could not parse date: {date_value} ({e})")
#             return None
#     else:
#         return None

# def get_gmail_service():
#     creds = None
#     if os.path.exists(TOKEN_PATH):
#         creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
#             creds = flow.run_local_server(port=0)
#         with open(TOKEN_PATH, "w") as token:
#             token.write(creds.to_json())
#     return build("gmail", "v1", credentials=creds)

# def parse_date_dynamic(date_value):
#     if isinstance(date_value, pd.Timestamp):
#         return date_value.to_pydatetime()
#     if isinstance(date_value, (int, float)):
#         return datetime.fromtimestamp(date_value)
#     if isinstance(date_value, str):
#         try:
#             return date_parser.parse(date_value, fuzzy=True)
#         except Exception:
#             return None
#     return None

# def get_email_body(payload):
#     if "body" in payload and "data" in payload["body"]:
#         try:
#             data = payload["body"]["data"]
#             return base64.urlsafe_b64decode(data).decode("utf-8")
#         except Exception:
#             pass
#     if "parts" in payload:
#         for part in payload["parts"]:
#             mime_type = part.get("mimeType", "")
#             if mime_type == "text/plain":
#                 data = part["body"].get("data")
#                 if data:
#                     return base64.urlsafe_b64decode(data).decode("utf-8")
#             elif mime_type == "text/html":
#                 data = part["body"].get("data")
#                 if data:
#                     html = base64.urlsafe_b64decode(data).decode("utf-8")
#                     return BeautifulSoup(html, "html.parser").get_text()
#             else:
#                 result = get_email_body(part)
#                 if result.strip():
#                     return result
#     return "[No text content found]"

# def extract_text_from_image(image_bytes):
#     return "[OCR skipped]"

# def fetch_recent_emails(start_date, end_date):
#     service = get_gmail_service()
#     query = f"after:{start_date} before:{end_date}"
#     print(f"📨 Fetching emails between {query}...")
#     results = service.users().messages().list(userId="me", q=query).execute()
#     messages = results.get("messages", [])
#     email_data = []
#     for msg in messages:
#         msg_id = msg["id"]
#         gmail_url = f"https://mail.google.com/mail/u/0/#inbox/{msg_id}"
#         msg_data = service.users().messages().get(userId="me", id=msg["id"], format="full").execute()
#         headers = msg_data["payload"]["headers"]
#         email_info = {
#             "id": msg["id"],
#             "gmail_url": gmail_url,
#             "from": None,
#             "subject": None,
#             "date": None,
#             "snippet": None,
#             "attachments": [],
#         }
#         for header in headers:
#             name = header["name"].lower()
#             if name == "from":
#                 email_info["from"] = header["value"]
#             elif name == "subject":
#                 email_info["subject"] = header["value"]
#             elif name == "date":
#                 email_info["date"] = header["value"]
#         email_info["snippet"] = get_email_body(msg_data["payload"])[:300]
#         if "parts" in msg_data["payload"]:
#             for part in msg_data["payload"]["parts"]:
#                 body = part.get("body", {})
#                 if "attachmentId" in body:
#                     att_id = body["attachmentId"]
#                     attachment = service.users().messages().attachments().get(
#                         userId="me", messageId=msg["id"], id=att_id
#                     ).execute()
#                     data = attachment.get("data")
#                     if data:
#                         file_bytes = base64.urlsafe_b64decode(data)
#                         sha_hash = hashlib.sha256(file_bytes).hexdigest()
#                         email_info["attachments"].append({
#                             "filename": part.get("filename", "unknown"),
#                             "hash": sha_hash,
#                         })
#         email_data.append(email_info)
#     print(f"✅ Found {len(email_data)} emails between {start_date} - {end_date}")
#     return email_data
