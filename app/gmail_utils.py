import base64, hashlib, os
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as date_parser
import pandas as pd


def parse_date_dynamic(date_value):
    if isinstance(date_value, pd.Timestamp):
        return date_value.to_pydatetime()
    if isinstance(date_value, (int, float)):
        return datetime.fromtimestamp(date_value)
    if isinstance(date_value, str):
        try:
            return date_parser.parse(date_value, fuzzy=True)
        except: return None
    return None

def get_email_body(payload):
    print("🔍 Extracting email body...")
    if "body" in payload and "data" in payload["body"]:
        try:
            print("📄 Plain body found")
            data = payload["body"]["data"]
            return base64.urlsafe_b64decode(data).decode("utf-8")
        except Exception as e:
            print("⚠ Body decode failed", e)

    if "parts" in payload:
        for idx, part in enumerate(payload["parts"]):
            mime = part.get("mimeType","")
            print(f"➡ scanning part[{idx}] type={mime}")

            if mime == "text/plain":
                data = part["body"].get("data")
                if data:
                    print("📄 Extracted text/plain successfully")
                    return base64.urlsafe_b64decode(data).decode("utf-8")

            elif mime == "text/html":
                data = part["body"].get("data")
                if data:
                    print("🌐 HTML extracted & parsed")
                    html = base64.urlsafe_b64decode(data).decode()
                    return BeautifulSoup(html,"html.parser").get_text()

            else:
                result = get_email_body(part)
                if result and result.strip(): return result

    return "[No text content found]"


def fetch_recent_emails(service, start_date, end_date):
    query = f"after:{start_date} before:{end_date}"
    print(f"\n🔍 Gmail Query → {query}")

    results = service.users().messages().list(userId="me", q=query).execute()
    messages = results.get("messages", [])
    print(f"📬 Found {len(messages)} emails\n")

    email_data = []

    for i,msg in enumerate(messages):
        msg_id = msg["id"]
        print(f"📩 EMAIL {i+1}/{len(messages)} MSG-ID: {msg_id}")

        msg_data = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
        payload = msg_data.get("payload",{})
        headers = payload.get("headers",[])

        info = {"id":msg_id,"attachments":[], "snippet":""}


        for h in headers:
            name=h["name"].lower()
            if name=="from": info["from"]=h["value"]
            if name=="subject": info["subject"]=h["value"]
            if name=="date": info["date"]=h["value"]

        info["snippet"] = get_email_body(payload)[:300]

        if "parts" in payload:
            print("🔍 Scanning parts for attachments...")

            for j,part in enumerate(payload["parts"]):
                mime = part.get("mimeType","")
                print(f"  ➤ Part {j} → mime={mime}")

                if mime != "application/pdf":
                    print("   ⏩ Skipped (not pdf)")
                    continue

                body = part.get("body",{})
                att_id = body.get("attachmentId")

                if not att_id:
                    print("   ❌ attachmentId missing → cannot download")
                    continue

                print(f"   📎 PDF Attachment detected → ID={att_id}")

                try:
                    attachment = service.users().messages().attachments().get(
                        userId="me", messageId=msg_id,id=att_id).execute()

                    data = attachment.get("data")
                    if not data:
                        print("   ⚠ NO DATA FIELD FOUND INSIDE ATTACHMENT!")
                        continue

                    file_bytes = base64.urlsafe_b64decode(data)
                    filename = part.get("filename","unknown.pdf")

                    info["attachments"].append({
                        "filename":filename,
                        "bytes":file_bytes,
                        "hash":hashlib.sha256(file_bytes).hexdigest()
                    })

                except Exception as e:
                    print("   ❌ Attachment fetch failed", e)

        else:
            print("❌ No parts found → No attachments")

        email_data.append(info)

    print(f"\n====== DONE FETCHING EMAILS ======")
    return email_data


import os
SAVE_DIR = "downloaded_pdfs"
os.makedirs(SAVE_DIR, exist_ok=True)

def save_only_pdf_attachments(messages):
    saved_files = []

    for msg in messages:
        for att in msg.get("attachments", []):

            file_name = att.get("filename")
            pdf_bytes = att.get("bytes")   

            if not pdf_bytes:
                print(f"⚠ No PDF data found for {file_name}")
                continue

            path = os.path.join(SAVE_DIR, file_name)

            try:
                with open(path, "wb") as f:
                    f.write(pdf_bytes)

                saved_files.append(path)
                print(f"📥 SAVED → {path}")

            except Exception as e:
                print(f"❌ Failed saving {file_name}: {e}")

    return saved_files
