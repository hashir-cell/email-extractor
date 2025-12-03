# fetch.py
from app.service import get_gmail_service, get_outlook_service
from app.gmail_utils import fetch_recent_emails as fetch_gmail  # ← now takes service

def fetch_outlook(service, start_iso, end_iso):
    filt = f"receivedDateTime ge {start_iso} and receivedDateTime lt {end_iso}"
    resp = service.users().messages().list(**{"$filter": filt, "$top": 100}).execute()
    emails = []
    for m in resp.get("value", []):
        info = {
            "id": m["id"],
            "url": f"https://outlook.office.com/mail/deeplink?itemId={m['id']}",
            "from": m.get("from", {}).get("emailAddress", {}).get("address"),
            "subject": m.get("subject"),
            "date": m.get("receivedDateTime"),
            "snippet": m.get("bodyPreview", "")[:300],
            "attachments": [],
            "account": "outlook",
        }
        if m.get("hasAttachments"):
            atts = service.users().messages().get(id=m["id"], **{"$select": "attachments"}).execute()
            for a in atts.get("attachments", []):
                info["attachments"].append({"filename": a.get("name")})
        emails.append(info)
    return emails


def fetch_all_selected(session_id: str, start_str: str, end_str: str, accounts: dict):
    all_emails = []
    for provider, emails in accounts.items():
        for email in emails:
            if provider == "gmail":
                svc = get_gmail_service(session_id, email)  # ← from services.py
                batch = fetch_gmail(svc, start_str, end_str)  # ← pass service
            else:
                svc = get_outlook_service(session_id, email)
                start_iso = f"{start_str.replace('/', '-')}T00:00:00Z"
                end_iso = f"{end_str.replace('/', '-')}T00:00:00Z"
                batch = fetch_outlook(svc, start_iso, end_iso)

            for rec in batch:
                rec["account"] = f"{email} ({provider})"
            all_emails.extend(batch)
    return all_emails