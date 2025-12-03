import base64
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Header
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import pandas as pd
from io import BytesIO
from datetime import datetime
from typing import List, Dict
from fastapi.middleware.cors import CORSMiddleware
from app.auth import create_session, get_oauth_url, exchange_code
from app.fetch import fetch_all_selected
from app.gmail_utils import save_only_pdf_attachments
from app.helper import hybrid_match_rag
from app.semantic_parsing import parser
from app.transaction_cleaner import clean_transactions
from app.matching_engine import hybrid_match
from app.rag_pipeline import INDEX_ROOT, csv_row_to_enhanced_query, format_results, global_search, ingest_all_emails

app = FastAPI(title="Financial Analyst API", version="1.0")

origins = [
    "http://localhost:5173", 
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, 
    allow_credentials=True,
    allow_methods=["*"],    
    allow_headers=["*"],    
)



@app.post("/session")
def create_new_session():
    return {"session_id": create_session()}


@app.get("/login/{provider}")
def get_login_url(provider: str, session_id: str = Header(alias="X-Session-ID")):
    if provider not in ["gmail", "outlook"]:
        raise HTTPException(400, "Invalid provider")
    url = get_oauth_url(provider, session_id)
    return {"auth_url": url}



@app.get("/{provider}/callback")
async def oauth_callback(
    provider: str,
    code: str,
    state: str | None = None,
):
    
    if not state:
        raise HTTPException(400, "Missing state")

    try:
        session_id, _ = state.split("|", 1)
    except ValueError:
        raise HTTPException(400, "Invalid state format")

    email = await exchange_code(provider, code, session_id)
    # Send back a tiny page that posts the result to the opener
    return HTMLResponse(
        f"""
        <script>
        if (window.opener) {{
            window.opener.postMessage({{email: "{email}", provider: "{provider}"}}, "*");
            window.close();
        }}
        </script>
        """
    )

@app.get("/accounts")
def list_accounts(session_id: str = Header(alias="X-Session-ID")):
    from app.auth import get_session
    session = get_session(session_id)
    accounts = []
    for prov, emails in session.items():
        if prov in ["gmail", "outlook"]:
            for e in emails:
                accounts.append(f"{e} ({prov})")
    return {"accounts": accounts}



@app.delete("/accounts")
def disconnect_account(account: str, session_id: str = Header(alias="X-Session-ID")):
    from app.auth import get_session
    session = get_session(session_id)
    email, prov = account.rsplit(" (", 1)
    prov = prov.rstrip(")")
    if prov in session and email in session[prov]:
        del session[prov][email]
        if not session[prov]:
            del session[prov]
    return {"status": "disconnected"}


@app.post("/process")
async def process_csv(
    file: UploadFile = File(...),
    accounts: List[str] = Form(...),
    session_id: str = Header(alias="X-Session-ID")
):
    if not accounts:
        raise HTTPException(400, "No accounts selected")


    selected = {}
    for acc in accounts:
        parts = [p.strip() for p in acc.split(",") if p.strip()]
        for part in parts:
            try:
                email_part, prov_part = part.rsplit(" (", 1)
                provider = prov_part.rstrip(")")
                email = email_part.strip()
                selected.setdefault(provider, []).append(email)
            except Exception as e:
                print(f"Invalid account format: {part} → {e}")
                continue

    content = await file.read()
    results = parser.parse_csv(BytesIO(content))
    transactions = clean_transactions(results)


    all_emails = []

    for txn in transactions:
        txn_date = pd.to_datetime(txn["date"], errors="coerce")
        start = (txn_date - pd.Timedelta(days=4)).strftime("%Y/%m/%d")
        end   = (txn_date + pd.Timedelta(days=4)).strftime("%Y/%m/%d")

        emails = fetch_all_selected(session_id, start, end, selected)
        all_emails.extend(emails)


    emails_metadata = []

    for email in all_emails:
        meta = {
            "email_id": email.get("id", ""),
            "from": email.get("from", ""),
            "subject": email.get("subject", ""),
            "date": email.get("date", ""),
            "account": email.get("account", ""),
            "snippet": email.get("snippet", ""),
            "attachments": [
                {
                    "filename": att.get("filename", ""),
                    "hash":     att.get("hash", ""),
                    "size":     len(att.get("bytes", b""))
                }
                for att in email.get("attachments", [])
            ]
        }
        emails_metadata.append(meta)


    manifests = ingest_all_emails(all_emails, batch_size=10)
    print(manifests)


    digest, exceptions = hybrid_match_rag(
        transactions=transactions,
        emails=emails_metadata,    
        top_k_per_batch=20,
        global_top_k=3
    )



    def df_to_csv(obj):
        df = pd.DataFrame(obj) if not isinstance(obj, pd.DataFrame) else obj
        return df.to_csv(index=False).encode()

    digest_csv = df_to_csv(digest)
    exceptions_csv = df_to_csv(exceptions)

    return {
        "digest_csv": base64.b64encode(digest_csv).decode(),
        "exceptions_csv": base64.b64encode(exceptions_csv).decode(),
        "digest_filename": f"ExpenseDigest_{datetime.now():%Y-%m-%d}.csv",
        "exceptions_filename": f"Exceptions_{datetime.now():%Y-%m-%d}.csv"
    }


# @app.post("/process")
# async def process_csv(
#     file: UploadFile = File(...),
#     accounts: List[str] = Form(...),  # ← This is correct
#     session_id: str = Header(alias="X-Session-ID")
# ):
#     if not accounts:
#         raise HTTPException(400, "No accounts selected")


#     selected = {}
#     for acc in accounts:
#         parts = [p.strip() for p in acc.split(",") if p.strip()]
#         for part in parts:
#             try:
#                 email_part, prov_part = part.rsplit(" (", 1)
#                 provider = prov_part.rstrip(")")
#                 email = email_part.strip()
#                 selected.setdefault(provider, []).append(email)
#             except Exception as e:
#                 print(f"Invalid account format: {part} → {e}")
#                 continue

#     content = await file.read()
#     results = parser.parse_csv(BytesIO(content))
#     transactions = clean_transactions(results)

#     all_emails = []
    
#     for txn in transactions:
#      txn_date = pd.to_datetime(txn["date"], errors="coerce")
#      start = (txn_date - pd.Timedelta(days=4)).strftime("%Y/%m/%d")
#      end   = (txn_date + pd.Timedelta(days=4)).strftime("%Y/%m/%d")

#      emails = fetch_all_selected(session_id, start, end, selected)
#      all_emails.extend(emails)



#     emails_metadata = []

#     for email in all_emails:

#      meta = {
#         "email_id": email.get("id", ""),
#         "from":     email.get("from", ""),
#         "subject":  email.get("subject", ""),
#         "date":     email.get("date", ""),
#         "account":  email.get("account", ""),
#         "snippet":  email.get("snippet", ""),
#         "attachments": []
#     }

#      for att in email.get("attachments", []):
#         meta["attachments"].append({
#             "filename": att.get("filename", ""),
#             "hash":     att.get("hash", ""),
#             "size":     len(att.get("bytes", b""))
#         })

#      emails_metadata.append(meta)

#      manifests=ingest_all_emails(emails_metadata,batch_size=100)

#      for csv_row in transactions:
#          query_info = csv_row_to_enhanced_query(csv_row)
#          batch_dirs = sorted([p for p in INDEX_ROOT.iterdir() if p.is_dir()])
#          results = global_search(query_info, batch_dirs, top_k=3, top_k_per_batch=20, rerank=True)
#          formatted_results = format_results(results)
         
         






#     def df_to_csv(obj):
#         df = pd.DataFrame(obj) if not isinstance(obj, pd.DataFrame) else obj
#         return df.to_csv(index=False).encode()

#     digest_csv = df_to_csv(digest)
#     exceptions_csv = df_to_csv(exceptions)

#     return {
#         "digest_csv": base64.b64encode(digest_csv).decode(),
#         "exceptions_csv": base64.b64encode(exceptions_csv).decode(),
#         "digest_filename": f"ExpenseDigest_{datetime.now():%Y-%m-%d}.csv",
#         "exceptions_filename": f"Exceptions_{datetime.now():%Y-%m-%d}.csv"
#     }
