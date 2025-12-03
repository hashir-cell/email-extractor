# auth.py  (FULL REPLACEMENT)
import os
import secrets
import base64
import hashlib
import requests
import json
from fastapi import HTTPException
from dotenv import load_dotenv
import google.auth.transport.requests
from google.oauth2 import id_token

load_dotenv()

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8000")
GMAIL_CLIENT_ID = os.getenv("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.getenv("GMAIL_CLIENT_SECRET")
OUTLOOK_CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID")
OUTLOOK_CLIENT_SECRET = os.getenv("OUTLOOK_CLIENT_SECRET")

# In-memory store (use Redis/DB in prod)
TOKENS = {}  # {session_id: {provider: {email: token_dict}}}


def generate_pkce():
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return verifier, challenge

def create_session():
    session_id = secrets.token_urlsafe(32)
    TOKENS[session_id] = {}
    return session_id

def get_session(session_id: str):
    if session_id not in TOKENS:
        raise HTTPException(401, "Invalid session")
    return TOKENS[session_id]

def get_oauth_url(provider: str, session_id: str):
    verifier, challenge = generate_pkce()
    state = f"{session_id}|{secrets.token_urlsafe(16)}"

    session = get_session(session_id)

    if provider == "gmail":
        # FIXED: Added 'email profile' scopes (required for userinfo)
        scopes = "https://www.googleapis.com/auth/gmail.readonly email profile"
        url = (
            f"https://accounts.google.com/o/oauth2/v2/auth?"
            f"client_id={GMAIL_CLIENT_ID}&"
            "response_type=code&"
            f"redirect_uri={REDIRECT_URI}/{provider}/callback&"
            f"scope={scopes}&"
            f"state={state}&"
            f"code_challenge={challenge}&"
            "code_challenge_method=S256&"
            "access_type=offline&prompt=consent"
        )
    elif provider == "outlook":
        # Outlook unchanged (uses openid for profile)
        scopes = "https://graph.microsoft.com/Mail.Read openid profile email"
        url = (
            f"https://login.microsoftonline.com/common/oauth2/v2.0/authorize?"
            f"client_id={OUTLOOK_CLIENT_ID}&"
            "response_type=code&"
            f"redirect_uri={REDIRECT_URI}/{provider}/callback&"
            f"scope={scopes}&"
            f"state={state}&"
            f"code_challenge={challenge}&"
            "code_challenge_method=S256"
        )
    else:
        raise HTTPException(400, "Unsupported provider")

    # Store verifier
    session[f"{provider}_verifier"] = verifier
    return url

async def exchange_code(provider: str, code: str, session_id: str):
    session = get_session(session_id)
    verifier = session.pop(f"{provider}_verifier", None)
    if not verifier:
        raise HTTPException(400, "Missing code verifier")

    # Token exchange
    if provider == "gmail":
        resp = requests.post("https://oauth2.googleapis.com/token", data={
            "client_id": GMAIL_CLIENT_ID,
            "client_secret": GMAIL_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": f"{REDIRECT_URI}/{provider}/callback",
            "code_verifier": verifier
        })
    else:  # outlook
        resp = requests.post("https://login.microsoftonline.com/common/oauth2/v2.0/token", data={
            "client_id": OUTLOOK_CLIENT_ID,
            "client_secret": OUTLOOK_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": f"{REDIRECT_URI}/{provider}/callback",
            "code_verifier": verifier
        })

    token_data = resp.json()
    if "error" in token_data:
        raise HTTPException(400, token_data.get("error_description", "Token exchange failed"))

    token = token_data

    # FIXED: Extract email with correct endpoint + fallback to ID token
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    email = None

    if provider == "gmail":
        # Step 1: Try correct userinfo endpoint (v2/me)
        try:
            profile_resp = requests.get(
                "https://www.googleapis.com/userinfo/v2/me",  # ← CORRECT ENDPOINT
                headers=headers
            )
            if profile_resp.status_code == 200:
                profile = profile_resp.json()
                email = profile.get("email")
                if email and profile.get("verified_email"):
                    print(f"✅ Got email from userinfo: {email}")  # Debug log
                else:
                    raise ValueError("Email missing or unverified in userinfo")
        except Exception as e:
            print(f"❌ Userinfo failed: {e}")

        # Step 2: Fallback to ID token parsing (always reliable)
        if not email:
            try:
                id_token.verify_oauth2_token(
                    token['id_token'],  # From token response
                    google.auth.transport.requests.Request(),
                    GMAIL_CLIENT_ID
                )
                # Decode payload (email is in 'email' claim)
                payload = id_token.verify_token(token['id_token'], GMAIL_CLIENT_ID)
                email = payload.get('email')
                print(f"✅ Got email from ID token: {email}")  # Debug log
                if not email:
                    raise ValueError("No email in ID token")
            except Exception as e:
                print(f"❌ ID token failed: {e}")
                raise HTTPException(400, "Failed to verify user identity")

    else:  # outlook
        profile_resp = requests.get("https://graph.microsoft.com/v1.0/me", headers=headers)
        if profile_resp.status_code != 200:
            raise HTTPException(400, "Failed to fetch Outlook profile")
        profile = profile_resp.json()
        email = profile.get("mail") or profile.get("userPrincipalName")

    if not email:
        raise HTTPException(400, "No email address found in profile")

    # Store token
    session.setdefault(provider, {})[email] = token
    print(f"✅ Stored token for {email} ({provider})")  # Debug log
    return email

# Rest of your functions unchanged...
def get_connected_accounts(session_id: str):
    session = get_session(session_id)
    accounts = []
    for prov, emails in session.items():
        if prov in ["gmail", "outlook"]:
            for e in emails:
                accounts.append(f"{e} ({prov})")
    return accounts

def disconnect_account(session_id: str, account: str):
    session = get_session(session_id)
    try:
        email, prov = account.rsplit(" (", 1)
        prov = prov.rstrip(")")
        if prov in session and email in session[prov]:
            del session[prov][email]
            if not session[prov]:
                del session[prov]
        return {"status": "disconnected"}
    except Exception:
        raise HTTPException(400, "Invalid account")