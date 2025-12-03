# services.py
import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import msal
import requests
from app.auth import get_session
from dotenv import load_dotenv

load_dotenv()

GMAIL_CLIENT_ID = os.getenv("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.getenv("GMAIL_CLIENT_SECRET")
OUTLOOK_CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID")
OUTLOOK_CLIENT_SECRET = os.getenv("OUTLOOK_CLIENT_SECRET")


def get_gmail_service(session_id: str, email: str):
    session = get_session(session_id)
    token_data = session["gmail"][email]

    # Reconstruct full credentials dict
    full_info = {
        "client_id": GMAIL_CLIENT_ID,
        "client_secret": GMAIL_CLIENT_SECRET,
        "refresh_token": token_data["refresh_token"],
        # Optional: add token_uri if needed
        "token_uri": "https://oauth2.googleapis.com/token",
    }

    creds = Credentials.from_authorized_user_info(full_info, ["https://www.googleapis.com/auth/gmail.readonly"])

    # If access token is missing or expired, refresh
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise ValueError("No valid access token and no refresh token")

    return build("gmail", "v1", credentials=creds)


def get_outlook_service(session_id: str, email: str):
    session = get_session(session_id)
    token_data = session["outlook"][email]

    app = msal.ConfidentialClientApplication(
        client_id=OUTLOOK_CLIENT_ID,
        client_credential=OUTLOOK_CLIENT_SECRET,
        authority="https://login.microsoftonline.com/common",
    )

    result = app.acquire_token_by_refresh_token(
        refresh_token=token_data["refresh_token"],
        scopes=["https://graph.microsoft.com/Mail.Read"]
    )

    if "error" in result:
        raise Exception(f"Outlook refresh failed: {result.get('error_description')}")

    access_token = result["access_token"]

    class GraphClient:
        def __init__(self, token):
            self.token = token

        def _call(self, method, path, **kwargs):
            url = f"https://graph.microsoft.com/v1.0{path}"
            headers = {"Authorization": f"Bearer {self.token}"}
            r = requests.request(method, url, headers=headers, **kwargs)
            r.raise_for_status()
            return type("Resp", (), {"execute": lambda: r.json()})()

        def users(self):
            class Users:
                def messages(self):
                    class Messages:
                        @staticmethod
                        def list(**kw): return GraphClient(self.token)._call("GET", "/me/messages", params=kw)
                        @staticmethod
                        def get(**kw): return GraphClient(self.token)._call("GET", f"/me/messages/{kw['id']}")
                    return Messages()
            return type("U", (), {"messages": lambda: Users().messages})()

    return GraphClient(access_token)