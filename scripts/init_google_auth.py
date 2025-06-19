from pathlib import Path
import json
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/androidpublisher",
]

CLIENT_SECRET = Path("config/client_secret.json")
TOKEN_STORE = Path.home() / ".config/gads-play-optimizer/credentials.json"

def main():
    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
    creds = flow.run_local_server(port=0)
    TOKEN_STORE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_STORE.write_text(creds.to_json())
    print(f"Saved tokens to {TOKEN_STORE}")

if __name__ == "__main__":
    main()
