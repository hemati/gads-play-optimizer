from pathlib import Path
import os
import yaml  # PyYAML installieren: pip install pyyaml
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/androidpublisher",
]

BASE_DIR = Path(__file__).resolve().parent.parent
CLIENT_SECRET = BASE_DIR / "config/client_secret.json"
TOKEN_STORE = Path.home() / ".config/gads-play-optimizer/credentials.json"
ADS_CONFIG = BASE_DIR / "config/google-ads.yaml"


def build_google_ads_yaml(
    creds,
    developer_token,
    client_id,
    client_secret,
    login_customer_id: str | None = None,
    dest: Path = ADS_CONFIG,
) -> None:
    """Schreibt die google-ads.yaml passend zum Refresh-Token, das wir eben bekommen haben."""
    data = {
        "developer_token": developer_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": creds.refresh_token,
        "use_proto_plus": True,
    }
    if login_customer_id:
        data["login_customer_id"] = login_customer_id

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(yaml.safe_dump(data, sort_keys=False))
    print(f"Google-Ads-Konfiguration unter {dest} gespeichert")


def main() -> None:
    # --- OAuth-Flow ----------------------------------------------------------
    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
    creds = flow.run_local_server(port=0)

    # Tokens für Google Play / Android-Publisher separat ablegen (falls gewünscht)
    TOKEN_STORE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_STORE.write_text(creds.to_json())
    print(f"OAuth-Tokens unter {TOKEN_STORE} gespeichert")

    # --- YAML bauen ----------------------------------------------------------
    developer_token = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") or input(
        "Developer-Token aus dem API-Center eingeben: "
    )
    # client_id / client_secret stehen bereits in client_secret.json; auslesen:
    with open(CLIENT_SECRET) as f:
        secret_json = yaml.safe_load(f)
    client_id = secret_json["installed"]["client_id"]
    client_secret = secret_json["installed"]["client_secret"]

    login_customer_id = (
        input("Login-Customer-ID (leer lassen, wenn nicht benötigt): ").strip() or None
    )

    build_google_ads_yaml(
        creds,
        developer_token=developer_token,
        client_id=client_id,
        client_secret=client_secret,
        login_customer_id=login_customer_id,
    )


if __name__ == "__main__":
    main()
