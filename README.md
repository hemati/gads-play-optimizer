## Setup

### 1  Install Python & dependencies

```bash
conda create -n gads-play-optimizer python=3.12 -y
conda activate gads-play-optimizer
pip install -r requirements.txt
```

Google’s Python client library works with any 3.9 – 3.12 interpreter ([developers.google.com][1]).

### 2  Enable APIs & download **client\_secret.json**

1. Open **Google Cloud Console → APIs & Services**.
2. Enable **Google Ads API** and **Google Play Developer API** for your project ([developers.google.com][2]).
3. Under **Credentials → Create credentials → OAuth client ID → Desktop app**, download the JSON and save it to **`config/client_secret.json`** (create the folder if it doesn’t exist) ([developers.google.com][3]).

### 3  Get a Google Ads **developer token**

* Sign in to your Google Ads *manager* (MCC) account → **Tools & Settings → Setup → API Center**.
  You receive a *test-access* token instantly ([developers.google.com][4], [developers.google.com][5]).
* Click **Apply for Basic access** so the same token can reach production accounts (approval ≈ 1-3 business days) ([developers.google.com][6]).
* Export it to the shell for the next step:

  ```bash
  export GOOGLE_ADS_DEVELOPER_TOKEN="INSERT_22_CHAR_TOKEN"
  ```

### 4  Run the all-in-one script

```bash
python scripts/init_google_auth.py
```

The script will:

| What happens                                                                                    | Output file                                      | Purpose                                                                                                                                                     |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Desktop OAuth flow via **InstalledAppFlow** prompts you to sign in ([developers.google.com][2]) | `~/.config/gads-play-optimizer/credentials.json` | Stores **refresh-token** for Ads + Play                                                                                                                     |
| Reads your `client_secret.json` and the exported developer-token                                | `config/google-ads.yaml`                         | Bundles **developer-token**, **client ID/secret** and **refresh-token** for the Ads client library ([developers.google.com][7], [developers.google.com][1]) |

If you log in through an MCC, the script will also ask for the **login-customer-ID** that should appear in the YAML header.

### 5  Project-specific environment variables

```bash
# 10-digit Ads client ID (no dashes) shown in the UI header
export GOOGLE_ADS_CUSTOMER_ID="1234567890"      :contentReference[oaicite:7]{index=7}

# Package name of the Play app you want to analyse
export GOOGLE_PLAY_PACKAGE_NAME="com.appcoholic.biblegpt"

# OpenAI key for recommendation generation
export OPENAI_API_KEY="sk-…"
```

### 6  Run the pipeline

```bash
python -m app.main
```

The task fetches Ads & Play metrics and writes `recommendations.json`.

### 7  (Option) Docker

```bash
docker build -t gads-play-optimizer .
docker run --env-file .env gads-play-optimizer
```

---

## Secret hygiene

* **Never commit** `config/client_secret.json`, `config/google-ads.yaml`, or the token file to version control—add them to `.gitignore` ([support.google.com][8]).
* Store long-lived tokens in a vault or CI secret manager to avoid accidental leaks ([support.google.com][9]).

---

> After Basic access is approved, the existing developer-token works for production calls without code changes, because both the YAML file and `GoogleAdsClient.load_from_storage()` already reference it ([developers.google.com][10]).

---

[1]: https://developers.google.com/google-ads/api/docs/client-libs/python/proto-getters?utm_source=chatgpt.com "Service and Type Getters | Google Ads API | Google for Developers"
[2]: https://developers.google.com/google-ads/api/docs/client-libs/python/oauth-web?utm_source=chatgpt.com "OAuth Desktop and Web Application Flows | Google Ads API | Google for ..."
[3]: https://developers.google.com/google-ads/api/docs/client-libs/java/oauth-web?hl=de&utm_source=chatgpt.com "OAuth-Desktop- und -Webanwendungsabläufe | Google Ads API | Google for ..."
[4]: https://developers.google.com/google-ads/api/docs/get-started/dev-token?utm_source=chatgpt.com "Obtain a developer token | Google Ads API | Google for Developers"
[5]: https://developers.google.com/google-ads/api/docs/access-levels?utm_source=chatgpt.com "Access Levels and Permissible Use | Google Ads API - Google Developers"
[6]: https://developers.google.com/google-ads/api/docs/get-started/dev-token?hl=de&utm_source=chatgpt.com "Entwickler-Token abrufen | Google Ads API - Google Developers"
[7]: https://developers.google.com/google-ads/api/docs/client-libs/python/configuration?utm_source=chatgpt.com "Configuration | Google Ads API | Google for Developers"
[8]: https://support.google.com/google-ads/answer/2375503?hl=en-EN&utm_source=chatgpt.com "Google Ads Application Programming Interface (API)"
[9]: https://support.google.com/admanager/answer/6078734?hl=en&utm_source=chatgpt.com "Add a service account user for API access - Google Ad Manager Help"
[10]: https://developers.google.com/google-ads/api/docs/get-started/make-first-call?utm_source=chatgpt.com "Make an API call | Google Ads API | Google for Developers"
