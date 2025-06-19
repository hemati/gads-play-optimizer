# gads-play-optimizer

Proof-of-concept project that syncs data from Google Ads and Google Play
and generates daily optimisation suggestions via OpenAI.

## Setup

1. Install Python 3.11 and create a virtual environment:
```bash
conda create -n gads-play-optimizer python=3.12 -y
pip install -r requirements.txt
```

2. Enable Google Ads API and Google Play Developer API in your Google Cloud
   project. In **APIs & Services → Credentials** create an **OAuth client ID**
   for a **Desktop** application and download the JSON file. Create a `config/`
   directory and save the file as `config/client_secret.json`.

3. Initialise credentials (opens a browser for login):
```bash
python scripts/init_google_auth.py
```
   Choose the Google account that has access to the Ads account and Play
   Console you want to use. The granted tokens are saved under
   `~/.config/gads-play-optimizer/credentials.json`. Re-run the script at any
   time to authorise a different account.

4. Copy `.env.example` to `.env` and set your `OPENAI_API_KEY`.

5. Run the pipeline locally:
```bash
python -m app.main
```
This fetches data and prints generated recommendations.

You can also build and run the Docker image:
```bash
docker build -t gads-play-optimizer .
docker run --env-file .env gads-play-optimizer
```

## License

This project is licensed under the GPLv3. See `LICENSE` for details.
