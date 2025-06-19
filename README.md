# gads-play-optimizer

Proof-of-concept project that syncs data from Google Ads and Google Play
and generates daily optimisation suggestions via OpenAI.

## Setup

1. Install Python 3.11 and create a virtual environment:
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

2. Enable Google Ads API and Google Play Developer API in your Google Cloud
   project and download an OAuth client file. Place it in `config/client_secret.json`.

3. Initialise credentials (opens a browser for login):
```bash
python scripts/init_google_auth.py
```

4. Copy `.env.example` to `.env` and set your `OPENAI_API_KEY`.

5. Run the Airflow pipeline locally:
```bash
airflow standalone
```
The DAG `airflow/daily_sync.py` fetches data and stores generated
recommendations.

Alternatively, build and run the FastAPI service:
```bash
docker build -t gads-play-optimizer .
docker run -p 8080:8080 --env-file .env gads-play-optimizer
```

## License

This project is licensed under the GPLv3. See `LICENSE` for details.
