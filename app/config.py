import os
from dataclasses import dataclass


@dataclass
class Settings:
    oracle_client_dir: str = os.getenv("ORACLE_CLIENT_DIR", r"C:\Oracle\instantclient\instantclient_23_0")
    oracle_user: str = os.getenv("ORACLE_USER", "soleitura")
    oracle_password: str = os.getenv("ORACLE_PASSWORD", "soleitura")
    oracle_dsn: str = os.getenv(
        "ORACLE_DSN",
        "dbprd.7141.cloudmv.com.br:1521/PRD7141.db7141.mv7141vcn.oraclevcn.com",
    )

    hf_api_url: str = os.getenv("HF_API_URL", "https://router.huggingface.co/v1/chat/completions")
    hf_token: str = os.getenv("HF_TOKEN", "")
    hf_model: str = os.getenv("HF_MODEL", "meta-llama/Meta-Llama-3-8B-Instruct")

    app_username: str = os.getenv("APP_USERNAME", "admin")
    app_password: str = os.getenv("APP_PASSWORD", "admin123")
    app_secret: str = os.getenv("APP_SECRET", "change-me")


settings = Settings()
