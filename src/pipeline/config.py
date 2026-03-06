from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

    @property
    def sqlalchemy_url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"

def get_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        db=os.getenv("POSTGRES_DB", "weather_energy"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )

def days_back() -> int:
    return int(os.getenv("DAYS_BACK", "30"))

def timezone() -> str:
    return os.getenv("PIPELINE_TIMEZONE", "America/Sao_Paulo")

# Cidades (lat/lon) — você pode adicionar outras
CITIES = [
    {"city": "Americana-SP", "lat": -22.739, "lon": -47.331},
    {"city": "Sao Paulo-SP", "lat": -23.5505, "lon": -46.6333},
    {"city": "Rio de Janeiro-RJ", "lat": -22.9068, "lon": -43.1729},
    {"city": "Brasilia-DF", "lat": -15.7939, "lon": -47.8828},
]
