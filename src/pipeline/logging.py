from __future__ import annotations

import logging
from pathlib import Path

from .config import LOGS_DIR

def get_logger(name: str = "pipeline") -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = Path(LOGS_DIR) / "pipeline.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
