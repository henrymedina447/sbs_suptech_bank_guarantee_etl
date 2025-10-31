import os
import logging

from dotenv import load_dotenv

logger = logging.getLogger("app.environment")


def load_environment() -> None:
    env_type = os.getenv("ENV_TYPE", "qa")
    path_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    file_env = f".env_{env_type}"
    dotenv_path = os.path.join(path_root, "environments", file_env)
    if os.path.exists(dotenv_path):
        logger.info(f"Usando el ambiente de: {env_type}")
        load_dotenv(dotenv_path, override=True)
    else:
        logger.error(f"Advertencia: No se encontró el archivo de entorno")
