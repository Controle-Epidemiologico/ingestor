import logging
import os
import sys
from typing import Optional, Dict, Any
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

DEFAULT_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
JSON_FORMAT = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'


def setup_logger(
    name: str,
    level: Optional[str] = None,
    format_str: str = DEFAULT_FORMAT,
    json_output: bool = False
) -> logging.Logger:
    """
    Configura e retorna um logger com o nome especificado.

    Args:
        name: Nome do logger, normalmente __name__ do módulo
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
              Se None, usa a variável de ambiente LOG_LEVEL ou INFO por padrão
        format_str: Formato do log
        json_output: Se True, usa formato JSON para logs (útil para processamento)

    Returns:
        Logger configurado
    """
    log_level_str = level or os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = LOG_LEVELS.get(log_level_str, logging.INFO)

    # Use formato JSON se configurado
    if json_output or os.getenv("LOG_JSON", "").lower() == "true":
        format_str = JSON_FORMAT

    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(format_str)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(log_level)
    logger.propagate = False

    return logger


@lru_cache(maxsize=128)
def get_logger(name: str) -> logging.Logger:
    """
    Obtém um logger configurado para o módulo especificado.

    Args:
        name: Nome do módulo (normalmente __name__)

    Returns:
        Logger configurado
    """
    return setup_logger(name)


def log_with_context(logger: logging.Logger, level: int, msg: str, context: Dict[Any, Any] = None):
    """
    Registra uma mensagem com contexto adicional

    Args:
        logger: Logger a ser usado
        level: Nível do log (logging.INFO, etc)
        msg: Mensagem
        context: Dicionário com dados contextuais
    """
    if context:
        msg = f"{msg} | Context: {context}"
    logger.log(level, msg)


logger = setup_logger("ingestor")