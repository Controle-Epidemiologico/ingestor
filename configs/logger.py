import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional, Union, List, Any

from configs.settings import Logging as LoggingConfig


class ColoredFormatter(logging.Formatter):
    """Formatter que adiciona cores aos logs no console."""

    COLORS = {
        "DEBUG": "\033[94m",  # Azul
        "INFO": "\033[92m",  # Verde
        "WARNING": "\033[93m",  # Amarelo
        "ERROR": "\033[91m",  # Vermelho
        "CRITICAL": "\033[91;1m",  # Vermelho negrito
        "RESET": "\033[0m",  # Reset
    }

    def format(self, record):
        log_message = super().format(record)
        if record.levelname in self.COLORS:
            return f"{self.COLORS[record.levelname]}{log_message}{self.COLORS['RESET']}"
        return log_message


class LoggerConfig:
    """Configuração centralizada de logging para toda a aplicação."""

    # Configurações padrão
    DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DETAILED_FORMAT = (
        "%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s"
    )

    # Cache de loggers configurados
    _configured_loggers: Dict[str, logging.Logger] = {}

    # Estado da configuração
    _is_configured = False

    @classmethod
    def configure(
        cls,
        log_level: Optional[str] = None,
        log_file: Optional[Union[str, Path]] = None,
        max_file_size_mb: int = 10,
        backup_count: int = 5,
        module_levels: Optional[Dict[str, str]] = None,
        use_colors: bool = True,
        detailed_format: bool = False,
    ) -> None:
        """
        Configura o sistema de logging global.

        Args:
            log_level: Nível de log (DEBUG, INFO, etc)
            log_file: Arquivo de log. Se None, só usa console
            max_file_size_mb: Tamanho máximo de arquivo de log em MB
            backup_count: Número de arquivos de backup
            module_levels: Dicionário com níveis específicos por módulo
            use_colors: Se deve usar cores no console
            detailed_format: Se deve usar formato detalhado com info de thread
        """
        # Evita configuração duplicada
        if cls._is_configured:
            return

        # Usa configurações do Pydantic Settings
        config = LoggingConfig()

        # Determina o nível de log (parâmetro tem prioridade sobre configuração)
        log_level = log_level or config.level

        # Define formato base
        log_format = cls.DETAILED_FORMAT if detailed_format else cls.DEFAULT_FORMAT

        # Configura o root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.getLevelName(log_level.upper()))

        # Limpa qualquer handler existente
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Adiciona handler de console
        console_handler = logging.StreamHandler(sys.stdout)

        if use_colors:
            console_formatter = ColoredFormatter(log_format)
        else:
            console_formatter = logging.Formatter(log_format)

        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

        # Adiciona handler de arquivo se especificado
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = RotatingFileHandler(
                log_path,
                maxBytes=max_file_size_mb * 1024 * 1024,
                backupCount=backup_count,
            )
            file_handler.setFormatter(logging.Formatter(log_format))
            root_logger.addHandler(file_handler)

        # Configura níveis específicos por módulo
        if module_levels:
            for module, level in module_levels.items():
                module_logger = logging.getLogger(module)
                module_logger.setLevel(logging.getLevelName(level.upper()))

        cls._is_configured = True

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Obtém um logger configurado para o módulo especificado.

        Args:
            name: Nome do módulo (geralmente __name__)

        Returns:
            Logger configurado
        """
        # Configura o sistema de logging se ainda não estiver configurado
        if not cls._is_configured:
            cls.configure()

        # Retorna do cache ou cria um novo
        if name not in cls._configured_loggers:
            cls._configured_loggers[name] = logging.getLogger(name)

        return cls._configured_loggers[name]


# Funções de conveniência
def get_logger(name: str) -> logging.Logger:
    """
    Obtém um logger configurado para o módulo especificado.

    Args:
        name: Nome do módulo (geralmente __name__)

    Returns:
        Logger configurado
    """
    return LoggerConfig.get_logger(name)


def configure_logging(**kwargs: Any) -> None:
    """
    Configura o sistema de logging com parâmetros personalizados.

    Args:
        **kwargs: Argumentos para a configuração do logger
    """
    LoggerConfig.configure(**kwargs)


# Configuração padrão na importação
if not LoggerConfig._is_configured:
    # Obtém config de ambiente via Pydantic
    config = LoggingConfig()
    is_dev = (
        config.environment.lower() == "development"
        if hasattr(config, "environment")
        else True
    )

    # Configuração padrão com base no ambiente
    LoggerConfig.configure(
        log_level=config.level,
        log_file=Path("logs/app.log") if not is_dev else None,
        use_colors=is_dev,
        detailed_format=not is_dev,
        module_levels={
            "connectors": config.connectors_level
            if hasattr(config, "connectors_level")
            else "INFO",
            "dags": config.dags_level if hasattr(config, "dags_level") else "INFO",
            "schemas": config.schemas_level
            if hasattr(config, "schemas_level")
            else "INFO",
        },
    )
