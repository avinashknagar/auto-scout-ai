import os
import yaml


def load_config(project_root: str = None) -> dict:
    """Load and validate config.yaml."""
    if project_root is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    config_path = os.path.join(project_root, "config.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Ensure required sections exist
    for section in ("scrape", "rate_limit", "platforms", "database"):
        if section not in config:
            raise ValueError(f"Missing required config section: {section}")

    # Defaults for optional sections
    config.setdefault("filters", {})
    config.setdefault("logging", {"level": "INFO", "file": None})

    # Resolve database path relative to project root
    db_path = config["database"]["path"]
    if not os.path.isabs(db_path):
        config["database"]["path"] = os.path.join(project_root, db_path)

    # Resolve log file path
    log_file = config["logging"].get("file")
    if log_file and not os.path.isabs(log_file):
        config["logging"]["file"] = os.path.join(project_root, log_file)

    return config


def load_tokens(project_root: str = None) -> dict:
    """Load tokens.yaml with auth credentials."""
    if project_root is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    tokens_path = os.path.join(project_root, "tokens.yaml")
    if not os.path.exists(tokens_path):
        raise FileNotFoundError(
            f"Tokens not found: {tokens_path}\n"
            "Create tokens.yaml with your Cars24 JWT and Spinny session cookies."
        )

    with open(tokens_path) as f:
        tokens = yaml.safe_load(f)

    return tokens
