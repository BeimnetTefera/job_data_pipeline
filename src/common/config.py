from pathlib import Path
import yaml

CONFIG_ROOT = Path(__file__).resolve().parents[2] / "configs" / "sources"

def load_source_config(name: str) -> dict:
    """Load a source's YAML config into a plain dict."""
    config_path = CONFIG_ROOT / f"{name}.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
    