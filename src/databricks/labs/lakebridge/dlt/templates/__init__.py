"""DLT Templates - Pre-built templates for common migration patterns."""

from pathlib import Path

TEMPLATES_DIR = Path(__file__).parent


def get_template(name: str) -> str:
    """Get a template by name."""
    template_path = TEMPLATES_DIR / f"{name}.py"
    if template_path.exists():
        return template_path.read_text()
    raise FileNotFoundError(f"Template not found: {name}")


def list_templates() -> list[str]:
    """List available templates."""
    return [p.stem for p in TEMPLATES_DIR.glob("*.py") if p.stem != "__init__"]
