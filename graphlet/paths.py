"""General purpose utilities."""

from pathlib import Path


def get_project_root() -> str:
    """Get the full path to the project root."""
    # return os.path.abspath("").parent.parent
    return str(Path(__file__).parent.parent.resolve())


def get_data_dir() -> str:
    """Get the data directory for the project."""
    return f"{get_project_root()}/data"
