"""Test the graphlet.utils module."""


from graphlet.paths import get_data_dir, get_project_root


def test_get_project_root() -> None:
    """test_get_project_root Test graphlet.paths."""

    project_root = get_project_root()
    folders = project_root.split("/")[1:]
    assert folders[-1] == "graphlet"
    assert folders[-2] != "graphlet"


def test_get_data_dir() -> None:
    """test_get_data_dir Test graphlet.utils.get_data_dir."""

    data_dir = get_data_dir()
    folders = data_dir.split("/")[1:]
    assert folders[-2] == "graphlet"
    assert folders[-1] == "data"
