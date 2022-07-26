"""Implements unit tests for the main Graphlet module."""
from graphlet import __version__


def test_version():
    """test_version Make sure the package version is accurate."""
    assert __version__ == "0.1.0"
