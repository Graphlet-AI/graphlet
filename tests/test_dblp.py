"""Test the graphlet.dblp module - downloading, parsing & processing the DBLP database."""

import gzip
import os
from urllib.parse import unquote, urlparse

import xmltodict

# from graphlet.dblp import dblp_to_json_lines
from graphlet.dblp import download
from graphlet.paths import get_data_dir


def test_download() -> None:
    """test_download_dblp Test downloading the DBLP data by parsing a smaller XML file."""
    url = "https://dblp.org/xml/osd.xml"

    # Change me to test
    gzip_ = False

    download(url, gzip_=gzip_)
    file_name = os.path.basename(unquote(urlparse(url).path))
    print(f"Test file_name: {file_name}")
    input_path = f"{get_data_dir()}/{file_name}.gz" if gzip_ else f"{get_data_dir()}/{file_name}"
    print(f"Test file_path: {input_path}")
    read_mode = "rb" if gzip_ else "r"
    print(f"Test read_mode: {read_mode}")

    xml_string = ""
    if gzip_:
        with gzip.GzipFile(filename=input_path, mode=read_mode) as f:
            xml_string = f.read().decode()
    else:
        with open(input_path, read_mode) as f:
            xml_string = f.read()

    parsed_xml = xmltodict.parse(xml_string)
    assert isinstance(parsed_xml, dict)
    assert len(parsed_xml.keys()) > 0


def test_download_dblp_gzip() -> None:
    """test_download_dblp Test downloading the DBLP data by parsing a smaller XML file and writing via gzip."""
    url = "https://dblp.org/xml/osd.xml"

    # Change me to test
    gzip_ = True

    download(url, gzip_=gzip_)
    file_name = os.path.basename(unquote(urlparse(url).path))
    print(f"Test file_name: {file_name}")
    input_path = f"{get_data_dir()}/{file_name}.gz" if gzip_ else f"{get_data_dir()}/{file_name}"
    print(f"Test file_path: {input_path}")
    read_mode = "rb" if gzip_ else "r"
    print(f"Test read_mode: {read_mode}")

    xml_string = ""
    if gzip_:
        with gzip.GzipFile(filename=input_path, mode=read_mode) as f:
            xml_string = f.read().decode()
    else:
        with open(input_path, read_mode) as f:
            xml_string = f.read()

    parsed_xml = xmltodict.parse(xml_string)
    assert isinstance(parsed_xml, dict)
    assert len(parsed_xml.keys()) > 0


# def test_dblp_to_json_lines() -> None:
#     """test_dblp_to_json_lines test writing JSON/JSON Lines from the DBLP XML."""
#     dblp_to_json_lines(gzip_=False)
