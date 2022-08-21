"""Parse the DBLP data to train the entity resolution model for property graphs."""

import gzip
import os
from urllib.parse import unquote, urlparse

import requests
import tqdm
import ujson
import xmltodict

from graphlet.paths import get_data_dir

DBLP_XML_URL = "https://dblp.org/xml/dblp.xml.gz"


def download(url=DBLP_XML_URL, folder: str = get_data_dir(), gzip_=True) -> None:
    """download Download a file like the DBLP data and store in the data directory.

    We can't store and redistribute it and it is regularly updated.

    Parameters
    ----------
    url : _type_, optional
        url to fetch, by default DBLP_XML_URL
    folder: str, by default get_data_dir()
    gzip_ : bool, optional
        gzip the output, by default True
    """
    file_name = os.path.basename(unquote(urlparse(url).path))
    response = requests.get(
        url,
    )

    output_path = f"{folder}/{file_name}.gz" if gzip_ else f"{folder}/{file_name}"
    write_mode = "wb" if gzip_ else "w"

    if gzip_:
        with gzip.GzipFile(filename=output_path, mode=write_mode) as f:
            f.write(response.content)
    else:
        with open(output_path, write_mode) as f:
            f.write(response.text)


def dblp_to_json_lines(folder: str = get_data_dir(), gzip_: bool = True) -> None:
    """dblp_to_json_lines write the types in DBLP out to their own JSON Lines files.

    Parameters
    ----------
    folder : str, optional
        folder to read XML from and save JSON Lines to, by default get_data_dir()
    gzip_ : bool, optional
        gzip the output, by default True
    """

    input_path = f"{folder}/dblp.xml.gz" if gzip_ else f"{folder}/dblp.xml"
    read_mode = "rb" if gzip_ else "r"

    # Takes a lot of RAM but it fits
    xml_string = ""
    if gzip_:
        with gzip.GzipFile(filename=input_path, mode=read_mode) as f:
            xml_string = f.read().decode()
    else:
        with open(input_path, "r") as f:
            xml_string = f.read()

    # Parse it all at once. The data is under the "dblp" object, one key per type.
    # Dump to JSON Lines as an easily parseable format.
    parsed_xml = xmltodict.parse(xml_string)
    with open(f"{folder}/dblp.json", "w") as f:
        ujson.dump(parsed_xml, f)

    # Write each type out to its own JSON Lines file
    for type_, records in parsed_xml["dblp"].items():

        out_path = f"{folder}/{type_}.json"
        print(f"Writing DBLP type {type_} to {out_path} ...")

        with open(out_path, "w") as f:
            # Dump each record with speedy ujson, and a progress bar.
            for obj_ in tqdm.tqdm(records):
                f.write(ujson.dumps(obj_) + "\n")
