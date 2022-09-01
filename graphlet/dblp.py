"""Parse the DBLP data to train the entity resolution model for property graphs."""

import gzip
import os
import typing
import uuid
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
import tqdm
import ujson
import xmltodict

from graphlet.paths import get_data_dir

DBLP_XML_URL = "https://dblp.org/xml/dblp.xml.gz"
DBLP_LABELS_URL = " https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/projekte/repeatability/DBLP/dblp50000.xml"
DBLP_COLUMNS = {
    "simple": [
        "@key",
        "@cdate",
        "@mdate",
        "@publtype",
        "address",
        "booktitle",
        "cdrom",
        "chapter",
        "cite",
        "crossref",
        "ee",
        "isbn",
        "journal",
        "month",
        "note",
        "number",
        "pages",
        "publisher",
        "publnr",
        "school",
        "title",
        "url",
        "volume",
        "year",
    ],
    # Just for docs, not used below
    "complex": [
        "author",
        "editor",
        "series",
    ],
}

# Just for docs, not used below
GRAPHLET_COLUMNS = ["entity_id", "entity_type", "entity_class"]

pd.set_option("display.max_columns", None)


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
    # Dump to JSON Lines as an easily parseable format with gzip compression.
    parsed_xml = xmltodict.parse(xml_string)
    with gzip.GzipFile(filename=f"{folder}/dblp.json.gz", mode="wb") as f:
        xml_string = ujson.dumps(parsed_xml)
        f.write(xml_string.encode())

    # Write each type out to its own JSON Lines file
    for type_, records in parsed_xml["dblp"].items():

        out_path = f"{folder}/types/{type_}.json"
        print(f"Writing DBLP type {type_} to {out_path} ...")

        # Write gzip compressed files
        with gzip.GzipFile(filename=out_path, mode="wb") as f:

            # Dump each record with speedy ujson, and a progress bar.
            for obj_ in tqdm.tqdm(records):
                # Encode the JSON, we are writing gzip
                f.write((ujson.dumps(obj_) + "\n").encode())


def build_nodes() -> None:
    """build_network build a network out of the DBLP data including SAME_AS edges for authors."""
    dfs = {}
    nodes = []

    for type_ in [
        "article",
        "book",
        "incollection",
        "inproceedings",
        "mastersthesis",
        "phdthesis",
        "proceedings",
        "www",
    ]:
        path_ = f"data/types/{type_}.json.gz"

        # Load each type's Gzip JSON Lines file and build a pd.DataFrame
        with gzip.GzipFile(filename=path_, mode="rb") as f:
            records = [ujson.loads(record.decode()) for record in f]
            dfs[type_] = pd.DataFrame.from_records(records)

            for type_, df in dfs.items():

                for index, row in df.iterrows():
                    d = row.to_dict()
                    n = build_node(d, type_)
                    nodes.append(n)

    node_df = pd.DataFrame(nodes)
    node_df.to_parquet("data/dblp.nodes.parquet")


def parse_series(x: typing.Union[str, dict]) -> dict:
    """parse_series "series" can be a string or dict, always return a dict.

    Parameters
    ----------
    x : dict
        input dictionary
    node : dict
        output dictionary

    Returns
    -------
    dict
        output dictionary with series field
    """

    y = {}
    if isinstance(x, str):
        y = {
            "#text": x,
            "@href": None,
        }
    if isinstance(x, dict):
        y = x

    return y


def parse_person_instance(x: typing.Union[str, dict]) -> dict:
    """parse_person_instance parse a string or dict instance of a person into a dict.

    Parameters
    ----------
    x : dict
        The input dictionary
    node : dict
        The in progress output dictionary
    """

    p = {}
    if isinstance(x, str):
        p = {"#text": x, "@orcid": None}
    if isinstance(x, dict):
        p = x

    return p


def build_node(x: dict, class_type: str) -> dict:
    """build_node parse a DBLP dict from the parsed XML and turn it into a node record with all columns.

    Parameters
    ----------
    x : typing.Dict[str: typing.Any]
        A dict from any of the types of XML records in DBLP.

    Returns
    -------
    dict
        A complete dict with all fields in an identical format.
    """

    node: dict = {"entity_id": str(uuid.uuid4()), "entity_type": "node", "class_type": class_type}

    for column in DBLP_COLUMNS["simple"]:
        if column in x:
            node[column] = x
        else:
            node[column] = None

    # Handle "author" as a list, string or dict and always create an "authors" field as a list of objects
    if "author" in x:

        if isinstance(x["author"], list):
            node["author"] = []
            for a in x["author"]:
                node["author"].append(parse_person_instance(a))
        else:
            node["author"] = [parse_person_instance(x)]

    else:
        node["authors"] = [
            {
                "#text": None,
                "@orcid": None,
            }
        ]

    # Handle "editor" as a list, string or dict and always create an "editors" field as a list of objects
    if "editor" in x:

        if isinstance(x["editor"], list):
            node["editor"] = []
            for a in x["editor"]:
                node["editor"].append(parse_person_instance(a))
        else:
            node["editor"] = [parse_person_instance(x)]

    else:
        node["editors"] = [
            {
                "#text": None,
                "@orcid": None,
            }
        ]

    # Handle "series" which can be a string or dict
    if "series" in x:
        node["series"] = parse_series(x["series"])
    else:
        node["series"] = {
            "#text": None,
            "@href": None,
        }

    return node


def main() -> None:
    """main get the DBLP XML and entity resolution labels, then ETL build a network."""

    # Download the XML for DBLP
    download(DBLP_XML_URL, gzip_=True)
    # Download the labels for DBLP
    download(DBLP_LABELS_URL, gzip_=True)
    # Convert DBLP to JSON Lines
    dblp_to_json_lines(gzip_=True)
    # Build a uniform set of network nodes: https://gist.github.com/rjurney/c5637f9d7b3bfb094b79e62a704693da
    build_nodes()


if __name__ == "__main__":
    main()
