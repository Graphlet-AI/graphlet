"""Parse the DBLP data to train the entity resolution model for property graphs."""

import gzip
import os
import uuid
from typing import Any, List, Optional, Union
from urllib.parse import unquote, urlparse

import pandas as pd

# import pandera as pa
import requests
import tqdm
import ujson
import xmltodict

# from graphlet.etl import NodeSchema
from graphlet.paths import get_data_dir

# from pandera import Field
# from pandera.dtypes import DateTime
# from pandera.typing import Series


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
        "chapter",
        "journal",
        "month",
        "number",
        "publnr",
        "volume",
    ],
    # Just for docs, not used below
    "complex": [
        "author",
        "editor",
        "series",
        "ee",
        "note",
        "title",
        "url",
        "isbn",
        "pages",
        "publisher",
        "school",
        "cdrom",
        "crossref",
        "year",
    ],
}

# Just for docs, not used below
GRAPHLET_COLUMNS = ["entity_id", "entity_type", "entity_class"]

pd.set_option("display.max_columns", None)


# class DBLPNodeSchema(NodeSchema):
#     """DBLPNodeSchema - subclass of NodeSchema for DBLP nodes."""

#     key: Series[str] = Field(nullable=False, str_length=(3,))
#     mdate: Series[str] = DateTime(nullable=False)
#     cdate: Series[str] = DateTime(nullable=True)
#     address: Series[str] = Field(nullable=True)
#     booktitle: Series[str] = Field(nullable=True)
#     cdrom: Series[str] = Field(nullable=True)
#     chapter: Series[str] = Field(nullable=True)
#     crossref: Series[str] = Field(nullable=True)
#     isbn: Series[str] = Field(nullable=True)
#     journal: Series[str] = Field(nullable=True)
#     month: Series[str] = Field(nullable=True)
#     number: Series[str] = Field(nullable=True)
#     note: Series[str] = Field(nullable=True)
#     pages: Series[str] = Field(nullable=True)
#     publisher: Series[str] = Field(nullable=True)
#     publnr: Series[str] = Field(nullable=True)
#     school: Series[str] = Field(nullable=True)
#     volume: Series[str] = Field(nullable=True)
#     year: Series[str] = Field(nullable=True)


def download(url=DBLP_XML_URL, folder: str = get_data_dir(), gzip_=True) -> None:
    """download Download a file like the DBLP data and store in the data directory.

    We can't store and redistribute it and it is regularly updated.

    Parameters
    ----------
    url : str, optional
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
            for obj_ in tqdm.tqdm(records, total=len(records)):
                # Encode the JSON, we are writing gzip
                f.write((ujson.dumps(obj_) + "\n").encode())


def profile_df(df: pd.DataFrame) -> Any:
    """profile_df Given a DBLP DataFrame, determine the column types by their values.

    Parameters
    ----------
    x : pandas.DataFrame
        A DataFrame with columns of different types of values.

    Returns
    -------
    typing.Any
        A report on what the column types should be to represent this data.
    """
    pass
    # for col_ in df.columns:

    #     s = df[col_]
    #     types_ = s.apply(lambda x: type(x))
    #     unique_types = s.unique()


def parse_type_util(x: Any, text_key: str, other_key: Optional[str] = None, default_other=None) -> List[dict]:
    """parse_type_util Given a list, dict or string, parse it into dict form.

    Parameters
    ----------
    x : typing.Any
        An instance of a person, note, etc.
    text_key : str
        Key to the #text field
    other_key : typing.Optional[str]
        Key to the other field
    default_other : typing.Optional[str]
        Default value for the other field

    Returns
    -------
    dict
        A dictionary with text_key and other_key fields
    """

    d: List[dict] = []

    # Strings go into the #text field, then set the other key's default value
    if isinstance(x, str):

        r = {"#text": x}

        if other_key and other_key in x:
            r.update({other_key: default_other})

        d.append(r)

    # Dicts go straight though
    if isinstance(x, dict):

        r = {text_key: x[text_key]}

        if other_key and other_key in x:
            r.update({other_key: x[other_key] or default_other})

        d += [r]

    # Lists are always
    if isinstance(x, list):
        for y in x:
            d += parse_type_util(y, text_key, other_key, default_other)

    return d


def parse_note(x: Union[str, list, dict]):
    """parse_note_instance use parse_type_to_dict to prase a note.

    Parameters
    ----------
    x : typing.Union[str, dict]
        A note to parse

    Returns
    -------
    str
        A parsed note
    """

    if isinstance(x, str):
        return x

    if isinstance(x, dict):
        return x.get("#text")

    return None


def parse_person(x: Union[str, dict]) -> List[dict]:
    """parse_person parse a string or dict instance of a person into a dict.

    Parameters
    ----------
    x : dict
        The input dictionary
    node : dict
        The in progress output dictionary
    """
    return parse_type_util(x, "#text", "@orcid", None)


def parse_ee(x: Any) -> Optional[List[dict]]:
    """parse_ee parse the ee record whether it is a string or dict."""

    return parse_type_util(x, "#text", "@type", "unknown")


def parse_title(x: Optional[Union[str, dict]]) -> Optional[str]:
    """parse_title parse the title str/dict of an article.

    Parameters
    ----------
    x : typing.Optional[typing.Union[str, dict]]


    Returns
    -------
    typing.Optional[str]
        Return the string, #text dict key or None
    """

    t: Optional[str] = None
    if isinstance(x, str):
        t = x
    elif isinstance(x, dict):  # noqa: SIM102
        t = x.get("#text")

    return t


def parse_url(x: Optional[Union[str, float, list]]) -> Any:
    """parse_url parse the urls which can be strings, lists of strings or floats (always NaN).

    Parameters
    ----------
    x : typing.Optional[typing.Union[str, float, list]]
        The input type: str, List[str] or float = NaN

    Returns
    -------
    str
        A string url for the article
    """

    if isinstance(x, str):
        return x
    if isinstance(x, list) and len(x) > 0:
        return x[0]

    return None


def parse_isbn(x: Optional[Union[str, List[str]]]) -> Optional[str]:
    """parse_isbn turn the isbn into a string.

    Parameters
    ----------
    x : Optional[Union[str, List[str]]]
        An optional string or list of strings

    Returns
    -------
    Optional[str]
        A string ISBN or None
    """

    i = None

    # Given a list, dump one ISBN
    if isinstance(x, list) and len(x) > 0:
        if isinstance(x[0], dict):
            i = x[0].get("#text")
        else:
            i = x[0]

    if isinstance(x, dict):  # noqa: SIM102
        i = x.get("#text")

    return i


def parse_pages(x: Optional[Union[str, list]]) -> Optional[str]:
    """parse_pages parse the pages field.

    Parameters
    ----------
    x : Optional[Union[str, dict]]
        The pages field

    Returns
    -------
    Optional[str]
        A string of the pages
    """

    p = None

    if isinstance(x, str):
        p = x

    if isinstance(x, list):
        p = ", ".join(x)

    return p


def parse_publisher(x: Optional[Union[str, dict]]) -> Optional[str]:
    """parse_publisher parse the publisher field.

    Parameters
    ----------
    x : Optional[Union[str, dict]]
        The publisher field

    Returns
    -------
    Optional[str]
        A string of the publisher
    """

    p = None

    if isinstance(x, str):
        p = x

    if isinstance(x, dict):
        p = x.get("#text")

    return p


def parse_school(x: Optional[Union[str, list]]) -> Optional[str]:
    """parse_school parse the school field.

    Parameters
    ----------
    x : Optional[Union[str, list]]
        The school field

    Returns
    -------
    Optional[str]
        A string of the school
    """

    s = None

    if isinstance(x, str):
        s = x

    if isinstance(x, list):
        s = ", ".join(x)

    return s


def parse_cdrom(x: Optional[Union[str, list]]) -> Optional[str]:
    """parse_cdrom parse the cdrom field.

    Parameters
    ----------
    x : Optional[Union[str, list]]
        The cdrom field

    Returns
    -------
    Optional[str]
        A string of the cdrom
    """

    c = None

    if isinstance(x, str):
        c = x

    if isinstance(x, list):
        c = ", ".join(x)

    return c


def parse_crossref(x: Optional[Union[str, list]]) -> Optional[str]:
    """parse_crossref Prase the cross reference field, taking the string or first list element.

    Parameters
    ----------
    x : Optional[Union[str, list]]
        The crossref field

    Returns
    -------
    Optional[str]
        A string of the crossref
    """

    c = None

    if isinstance(x, str):
        c = x

    if isinstance(x, list) and len(x) > 0:
        c = x[0]

    return c


def parse_year(x: Optional[Union[str, list]]) -> Optional[str]:
    """parse_year parse the year field.

    Parameters
    ----------
    x : Optional[Union[str, list]]
        The year field

    Returns
    -------
    Optional[sr]
        A stroing of the year
    """

    y = None

    if isinstance(x, str):
        y = x

    if isinstance(x, list) and len(x) > 0:
        y = x[0]

    return y


def build_node(x: dict, class_type: str) -> dict:  # noqa: C901
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
        node[column] = x[column] if column in x else None

    # Handle "author" as a list, string or dict and always create an "authors" field as a list of objects
    if "author" in x:
        node["authors"] = parse_person(x["author"])

    # Handle "editor" as a list, string or dict and always create an "editors" field as a list of objects
    if "editor" in x:

        node["editors"] = parse_person(x["editor"])

    # Handle "series" which can be a string or dict
    if "series" in x:

        if isinstance(x["series"], str):
            node["series_text"] = x["series"]
            node["series_href"] = None
        if isinstance(x["series"], dict):
            node["series_text"] = x["series"]["#text"]
            node["series_href"] = x["series"]["@href"]
    else:
        node["series_text"] = None
        node["series_href"] = None

    # Parse the "ee" field which can be str, list(str), dict or list(dict)
    if "ee" in x:
        if isinstance(x["ee"], list):
            node["ee"] = [parse_ee(e) for e in x["ee"]]
        else:
            node["ee"] = [parse_ee(x["ee"])]

    # Parse the note using the new parse_note
    if "note" in x:
        node["note"] = parse_note(x["note"])

    # Parse the string or dict title and get just the string title
    if "title" in x:
        node["title"] = parse_title(x["title"])

    if "isbn" in x:
        node["isbn"] = parse_isbn(x["isbn"])

    if "pages" in x:
        node["pages"] = parse_pages(x["pages"])

    if "publisher" in x:
        node["publisher"] = parse_publisher(x["publisher"])

    if "school" in x:
        node["school"] = parse_school(x["school"])

    if "cdrom" in x:
        node["cdrom"] = parse_cdrom(x["cdrom"])

    if "crossref" in x:
        node["crossref"] = parse_crossref(x["crossref"])

    if "year" in x:
        node["year"] = parse_year(x["year"])

    return node


def build_nodes() -> None:
    """build_nodes build a network out of the DBLP data including SAME_AS edges for authors."""
    dfs = {}
    nodes = []
    types_ = [
        "article",
        "book",
        "incollection",
        "inproceedings",
        "mastersthesis",
        "phdthesis",
        "proceedings",
        "www",
    ]

    for type_ in types_:
        path_ = f"data/types/{type_}.json.gz"

        # Load each type's Gzip JSON Lines file and build a pd.DataFrame
        print(f"Opening {type_} records at {path_} ...")
        with gzip.GzipFile(filename=path_, mode="rb") as f:

            record_count = sum([1 for x in f])
            f.seek(0)

            print(f"Parsing JSON records for {path_} ...")
            records = [ujson.loads(record.decode()) for record in tqdm.tqdm(f, total=record_count)]
            dfs[type_] = pd.DataFrame.from_records(records)

            print(f"Building nodes for class {type_} ...")
            type_nodes = []
            for index, row in tqdm.tqdm(dfs[type_].iterrows(), total=len(dfs[type_].index)):
                d = row.to_dict()
                n = build_node(d, type_)
                nodes.append(n)

                type_nodes.append(n)

            print(f"Creating DataFrame for {type_} ...")
            type_df = pd.DataFrame(type_nodes)
            type_df.head()

            print(f"Writing {type_} to Parquet ...")
            type_df.to_parquet(f"data/types/{type_}.parquet")

            print(f"Class {type_} completed! Finished writing {type_} to Parquet ...")

    node_df = pd.DataFrame(nodes)
    print(node_df.head())

    node_df.to_parquet("data/dblp.nodes.parquet")


def build_edges() -> None:
    """build_edges given the nodes, build the edges.

    Parameters
    ----------
    node_df : pd.DataFrame
        A DataFrame of the uniform schema defined at https://gist.github.com/rjurney/c5637f9d7b3bfb094b79e62a704693da
    """

    node_df = pd.read_parquet("data/dblp.nodes.parquet")

    edges = []
    types_ = [
        "article",
        "book",
        "incollection",
        "inproceedings",
        "mastersthesis",
        "phdthesis",
        "proceedings",
        "www",
    ]

    for type_ in types_:

        for index, row in tqdm.tqdm(node_df.iterrows(), total=len(node_df.index)):
            if "authors" in row:
                for author in row["authors"]:

                    # NEXT LINE NOT DONE
                    author_entity_id = ""
                    edges.append(
                        {
                            "entity_id": str(uuid.uuid4()),
                            "entity_type": "edge",
                            "class_type": "AUTHORED",
                            "src": row["entity_id"],
                            "dst": author_entity_id,
                        }
                    )

            if "editors" in row:
                for editor in row["editors"]:

                    # NEXT LINE NOT DONE
                    editor_entity_id = ""
                    edges.append(
                        {
                            "entity_id": str(uuid.uuid4()),
                            "entity_type": "edge",
                            "class_type": "EDITED",
                            "src": row["entity_id"],
                            "dst": editor_entity_id,
                        }
                    )

    edge_df = pd.DataFrame(edges)
    print(edge_df.head())

    edge_df.to_parquet("data/dblp.edges.parquet")


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
    # Build a uniform set of network edges
    build_edges()


if __name__ == "__main__":
    main()
