import yaml
import os
import base64
import io
import logging


def read_yaml(path):
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return data


def read_dag_specs(dir):
    return {f: read_yaml(f"{dir}/{f}") for f in os.listdir(dir)}


def parse_contents(contents, filename, date):
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)

    try:
        logging.info(
            f"content_type: {content_type}, filename: {filename}, date: {date}"
        )
        decoded_content = io.StringIO(decoded.decode("utf-8"))
        data = yaml.safe_load(decoded_content.read())
        return filename, data
    except Exception as e:
        logging.error(e)
        return filename, None
