"""
This module included basic util function for pipeline
"""
import json


def read_config(conf):
    """
    Reading pipeline configurations
    :param conf: pipline name
    :return: json
    """
    with open(f"../pipeline-meta/cfg_{conf}.json", mode='r', encoding='utf-8') as f:
        conf_file = f.read()
        return json.loads(conf_file)
