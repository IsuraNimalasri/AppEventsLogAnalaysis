"""
This module included basic util function for pipeline
"""
import json


def read_config(conf_path):
    """
    Reading pipeline configurations
    :param conf: pipline name
    :return: json
    """
    with open(conf_path, mode='r', encoding='utf-8') as f:
        conf_file = f.read()
        return json.loads(conf_file)
