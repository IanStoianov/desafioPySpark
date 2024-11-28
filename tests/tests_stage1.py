import os 
import sys
import json

from unittest import mock
import pytest

HERE = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "../src"))

sys.path.insert(0, PROJECT_ROOT)

from stage1 import Stage1, query_api

def stage1_err(*args):
    raise Exception()


def test_query_API():
    list_url = "https://api.openbrewerydb.org/v1/breweries"
    params={'page':1,
        'per_page':50}
    retorno=query_api(list_url, params)

    assert len(retorno.url) == "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=50"

def test_query_API_ludicrous_range():
    list_url = "https://api.openbrewerydb.org/v1/breweries"
    params={'page':5000,
        'per_page':50}
    retorno=query_api(list_url, params)

    assert len(retorno.text) == []