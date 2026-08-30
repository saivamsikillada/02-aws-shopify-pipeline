import importlib


def test_boto3_import():
    assert importlib.import_module("boto3")