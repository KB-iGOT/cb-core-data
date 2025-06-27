from spark_app import utils

def test_normalize_name():
    assert utils.normalize_name("  Sharath  ") == "sharath"
