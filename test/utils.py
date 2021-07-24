import json
import base64
from unittest.mock import Mock

from main import main

def encode_data(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    return {"message": {"data": data_encoded}}

def assertion(res):
    results = res['results']
    assert results["num_processed"] >= 0
    if results["num_processed"] > 0:
        assert results["output_rows"] > 0
        assert results["num_processed"] == results["output_rows"]

def process(data):
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assertion(res)

