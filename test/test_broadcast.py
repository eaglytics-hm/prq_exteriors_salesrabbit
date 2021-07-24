from unittest.mock import Mock

from main import main
from .utils import encode_data


def test_broadcast():
    data = {"broadcast": "lead"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    results = res["results"]
    assert results["message_sent"] > 0
