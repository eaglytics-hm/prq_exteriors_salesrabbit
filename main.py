import json
import base64

from models import SalesRabbit
from broadcast import broadcast


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if data:
        if 'broadcast' in data:
            results = broadcast(data)
        else:
            results = SalesRabbit.factory(data["table"], data.get("start")).run()

        responses = {"pipelines": "SalesRabbit", "results": results}
        print(responses)
        return responses
    else:
        raise NotImplementedError(data)
