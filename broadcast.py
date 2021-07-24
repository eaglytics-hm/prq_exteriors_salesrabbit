import os
import json

from google.cloud import pubsub_v1

API_VER = os.getenv("API_VER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BASE_URL = f"https://graph.facebook.com/{API_VER}/"
BUSINESS_ID = "444284753088897"

TABLES = [{"table": "Leads"}, {"table": "LeadStatusHistories"}]


def broadcast(broadcast_data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))

    for table in TABLES:
        data = {
            "table": table['table'],
            "start": broadcast_data.get("start"),
        }
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return {"message_sent": len(TABLES)}
