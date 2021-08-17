from .utils import process

START = "2021-08-17"


def test_leads():
    data = {
        "table": "Leads",
        "start": START,
    }
    process(data)


def test_lead_status_histories():
    data = {
        "table": "LeadStatusHistories",
        "start": START,
    }
    process(data)
