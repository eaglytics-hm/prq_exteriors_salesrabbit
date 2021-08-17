from .utils import process


def test_leads():
    data = {
        "table": "Leads",
    }
    process(data)


def test_lead_status_histories():
    data = {
        "table": "LeadStatusHistories",
    }
    process(data)
