from .utils import process

START_DATE = '2021-07-01'
def test_leads():
    data = {"table": "Leads", "start": START_DATE}
    process(data)

def test_lead_status_histories():
    data = {"table": "LeadStatusHistories", "start": START_DATE}
    process(data)
