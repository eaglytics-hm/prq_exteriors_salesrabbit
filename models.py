import os
import json
from datetime import datetime, timezone
from abc import ABCMeta, abstractmethod

import requests
from google.cloud import bigquery
import jinja2


BASE_URL = "https://api.salesrabbit.com"
HEADERS = {
    "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
    "Content-Type": "application/json",
}

BQ_CLIENT = bigquery.Client()
DATASET = "SalesRabbit"

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)


class SalesRabbit(metaclass=ABCMeta):
    def __init__(self, start):
        self.table = self.get_table()
        self.keys, self.schema = self.get_config(self.table)
        self.start = self.get_start(start)

    @staticmethod
    def factory(table, start=None):
        if table == "Leads":
            return Leads(start)
        elif table == "LeadStatusHistories":
            return LeadStatusHistories(start)

    def get_start(self, _start):
        if _start:
            start = (
                datetime.strptime(_start, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%S")
                + "+00:00"
            )
        else:
            template = TEMPLATE_ENV.get_template("read_max_incre.sql.j2")
            rendered_query = template.render(
                incre_key=self.keys["incre_key"],
                dataset=DATASET,
                table=self.table,
            )
            results = BQ_CLIENT.query(rendered_query).result()
            row = [dict(row.items()) for row in results][0]
            start = row["incre"].strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"
        return start

    @abstractmethod
    def get_table(self):
        pass

    def get_config(self, table):
        with open(f"configs/{table}.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    def get(self):
        endpoint = self._get_endpoint()
        headers = {
            **HEADERS,
            **{
                "If-Modified-Since": self.start,
            },
        }
        url = f"{BASE_URL}/{endpoint}"
        params = {
            "perPage": 2000,
            "page": 1,
        }
        rows = []
        with requests.Session() as session:
            while True:
                with session.get(
                    url,
                    params=params,
                    headers=headers,
                ) as r:
                    r
                    if r.status_code == 304:
                        return []
                    res = r.json()
                rows.extend([res["data"]])
                more_pages = res["meta"]["morePages"]
                if more_pages:
                    params["page"] += 1
                else:
                    break
        return rows

    @abstractmethod
    def _get_endpoint(self):
        raise NotImplementedError

    def transform(self, _rows):
        rows = self._transform(_rows)
        return rows

    @abstractmethod
    def _transform(self, rows):
        raise NotImplementedError

    def load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()

    def update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.keys["p_key"]),
            incre_key=self.keys["incre_key"],
        )
        BQ_CLIENT.query(rendered_query)

    def run(self):
        rows = self.get()
        responses = {
            "table": self.table,
            "start": self.start,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            responses = {
                **responses,
                "num_processed": len(rows),
                "output_rows": loads.output_rows,
            }
        return responses


class LeadStatusHistories(SalesRabbit):
    def __init__(self, start):
        super().__init__(start)

    def get_table(self):
        return "LeadStatusHistories"

    def _get_endpoint(self):
        return "leadStatusHistories"

    def _transform(self, _rows):
        rows = [
            {
                **i,
                "lead_id": int(k),
            }
            for page in _rows
            for k, v in page.items()
            for i in v
        ]
        return rows


class Leads(SalesRabbit):
    def __init__(self, start):
        super().__init__(start)

    def get_table(self):
        return "Leads"

    def _get_endpoint(self):
        return "leads"

    def _transform(self, rows):
        rows = [item for sublist in rows for item in sublist]
        for row in rows:
            for field in [
                "customFields",
                "integrationData",
            ]:
                row[field] = json.dumps(row[field])
        return rows
