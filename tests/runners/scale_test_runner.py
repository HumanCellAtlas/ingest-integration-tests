import requests
import csv
import time
import os
import codecs
import json
from tests.fixtures.util import load_file


class ScaleTestRunner:

    def __init__(self):
        pass

    def run(self, locust_api_url, num_users, hatch_rate, time_under_load_seconds):
        locust_swarm_api = f'{locust_api_url}/swarm'
        stop_warm_api = f'{locust_api_url}/stop'
        start_swarm_request = requests.post(locust_swarm_api, data={"locust_count": num_users, "hatch_rate": hatch_rate})

        if start_swarm_request.status_code == 200:
            time.sleep(time_under_load_seconds)
            csv_results = requests.get("http://locust.ingest.testing.data.humancellatlas.org/stats/requests/csv", stream=True)
            csv_results_reader = csv.DictReader(codecs.iterdecode(csv_results.iter_lines(), 'utf-8'), delimiter=',')

            stop_swarm_request = requests.get(stop_warm_api)

            slack_payload = SlackPayload()
            slack_payload.add_slack_payload_field("Num. users", str(num_users))
            slack_payload.add_slack_payload_text_line(",".join(csv_results_reader.fieldnames))

            rows = list()
            for row in csv_results_reader:
                rows.append(row)
                row_values = list(row.values())
                slack_payload.add_slack_payload_text_line(",".join(row_values))

            last_row = rows[-1]
            slack_payload.format_results_jist(last_row)
            slack_webhook_url = ScaleTestRunner._get_slack_webhook_url()
            requests.post(slack_webhook_url, json=True, data=json.dumps(slack_payload.to_dict()))
        else:
            raise Exception(f'Error requesting locust swarm -- \n status code %s\n response %s',
                            str(start_swarm_request.status_code),
                            str(start_swarm_request.text))


    @staticmethod
    def _get_slack_webhook_url() -> str:
        slack_webhook = load_file(os.environ.get('SLACK_WEBHOOK_URL'))
        return slack_webhook["webhook_url"]


class SlackPayload:

    def __init__(self):
        self._slack_payload = SlackPayload._init_slack_payload()

    @staticmethod
    def _init_slack_payload() -> dict:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return load_file(f'{dir_path}/../templates/scale-test-results-template.json')

    def format_results_jist(self, final_results_row: dict) -> dict:
        for result_key, result_value in final_results_row.items():
            self.add_slack_payload_field(result_key, result_value)
        return self._slack_payload

    def add_slack_payload_field(self, title: str, value: str) -> dict:
        self._slack_payload["attachments"][0]["fields"].append({
                "title": title,
                "value": value,
                "short": False
        })
        return self._slack_payload

    def add_slack_payload_text_line(self, text: str) -> dict:
        self._slack_payload["attachments"][0]["text"] = "\n".join([self._slack_payload["attachments"][0]["text"], text])
        return self._slack_payload

    def to_dict(self) -> dict:
        return self._slack_payload
