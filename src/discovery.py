# query the dbt cloud discovery api to get the manifest and run results given a job id
# input: a job id
# output: a manifest and run results json file saved to file system
import requests, json

class Discovery:
    def __init__(self):
        self.manifest = {}
        self.run_results = {}

    def query_discovery_api(self, account_id, job_id, token):
        HEADERS = {"Authorization": token}
        # get the manifest
        self.manifest = requests.get(f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/artifacts/manifest.json", headers=HEADERS)
        # get the run results
        self.run_results = requests.get(f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/artifacts/run_results.json", headers=HEADERS)
        return self.manifest, self.run_results
    
    def load_manifest_and_run_results(self, manifest_path, run_results_path):
        with open(manifest_path, 'r') as f:
            self.manifest = json.load(f)
        with open(run_results_path, 'r') as f:
            self.run_results = json.load(f)
        return self.manifest, self.run_results
    
    def get_manifest_and_run_results(self):
        return self.manifest, self.run_results
