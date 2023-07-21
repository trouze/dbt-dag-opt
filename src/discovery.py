# query the dbt cloud discovery api to get the manifest and run results given a job id
# input: a job id
# output: a manifest and run results json file saved to file system
import requests, json

class Discovery:
    def __init__(self, account_id, job_id, token, file_method=False):
        self.account_id = account_id
        self.job_id = job_id
        self.token = token
        if file_method:
            self.manifest, self.run_results = self.query_discovery_api()
        else:
            self.manifest, self.run_results = self.query_discovery_api()

    def query_discovery_api(self):
        HEADERS = {"Authorization": self.token}
        # get the manifest
        manifest = requests.get(f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/jobs/{self.job_id}/artifacts/manifest.json", headers=HEADERS)
        # get the run results
        run_results = requests.get(f"https://cloud.getdbt.com/api/v2/accounts/{self.account_id}/jobs/{self.job_id}/artifacts/run_results.json", headers=HEADERS)
        return manifest, run_results
    
    def load_manifest_and_run_results(self, manifest_path, run_results_path):
        with open(manifest_path, 'r') as f:
            self.manifest = json.load(f)
        with open(run_results_path, 'r') as f:
            self.run_results = json.load(f)

    
    def get_manifest_and_run_results(self):
        return self.manifest, self.run_results
