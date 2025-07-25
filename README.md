# dbt-dag-opt
Struggling with long running dbt pipelines? Use this utility to determine the most troublesome paths through your dbt DAG by total execution time. Just because a model is long running, doesn't mean improving it's run time will materially speed up your dbt jobs. Long chained models with comparatively faster runtimes can add up and slow down total pipeline execution time. This utility uses a longest path algorithm to determine your longest running paths through your DAG, starting with each of your sources in dbt.

This package uses [Fire](https://python-fire.readthedocs.io/en/latest/) to run like a CLI. To get started, you can either run using the dbt Cloud Admin API, or pass file paths for your `manifest.json` and `run_results.json` files.

File path method
```
python3 entrypoint.py --file_method=True --manifest_path='artifacts/manifest.json' --run_results_path='artifacts/run_results.json'
```

dbt Cloud API
```
python3 entrypoint.py --account_id='<my_id>' --job_id='<job_id>' --token='<api_token>'
python3 entrypoint.py --base-url='https://cu288.us1.dbt.com' --account_id='70437463654419' --job_id='70437463655408' --token='dbtu_hayC4-EeNKK-lNbu5xYspNEhbLFeQK1ojfNXAC58J_qr2lRBwA'
```

The utility will save a json file to your working directory that has information on the longest path in your DAG for each starting node (usually sources). It's recommended to use this information to divide and conquer what models you should seek to optimize in order to shorten your pipeline runtimes. 