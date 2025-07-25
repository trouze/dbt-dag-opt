import src.discovery as discovery
import src.graph_parser as gp
import src.longest_path as lp
import fire
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

def compute_longest_path(edges, node):
    return lp.find_longest_path(edges, node)

def main(manifest_path=None, run_results_path=None, base_url="https://cloud.getdbt.com", account_id=None, job_id=None, token=None, file_method=False):
    if not file_method:
        manifest, run_results = discovery.Discovery().query_discovery_api(base_url, account_id, job_id, token)
        manifest = manifest.json()
        run_results = run_results.json()
    else:
        manifest, run_results = discovery.Discovery().load_manifest_and_run_results(manifest_path, run_results_path)
    weights = gp.Parser().get_unique_ids_and_execution_time(manifest['nodes'],run_results['results']) # {"from": 0.1, "from": 0.2}
    edges = gp.Parser().get_edges_and_weights(manifest['child_map'], weights) # {["from","to"], ["from","to"]"]}
    start_nodes = gp.Parser().get_start_nodes(manifest['parent_map']) # ["node", "node"]
    # for each starting node, find the longest path and append results to a dictionary
    longest_paths = {}
    output_file = './longest_paths.json'

    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(compute_longest_path, edges, node): node for node in start_nodes}

        with open(output_file, 'a') as f:  # Open in append mode
            for i, future in enumerate(tqdm(as_completed(futures), total=len(start_nodes), desc="Processing nodes")):
                node = futures[future]
                try:
                    result = future.result(timeout=20)
                    longest_paths.update(result)

                    # Write result incrementally to JSON
                    for key, value in result.items():
                        json.dump({key: value}, f)
                        if i < len(start_nodes) - 1:
                            f.write(',\n')
                        else:
                            f.write('\n')
                except TimeoutError:
                    print(f"Task for node {node} timed out and was canceled.")
                    future.cancel()  # Explicitly cancel the task
                except Exception as e:
                    print(f"Task for node {node} failed with error: {e}")

    # take the longest_paths json and print the top 5 longest paths to the terminal
    sorted_longest_paths = sorted(longest_paths.items(), key=lambda x: x[1]['distance'], reverse=True)
    for i in range(5):
        print(sorted_longest_paths[i])


if __name__ == '__main__':
    fire.Fire(main)