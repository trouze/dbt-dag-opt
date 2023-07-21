import src.discovery as discovery
import src.graph_parser as gp
import src.longest_path as lp
import fire
import json

def main(manifest_path=None, run_results_path=None, account_id=None, job_id=None, token=None, file_method=False):
    if not file_method:
        manifest, run_results = discovery.Discovery().get_manifest_and_run_results(account_id, job_id, token)
    else:
        manifest, run_results = discovery.Discovery().load_manifest_and_run_results(manifest_path, run_results_path)
    weights = gp.Parser().get_unique_ids_and_execution_time(manifest['nodes'],run_results['results']) # {"from": 0.1, "from": 0.2}
    edges = gp.Parser().get_edges_and_weights(manifest['child_map'], weights) # {["from","to"], ["from","to"]"]}
    start_nodes = gp.Parser().get_start_nodes(manifest['parent_map']) # ["node", "node"]
    # for each starting node, find the longest path and append results to a dictionary
    longest_paths = {}
    for node in start_nodes:
        longest_paths.update(lp.find_longest_path(edges, node))
    # dump the dictionary to a json file
    with open('./longest_paths.json', 'w') as f:
        json.dump(longest_paths, f)
    # take the longest_paths json and print the top 5 longest paths to the terminal
    sorted_longest_paths = sorted(longest_paths.items(), key=lambda x: x[1]['distance'], reverse=True)
    for i in range(5):
        print(sorted_longest_paths[i])


if __name__ == '__main__':
    fire.Fire(main)