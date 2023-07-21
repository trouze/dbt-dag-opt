import src.graph_parser as gp
import src.longest_path as lp
import src.discovery as discovery
import fire

def main(account_id, job_id, token, file_method=False):
    if file_method:
        manifest, run_results = discovery.Discovery(account_id, job_id, token).get_manifest_and_run_results()
    else:
        manifest, run_results = discovery.Discovery(account_id, job_id, token, file_method=True).load_manifest_and_run_results('./artifacts/manifest.json', './artifacts/run_results.json')
    edges = gp.get_edges(manifest['child_map'])
    weights = gp.get_unique_ids_and_execution_time(run_results)
    start_nodes = gp.get_start_nodes(manifest['parent_map'])
    longest_paths = lp.longest_paths(edges, weights, start_nodes)
    with open('output.txt', 'w') as f:
        for node in longest_paths:
            f.write(f'{node}: {longest_paths[node]}\n')

if __name__ == '__main__':
    fire.Fire(main)