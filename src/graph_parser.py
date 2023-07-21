class Parser:
    def __init__(self) -> None:
        self.edges = []
        self.weights = []
        self.start_nodes = []

    # write a function that takes nodes and run_results unique_id and execution_time and maps the unique_id to the nodes and returns a dictionary of node:weight where weight is execution_time
    # when an execution_time is not found for a node in nodes, set the weight to 0
    # input: nodes, run_results
    # output: dictionary of node:weight
    # example: nodes = ["model.gtm_germany.deu_ims_rpm_transactions_product_line", "model.gtm_germany.deu_ims_rpm_master"], run_results = [{"unique_id": "model.gtm_germany.deu_ims_rpm_transactions_product_line", "execution_time": 0.1}, {"unique_id": "model.gtm_germany.deu_ims_rpm_master", "execution_time": 0.2}]
    # output: {"model.gtm_germany.deu_ims_rpm_transactions_product_line": 0.1, "model.gtm_germany.deu_ims_rpm_master": 0.2}
    def get_unique_ids_and_execution_time(self,nodes,run_results):
        for node in nodes:
            for result in run_results:
                if node == result['unique_id']:
                    self.weights.append(result['execution_time'])
            if node not in self.weights:
                self.weights.append(0)
        return dict(zip(nodes, self.weights))

    # write a function that takes a dictionary of nodes, their children in a list, and the weight of the nodes and returns a list of edges in a tuple, if no weight is found, set the weight to 0
    # if weight[node] returns a KeyError, set the weight to 0!
    # input: parent_map and weights
    # output: list of edges and weights
    # example: parent_map = {"model.gtm_germany.deu_ims_rpm_transactions_product_line": ["model.gtm_germany.deu_ims_rpm_master", "model.gtm_germany.deu_ims_rpm_master"], "model.gtm_germany.deu_ims_rpm_master": []}, weights = {"model.gtm_germany.deu_ims_rpm_transactions_product_line": 0.1, "model.gtm_germany.deu_ims_rpm_master": 0.2}
    # output: [("model.gtm_germany.deu_ims_rpm_transactions_product_line", "model.gtm_germany.deu_ims_rpm_master", 0.1), ("model.gtm_germany.deu_ims_rpm_transactions_product_line", "model.gtm_germany.deu_ims_rpm_master", 0.1)]
    def get_edges_and_weights(self,child_map,weights):
        for node in child_map:
            for child in child_map[node]:
                try:
                    self.edges.append((node, child, weights[node]))
                except KeyError:
                    self.edges.append((node, child, 0.0))
        return self.edges
        

    # write a function that intakes a dictionary of nodes and their neighbors and returns a list of the nodes that have no parents and start with 'model.' or 'source.'
    # input: graph
    # output: list of start nodes
    # example: graph = {"model.gtm_germany.deu_ims_rpm_transactions_product_line": ["model.gtm_germany.deu_ims_rpm_master", "model.gtm_germany.deu_ims_rpm_master"], "model.gtm_germany.deu_ims_rpm_master": []}
    # output: ["model.gtm_germany.deu_ims_rpm_transactions_product_line"]
    # graph = {"model.gtm_germany.deu_ims_rpm_transactions_product_line": ["model.gtm_germany.deu_ims_rpm_master", "model.gtm_germany.deu_ims_rpm_master"], "model.gtm_germany.deu_ims_rpm_master": []}
    def get_start_nodes(self,graph):
        for node in graph:
            if graph[node] == [] and (node.startswith('model.') or node.startswith('source.')):
                self.start_nodes.append(node)
        return self.start_nodes
