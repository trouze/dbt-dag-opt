# write an algorithm to perform a topological sort to find the longest paths given a list of edges and their weights, starting from a set of given nodes
# input: a list of edges and their weights, a list of starting nodes
# output: a list of longest paths starting from the given nodes
def longest_paths(edges, weights, start_nodes):
    # build a graph
    graph = {}
    for i in range(len(edges)):
        if edges[i][0] not in graph:
            graph[edges[i][0]] = {}
        graph[edges[i][0]][edges[i][1]] = weights[i]
    # topological sort
    sorted_nodes = []
    visited = set()
    for node in start_nodes:
        if node not in visited:
            dfs(graph, node, visited, sorted_nodes)
    # find the longest paths
    longest_paths = {}
    for node in sorted_nodes:
        if node not in graph:
            longest_paths[node] = 0
        else:
            longest_paths[node] = 0
            for neighbor in graph[node]:
                longest_paths[node] = max(longest_paths[node], longest_paths[neighbor] + graph[node][neighbor])
    return longest_paths

def dfs(graph, node, visited, sorted_nodes):
    visited.add(node)
    if node in graph:
        for neighbor in graph[node]:
            if neighbor not in visited:
                dfs(graph, neighbor, visited, sorted_nodes)
    sorted_nodes.append(node)