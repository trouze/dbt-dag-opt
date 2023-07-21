def find_longest_path(edges, starting_node):
    def dfs(node, current_path, current_distance):
        nonlocal longest_path, max_distance
        current_path.append(node)

        if current_distance > max_distance:
            longest_path = current_path.copy()
            max_distance = current_distance

        for edge in edges:
            from_node, to_node, distance = edge

            if distance == 'null':
                distance = 0.0
            else:
                distance = float(distance)

            if from_node == node and to_node not in current_path:
                dfs(to_node, current_path, current_distance + distance)

        current_path.pop()

    longest_path = []
    max_distance = 0.0
    dfs(starting_node, [], 0.0)

    return {starting_node: {'path': longest_path, 'distance': max_distance}}