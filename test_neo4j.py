from Repository import Neo4jRepository

repo = Neo4jRepository(
    "bolt://localhost:7687",
    "neo4j",
    "12345678"
)


print("=== All nodes and arcs ===")
print(repo.get_all_nodes_and_arcs())

print("=== Persons and Cities ===")
print(repo.get_nodes_by_labels(["Person", "City"]))

print("=== Node by uri ===")
print(repo.get_node_by_uri("1"))

print("=== Create and Delete ===")
repo.create_node({
    "label": "Person",
    "uri": "100",
    "name": "Ivan"
})
repo.create_node({
    "label": "City",
    "uri": "200",
    "name": "Moscow"
})
arc = repo.create_arc("100", "200", "LIVED")
print("Arc id:", arc.element_id)
print(repo.get_all_nodes_and_arcs())
repo.delete_arc_by_id(arc.element_id)
repo.delete_node_by_uri("100")
repo.delete_node_by_uri("200")
print(repo.get_all_nodes_and_arcs())

repo.close()