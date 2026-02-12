from neo4j import GraphDatabase

class Neo4jRepository:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(
            uri,
            auth=(user, password)
        )

    def close(self):
        self.driver.close()

    def get_all_nodes_and_arcs(self):
        query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        RETURN n, r, m
        """
        with self.driver.session() as session:
            result = session.run(query)
            data = []
            for record in result:
                data.append({
                    "from": record["n"],
                    "arc": record["r"],
                    "to": record["m"]
                })
            return data

    def get_nodes_by_labels(self, labels):
        query = """
        MATCH (n)
        WHERE any(l IN labels(n) WHERE l IN $labels)
        RETURN n
        """
        with self.driver.session() as session:
            result = session.run(query, labels=labels)
            nodes = []
            for record in result:
                nodes.append(record["n"])
            return nodes

    def get_node_by_uri(self, uri):
        query = """
        MATCH (n {uri:$uri})
        RETURN n
        """
        with self.driver.session() as session:
            result = session.run(query, uri=uri)
            record = result.single()
            if record:
                return record["n"]
            return None

    def create_node(self, params: dict):
        if "label" not in params:
            raise ValueError("params must contain 'label'")
        label = params["label"]
        properties = params.copy()
        properties.pop("label")
        query = f"""
        CREATE (n:{label})
        SET n += $props
        RETURN n
        """
        with self.driver.session() as session:
            result = session.run(query, props=properties)
            record = result.single()
            return record["n"]

    def create_arc(self, node1_uri: str, node2_uri: str, arc_type: str):
        if not arc_type.isidentifier():
            raise ValueError("Invalid arc type")
        query = f"""
        MATCH (a {{uri:$uri1}}), (b {{uri:$uri2}})
        CREATE (a)-[r:{arc_type}]->(b)
        RETURN r
        """
        with self.driver.session() as session:
            result = session.run(
                query,
                uri1=node1_uri,
                uri2=node2_uri
            )
            record = result.single()
            return record["r"]

    def delete_node_by_uri(self, uri: str):
        query = """
        MATCH (n {uri:$uri})
        DETACH DELETE n
        """
        with self.driver.session() as session:
            session.run(query, uri=uri)

    def delete_arc_by_id(self, arc_id: int):
        query = """
        MATCH ()-[r]->()
        WHERE elementId(r) = $id
        DELETE r
        """
        with self.driver.session() as session:
            session.run(query, id=arc_id)