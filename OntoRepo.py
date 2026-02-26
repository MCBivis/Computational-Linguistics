from Repository import Neo4jRepository
from typing import List, Dict, Optional, Any


class OntoRepository:

    def __init__(self, uri: str, user: str, password: str):
        self.repo = Neo4jRepository(uri, user, password)

    def close(self) -> None:
        self.repo.close()

    def get_ontology(self) -> Dict[str, List[Dict[str, Any]]]:
        raw = self.repo.get_all_nodes_and_arcs()
        nodes: Dict[str, Dict[str, Any]] = {}
        arcs: List[Dict[str, Any]] = []

        for item in raw:
            n = item["from"]
            r = item["arc"]
            m = item["to"]

            if n is not None and n.get("uri") not in nodes:
                nodes[n.get("uri")] = self.repo.collect_node(n)
            if m is not None and m.get("uri") not in nodes:
                nodes[m.get("uri")] = self.repo.collect_node(m)
            if r is not None:
                arcs.append(self.repo.collect_arc(r))

        return {
            "nodes": list(nodes.values()),
            "arcs": arcs,
        }

    def get_ontology_parent_classes(self) -> List[Dict[str, Any]]:
        query = """
        MATCH (c:Class)
        WHERE NOT EXISTS( (c)-[:subClassOf]->(:Class) )
        RETURN c
        """
        with self.repo.driver.session() as session:
            result = session.run(query)
            return [self.repo.collect_node(rec["c"]) for rec in result]

    def get_class(self, class_uri: str) -> Optional[Dict[str, Any]]:
        node = self.repo.get_node_by_uri(class_uri)
        if node is None or "Class" not in node.labels:
            return None
        return self.repo.collect_node(node)

    def get_class_parents(self, class_uri: str) -> List[Dict[str, Any]]:
        query = """
        MATCH (child:Class {uri:$uri})-[:subClassOf]->(parent:Class)
        RETURN parent
        """
        with self.repo.driver.session() as session:
            result = session.run(query, uri=class_uri)
            return [self.repo.collect_node(r["parent"]) for r in result]

    def get_class_children(self, class_uri: str) -> List[Dict[str, Any]]:
        query = """
        MATCH (child:Class {uri:$uri})-[:subClassOf]->(parent:Class)
        RETURN child
        """
        with self.repo.driver.session() as session:
            result = session.run(query, uri=class_uri)
            return [self.repo.collect_node(r["child"]) for r in result]

    def get_class_objects(self, class_uri: str) -> List[Dict[str, Any]]:
        query = """
        MATCH (o:Object)-[:type]->(c:Class {uri:$uri})
        RETURN o
        """
        with self.repo.driver.session() as session:
            result = session.run(query, uri=class_uri)
            return [self.repo.collect_node(r["o"]) for r in result]

    def update_class(
        self,
        class_uri: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        payload: Dict[str, Any] = {}
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description
        if not payload:
            return self.get_class(class_uri)
        node = self.repo.update_node(class_uri, payload)
        return self.repo.collect_node(node) if node else None

    def create_class(
        self,
        name: str,
        description: str = "",
        parent_uri: Optional[str] = None,
    ) -> Dict[str, Any]:
        uri = self.repo.generate_random_string(namespace=name)
        params = {
            "label": "Class",
            "uri": uri,
            "name": name,
            "description": description,
        }
        node = self.repo.create_node(params)

        if parent_uri:
            self.repo.create_arc(uri, parent_uri, "subClassOf")

        return self.repo.collect_node(node)

    def delete_class(self, class_uri: str) -> None:
        children_query = """
        MATCH (sub:Class)-[:subClassOf]->(c:Class {uri:$uri})
        RETURN sub.uri AS uri
        """
        with self.repo.driver.session() as session:
            result = session.run(children_query, uri=class_uri)
            for record in result:
                child_uri = record["uri"]
                if child_uri:
                    self.delete_class(child_uri)

        delete_query = """
        MATCH (c:Class {uri:$uri})
        OPTIONAL MATCH (o:Object)-[:type]->(c)
        OPTIONAL MATCH (dp:DatatypeProperty)-[:domain]->(c)
        OPTIONAL MATCH (op:ObjectProperty)-[:domain]->(c)
        OPTIONAL MATCH (opr:ObjectProperty)-[:range]->(c)
        DETACH DELETE c, o, dp, op, opr
        """
        with self.repo.driver.session() as session:
            session.run(delete_query, uri=class_uri)

    def add_class_attribute(
        self,
        class_uri: str,
        attr_name: str,
    ) -> Dict[str, Any]:
        uri = self.repo.generate_random_string(namespace=attr_name)
        params = {
            "label": "DatatypeProperty",
            "uri": uri,
            "title": attr_name,
        }
        node = self.repo.create_node(params)
        self.repo.create_arc(uri, class_uri, "domain")
        return self.repo.collect_node(node)

    def delete_class_attribute(self, datatype_property_uri: str) -> None:
        self.repo.delete_node_by_uri(datatype_property_uri)

    def add_class_object_attribute(
        self,
        class_uri: str,
        attr_name: str,
        range_class_uri: str,
    ) -> Dict[str, Any]:
        uri = self.repo.generate_random_string(namespace=attr_name)
        params = {
            "label": "ObjectProperty",
            "uri": uri,
            "title": attr_name,
        }
        prop_node = self.repo.create_node(params)

        self.repo.create_arc(uri, class_uri, "domain")
        self.repo.create_arc(uri, range_class_uri, "range")

        return self.repo.collect_node(prop_node)

    def delete_class_object_attribute(self, object_property_uri: str) -> None:
        self.repo.delete_node_by_uri(object_property_uri)

    def add_class_parent(self, parent_uri: str, target_uri: str) -> None:
        self.repo.create_arc(target_uri, parent_uri, "subClassOf")

    def get_object(self, object_uri: str) -> Optional[Dict[str, Any]]:
        node = self.repo.get_node_by_uri(object_uri)
        if node is None or "Object" not in node.labels:
            return None
        return self.repo.collect_node(node)

    def delete_object(self, object_uri: str) -> None:
        self.repo.delete_node_by_uri(object_uri)

    def create_object(
        self,
        class_uri: str,
        properties: Dict[str, Any],
        relations: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if relations is None:
            relations = {}

        signature = self.collect_signature(class_uri)
        required_fields = {
            p.get("name")
            for p in signature.get("params", [])
            if p and p.get("name")
        }
        required_fields.add("title")
        required_fields.add("description")

        missing = [f for f in required_fields if f not in properties]
        if missing:
            raise ValueError(
                f"Missing required properties for class {class_uri}: "
                + ", ".join(missing)
            )

        extra = [name for name in properties.keys() if name not in required_fields]
        if extra:
            raise ValueError(
                "Extra properties not allowed for class "
                f"{class_uri}: " + ", ".join(extra)
            )

        allowed_relations = {
            p.get("name")
            for p in signature.get("obj_params", [])
            if p and p.get("name")
        }
        invalid_rels = [
            name for name in relations.keys() if name not in allowed_relations
        ]
        if invalid_rels:
            raise ValueError(
                "Relations not allowed for this class: " + ", ".join(invalid_rels)
            )

        uri = self.repo.generate_random_string(namespace=properties.get("title"))
        params = {
            "label": "Object",
            "uri": uri,
        }
        params.update(properties)
        node = self.repo.create_node(params)

        self.repo.create_arc(uri, class_uri, "type")

        for rel_name, targets in relations.items():
            if isinstance(targets, (list, tuple, set)):
                targets_iter = targets
            else:
                targets_iter = [targets]
            for target_uri in targets_iter:
                self.repo.create_arc(uri, target_uri, rel_name)

        return self.repo.collect_node(node)

    def update_object(
        self,
        object_uri: str,
        properties: Dict[str, Any],
        relations: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if relations is None:
            relations = {}

        with self.repo.driver.session() as session:
            cls_query = """
            MATCH (o:Object {uri:$uri})-[:type]->(c:Class)
            RETURN c.uri AS uri
            """
            rec = session.run(cls_query, uri=object_uri).single()
            if not rec:
                return None
            class_uri = rec["uri"]

        signature = self.collect_signature(class_uri)
        allowed_fields = {
            p.get("name")
            for p in signature.get("params", [])
            if p and p.get("name")
        }
        allowed_fields.add("title")
        allowed_fields.add("description")

        extra = [name for name in properties.keys() if name not in allowed_fields]
        if extra:
            raise ValueError(
                "Extra properties not allowed for class "
                f"{class_uri}: " + ", ".join(extra)
            )

        allowed_relations = {
            p.get("name")
            for p in signature.get("obj_params", [])
            if p and p.get("name")
        }
        invalid_rels = [
            name for name in relations.keys() if name not in allowed_relations
        ]
        if invalid_rels:
            raise ValueError(
                "Relations not allowed for this class: " + ", ".join(invalid_rels)
            )

        if not properties and not relations:
            return self.get_object(object_uri)

        node = self.repo.update_node(object_uri, properties)

        if relations:
            with self.repo.driver.session() as session:
                rel_types = list(relations.keys())
                delete_q = """
                MATCH (o:Object {uri:$uri})-[r]->()
                WHERE type(r) IN $types
                DELETE r
                """
                session.run(delete_q, uri=object_uri, types=rel_types)

            for rel_name, targets in relations.items():
                if isinstance(targets, (list, tuple, set)):
                    targets_iter = targets
                else:
                    targets_iter = [targets]
                for target_uri in targets_iter:
                    self.repo.create_arc(object_uri, target_uri, rel_name)

        return self.repo.collect_node(node) if node else None

    def collect_signature(self, class_uri: str) -> Dict[str, Any]:
        def uniq_by_uri(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            seen: set[str] = set()
            out: List[Dict[str, Any]] = []
            for it in items:
                uri = (it or {}).get("uri")
                if not uri or uri in seen:
                    continue
                seen.add(uri)
                out.append(it)
            return out

        def collect_recursive(uri: str, visited: set[str]) -> tuple[
            List[Dict[str, Any]],
            List[Dict[str, Any]],
        ]:
            if uri in visited:
                return [], []
            visited.add(uri)

            with self.repo.driver.session() as session:
                dt_query = """
                MATCH (dp:DatatypeProperty)-[:domain]->(c:Class {uri:$uri})
                RETURN dp
                """
                dt_result = session.run(dt_query, uri=uri)
                datatype_props_local = [
                    self.repo.collect_node(r["dp"]) for r in dt_result
                ]

                op_query = """
                MATCH (op:ObjectProperty)-[:domain]->(c:Class {uri:$uri})
                MATCH (op)-[:range]->(r:Class)
                RETURN op, r
                """
                op_result = session.run(op_query, uri=uri)
                object_props_local: List[Dict[str, Any]] = []
                for r in op_result:
                    op_node = r["op"]
                    range_node = r["r"]
                    op_data = self.repo.collect_node(op_node)
                    op_data["range_class"] = (
                        self.repo.collect_node(range_node)
                        if range_node is not None
                        else None
                    )
                    object_props_local.append(op_data)

                parent_query = """
                MATCH (c:Class {uri:$uri})-[:subClassOf]->(p:Class)
                RETURN p.uri AS uri
                """
                parents = [
                    rec["uri"]
                    for rec in session.run(parent_query, uri=uri)
                    if rec.get("uri")
                ]

            datatype_props_parents: List[Dict[str, Any]] = []
            object_props_parents: List[Dict[str, Any]] = []
            for p_uri in parents:
                dp, op = collect_recursive(p_uri, visited)
                datatype_props_parents.extend(dp)
                object_props_parents.extend(op)

            return (
                uniq_by_uri(datatype_props_local + datatype_props_parents),
                uniq_by_uri(object_props_local + object_props_parents),
            )

        datatype_props, object_props = collect_recursive(class_uri, set())
        return {"params": datatype_props, "obj_params": object_props}
