import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
from networkx.readwrite.json_graph.cytoscape import cytoscape_data


class Task:
    def __init__(self):
        """ """
        pass


class DAG:
    layout = None

    def __init__(self, dag):
        self.graph = nx.DiGraph()
        self.name = dag["name"]

        for name, task in dag["tasks"].items():
            self.graph.add_node(name, **task)
            if "dependencies" in task:
                for dependency in task["dependencies"]:
                    self.graph.add_edge(dependency, name, color="black")

        self.layout = graphviz_layout(self.graph)

    def to_cyto_node(self, node):
        id = node["data"]["id"]
        x, y = self.layout[id]
        node["data"]["x"] = x
        node["data"]["y"] = y
        return node

    def render_state(self):
        """
        Given events that have taken place, render current graph
        """
        cyto = cytoscape_data(self.graph)
        nodes = cyto["elements"]["nodes"]
        edges = cyto["elements"]["edges"]

        nodes = [self.to_cyto_node(node) for node in nodes]

        return nodes + edges


if __name__ == "__main__":
    from read_graph import read_yaml

    data = read_yaml("data/simple_dag.yaml")
    tasks = data["users"]["test_user"]
    dag = DAG(tasks)
