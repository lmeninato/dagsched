import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout
from networkx.readwrite.json_graph.cytoscape import cytoscape_data


class Task:
    required = ["label", "duration"]
    optional = {"cpus": 1, "ram": 1}

    def __init__(self, props):
        """
        Build task
        """
        self.validate(props)
        self.props = self.add_defaults(props)

    def validate(self, props):
        for req in self.required:
            if req not in props:
                raise ValueError(f"Missing {req} in task definition")

    def add_defaults(self, props):
        for opt, val in self.optional.items():
            if opt not in props:
                props[opt] = val
        return props

    def get_props(self):
        return self.props


class DAG:
    layout = None

    def __init__(self, dag):
        self.graph = nx.DiGraph()
        self.name = dag["name"]

        for name, task in dag["tasks"].items():
            task = Task(task)
            task_props = task.get_props()
            self.graph.add_node(name, **task_props)
            if "dependencies" in task_props:
                for dependency in task_props["dependencies"]:
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
