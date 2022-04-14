import orjson
from dataclasses import dataclass


@dataclass
class Task:
    required = ["label", "duration"]
    optional = {"cpus": 1, "ram": 1}

    def __init__(self, name, props):
        """
        Build task
        """
        self.id = name
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
        props["id"] = self.id
        return props

    def get_props(self):
        return self.props

    def toJSON(self):
        return orjson.dumps(self)


@dataclass
class DAG:
    layout = None

    def __init__(self, dag, deserialize=False):
        self.nodes = []
        self.edges = []
        self.name = dag["name"]
        self.arrival_time = dag["arrival_time"]

        if deserialize:
            self.nodes = dag["nodes"]
            self.edges = dag["edges"]
            return
        for name, task in dag["tasks"].items():
            self.add_task(name, task)

    def add_task(self, name, task):
        task = Task(name, task)
        props = task.get_props()
        node = {"data": props}
        self.nodes.append(node)

        if "dependencies" in props:
            for dependency in props["dependencies"]:
                edge = {"data": {"source": dependency, "target": name}}
                self.edges.append(edge)

    def render_state(self):
        """
        Given events that have taken place, render current graph
        """
        return self.nodes + self.edges

    def toJSON(self):
        return orjson.dumps(self)


if __name__ == "__main__":
    from read_graph import read_yaml

    data = read_yaml("data/simple_dag.yml")
    tasks = data["users"]["test_user"]
    dag = DAG(tasks)
