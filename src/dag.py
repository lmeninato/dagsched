import orjson
from dataclasses import dataclass
from enum import Enum


class TaskStatus(Enum):
    READY = 1
    FINISHED = 2
    BLOCKED = 3
    RUNNING = 4
    PREEMPTED = 5


@dataclass
class Task:
    """
    Needs to be serializable to JSON to be useable in dash and cytoscape

    status is tracked by TaskStatus enum
    properties that are available in the front-end must be stored in the props dict
    """

    required = ["label", "duration"]
    optional = {"cpus": 1, "ram": 1}
    status = None
    # used to track when a task started to be ready
    ready_time = None
    start = None
    end = None
    runtime = 0
    # used for preemption to know how long a task ran for
    prev_runtime = None
    priority = None

    def __init__(self, name, props, status=None):
        """
        Build task
        """
        self.id = name
        self.validate(props)

        # used for visible properties in cytoscape
        self.props = self.add_defaults(props)
        self.status = status

    def validate(self, props):
        for req in self.required:
            if req not in props:
                raise ValueError(f"Missing {req} in task definition")

    def add_defaults(self, props):
        for opt, val in self.optional.items():
            if opt not in props:
                props[opt] = val
        props["id"] = self.id

        if self.status:
            props["status"] = self.status
        if "priority" in props:
            self.priority = props["priority"]
        return props

    def get_props(self):
        if self.status and isinstance(self.status, TaskStatus):
            self.props["status"] = self.status.name
        return self.props

    def toJSON(self):
        return orjson.dumps(self)


@dataclass
class DAG:
    """
    Must be JSON serializable to be accessible in Dash state

    Stores metadata about DAG (e.g. name of user, arrival time, nodes and edges)

    render_state method returns nodes and edges in cytoscape js format
    """

    layout = None

    def __init__(self, dag, deserialize=False):
        self.nodes = []
        self.edges = []
        self.name = dag["name"]
        self.arrival_time = dag["arrival_time"]
        self.tasks = {}
        # add compound parent nodes
        self.nodes.append(
            {
                "data": {"id": self.name, "label": self.name},
                "classes": "parent",
            }
        )

        if deserialize:
            # if the task came from Dash, we need to
            # deserialize it, hence this ugly mess
            for node in dag["nodes"]:
                data = node["data"]
                if "classes" in node and node["classes"] == "parent":
                    # skip parent nodes to construct compound nodes
                    continue
                name = data["id"]
                if name in dag["tasks"]:
                    status = dag["tasks"][name]["status"]
                    if status:
                        status = TaskStatus(status).name
                    data["status"] = status
                self.add_task(name, data)
            return
        for name, task in dag["tasks"].items():
            self.add_task(name, task)

    def add_task(self, name, task):
        if self.name not in name:
            name = f"{self.name},{name}"
        task = Task(name, task)
        self.tasks[name] = task

        props = task.get_props()
        props["parent"] = self.name
        node = {"data": props}
        self.nodes.append(node)

        if "dependencies" in props and props["dependencies"]:
            for dependency in props["dependencies"]:
                if self.name not in dependency:
                    dependency = f"{self.name},{dependency}"
                edge = {"data": {"source": dependency, "target": name}}
                self.edges.append(edge)

    def render_state(self):
        """
        Given events that have taken place, render current graph
        """

        return self.nodes + self.edges

    def toJSON(self):
        # needed because of dash limitation where it requires
        # classes to be json serializable to be stored in a client-side dcc.Store
        return orjson.dumps(self)


if __name__ == "__main__":
    from read_graph import read_yaml

    data = read_yaml("data/simple_dag.yml")
    tasks = data["users"]["test_user"]
    dag = DAG(tasks)
