import orjson
from dataclasses import dataclass
from enum import Enum


class TaskStatus(Enum):
    READY = 0
    FINISHED = 1
    BLOCKED = 2
    RUNNING = 3
    PREEMPTED = 4


@dataclass
class Task:
    required = ["label", "duration"]
    optional = {"cpus": 1, "ram": 1}
    status = None
    start = None
    end = None
    runtime = 0
    priority = None

    def __init__(self, name, props, status=None):
        """
        Build task
        """
        self.id = name
        self.validate(props)
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
    layout = None

    def __init__(self, dag, deserialize=False):
        self.nodes = []
        self.edges = []
        self.name = dag["name"]
        self.arrival_time = dag["arrival_time"]
        self.tasks = {}

        if deserialize:
            for node in dag["nodes"]:
                data = node["data"]
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
        task = Task(name, task)
        self.tasks[name] = task

        props = task.get_props()
        node = {"data": props}
        self.nodes.append(node)

        if "dependencies" in props and props["dependencies"]:
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
