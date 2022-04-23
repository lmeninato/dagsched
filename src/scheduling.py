from dag import DAG, TaskStatus
from collections import deque
from copy import deepcopy

import logging


class SchedulerHistory:
    # time -> list of messages at time t
    messages = {}

    # time -> cluster resources used
    utilizations = {}

    # time -> user -> user DAG state at time t
    dags = {}

    # times stored
    times = set()

    def add_event(self, t, messages, dags, utilization):
        self.times.add(t)
        self.messages[t] = deepcopy(messages)
        self.dags[t] = deepcopy(dags)
        self.utilizations[t] = deepcopy(utilization)

    def get_events_at_time_t(self, t):
        if t not in self.times:
            raise KeyError(f"Time {t} not in scheduler history")

        return self.messages[t], self.dags[t], self.utilizations[t]


class Scheduler:
    """
    Base Scheduler Class
    """

    def __init__(self, cluster, dags, users, deserialize=True):
        self.cluster = cluster
        self.utilization = {"cpus": 0, "ram": 0}
        self.dags = {
            user: DAG(dag, deserialize=deserialize) for user, dag in dags.items()
        }
        self.users = users
        self.time = 0
        self.messages = []
        self.history = SchedulerHistory()
        self.running = {}

    def perform_scheduling_round():
        """
        virtual function, must be overriden
        """
        raise NotImplementedError()

    def get_history(self, t):
        return self.history.get_events_at_time_t(t)

    def store_history(self):
        self.history.add_event(self.time, self.messages, self.dags, self.utilization)

    def get_ready_tasks(self):
        tasks = []
        for user, dag in self.dags.items():
            if dag.arrival_time <= self.time:
                for label, task in dag.tasks.items():
                    if task.status and task.status in (
                        TaskStatus.READY,
                        TaskStatus.FINISHED,
                    ):
                        continue
                    if self.task_can_be_scheduled(dag, task):
                        logging.info(
                            f"Task (user: {user}, label: {label}, "
                            f"task: {task}) now READY"
                        )
                        task.status = TaskStatus.READY
                        tasks.append((user, label, task))
                    else:
                        task.status = TaskStatus.BLOCKED
        return tasks

    def cluster_can_shedule_task(self, task):
        if task.status == TaskStatus.FINISHED:
            return False
        if task.props["cpus"] + self.utilization["cpus"] > self.cluster["cpus"]:
            return False
        if task.props["ram"] + self.utilization["ram"] > self.cluster["ram"]:
            return False
        return True

    def task_can_be_scheduled(self, dag, task):
        """
        check if task name appears in dependency of any other task in the dag
        """

        if "dependencies" in task.props:
            for label, _task in dag.tasks.items():
                if label in task.props["dependencies"]:
                    if _task.status and _task.status != TaskStatus.FINISHED:
                        logging.info(f"Cannot run {task.id} until {_task.id} finishes")
                        return False

        return True

    def schedule_task(self, user, label, task):
        if not self.cluster_can_shedule_task(task):
            return False

        cpus, ram = task.props["cpus"], task.props["ram"]

        logging.info(f"Scheduled {user} task {label} with {cpus} cpus and {ram} ram")
        self.messages.append(
            f"Scheduled {user} task {label} with {cpus} cpus and {ram} ram"
        )

        task.status = TaskStatus.RUNNING
        task.start = self.time
        self.utilization["cpus"] += cpus
        self.utilization["ram"] += ram

        self.running[(user, label)] = task

        return True

    def remove_finished_tasks(self):
        # need to loop over a copy of the dict to mutate the original dynamically
        for key, task in self.running.copy().items():
            user, label = key
            if task.start + task.props["duration"] >= self.time:
                task.status = TaskStatus.FINISHED
                task.end = self.time
                logging.info(f"Finished {user} task {label} at {self.time}")
                self.messages.append(f"Finished {user} task {label} at {self.time}")
                self.utilization["cpus"] -= task.props["cpus"]
                self.utilization["ram"] -= task.props["ram"]
                del self.running[key]

    def next_time_task_finishes(self):
        next_time = float("inf")
        for _, task in self.running.items():
            duration = task.props["duration"]
            next_time = min(next_time, task.start + duration)
        return next_time

    def next_dag_arrival_time(self):
        next_time = float("inf")
        for _, dag in self.dags.items():
            if dag.arrival_time > self.time:
                next_time = min(next_time, dag.arrival_time)
        return next_time

    def set_next_event_time(self):
        """
        Next time something happens:
            - next event to finish
            - OR arrival of a DAG of tasks
        """
        next_event_time = self.next_time_task_finishes()
        next_arrival_time = self.next_dag_arrival_time()

        logging.debug(f"next event time is: {next_event_time}")
        logging.debug(f"next arrival time: {next_arrival_time}")

        next_time = min(next_event_time, next_arrival_time)

        # done scheduling -> signal that scheduling has completed
        if next_time == float("inf"):
            logging.info("No events remaining: scheduling finished or deadlock!")
            logging.info(f"Scheduling finished at time: {self.time}")
            return True

        # not done scheduling -> prepare time for next scheduling round
        logging.info(f"Increasing scheduler time from {self.time} to {next_time}")
        self.time = next_time
        return False


class FCFS(Scheduler):
    def __init__(self, cluster, dags, users, deserialize=True):
        super().__init__(cluster, dags, users, deserialize)
        self.ready = deque()

    def perform_scheduling_round(self):
        """
        FCFS Scheduling
            -> put all ready tasks in queue
            -> run tasks from queue until resources are exhausted
                -> produce event message as each task transitions from ready to running
            -> store state of dags and cluster
        """
        self.remove_finished_tasks()

        for user, label, task in self.get_ready_tasks():
            self.ready.appendleft((user, label, task))
            self.messages.append(f"Added {user} task {label} to ready queue")

        self.schedule_tasks()

        self.store_history()
        finished = self.set_next_event_time()
        return finished

    def get_ready_tasks(self):
        return super().get_ready_tasks()

    def remove_finished_tasks(self):
        super().remove_finished_tasks()

    def schedule_tasks(self):
        """
        We can either block until the head task can run or schedule
        tasks with smaller resource requirements immediately, but
        this would lead to starving out tasks with high resource
        requirements. For simplicity, we will just have blocking
        until the head task can be scheduled. The downside is this
        leads to head-of-line (HOL) blocking.
        """
        if not len(self.ready):
            return

        while len(self.ready):
            user, label, task = self.ready[-1]
            task_scheduled = super().schedule_task(user, label, task)
            if task_scheduled:
                self.ready.pop()
            else:
                break

    def store_history(self):
        return super().store_history()

    def set_next_event_time(self):
        return super().set_next_event_time()


if __name__ == "__main__":
    """
    for testing:
    python3 -it src/scheduling.py

    interactively play around with scheduling objects
    """

    from read_graph import read_yaml

    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S %p",
        level=logging.DEBUG,
    )

    data = read_yaml("data/simple_dag.yml")
    tasks = data["users"]["test_user"]
    dag = DAG(tasks)
    users = list(data["users"].keys())
    scheduler = FCFS(data["cluster"], data["users"], users, deserialize=False)
    finished = False
    while not finished:
        finished = scheduler.perform_scheduling_round()
