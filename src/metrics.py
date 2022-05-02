from statistics import mean
from collections import defaultdict


class SchedulingMetrics:
    def __init__(self, dags):
        # for each user, store the arrival time
        self.arrivals = {}

        # for each user, for each job, track number of preemptions
        self.preemptions = defaultdict(dict)

        # for each user, for each job, track job duration
        self.job_completion_time = defaultdict(dict)

        # for each user, for each job, track job queuing time
        self.job_queue_time = defaultdict(dict)

        for user, dag in dags.items():
            self.arrivals[user] = dag.arrival_time
            for task_id in dag.tasks.keys():
                self.preemptions[user][task_id] = 0
                self.job_completion_time[user][task_id] = {}
                self.job_queue_time[user][task_id] = 0

    def store_preemption(self, user, task):
        self.preemptions[user][task.id] += 1

    def get_makespan(self, func=mean):
        """
        get duration to compelete dags per user

        can be used to get min, max, average, etc. makespan
        or return all makespans with the identity function
        """

        makespans = [
            self.get_local_makespan(user) for user in self.job_completion_time.keys()
        ]

        return func(makespans)

    def get_local_makespan(self, user):
        """
        get makespan per user (time to complete dag)
        """
        arrival_time = self.arrivals[user]
        max_finish_time = -float("inf")

        for _, times in self.job_completion_time[user].items():
            max_finish_time = max(max_finish_time, times["end"])

        return max_finish_time - arrival_time

    def store_task_finish_time(self, user, task):
        self.job_completion_time[user][task.id] = {"start": task.start, "end": task.end}

    def get_jct(self, local_func=mean, global_func=mean):
        """
        get global task completion time
        can pass in custom functions to determine what statistic is desired:
        e.g.

        max of minimum jcts:

        self.get_jct(min, max)
        """
        jct = [
            self.get_local_jct(user, local_func)
            for user in self.job_completion_time.keys()
        ]

        return global_func(jct)

    def get_local_jct(self, user, func=mean):
        """
        get how long tasks takes to complete on average for user
        """
        times = []

        for _, t in self.job_completion_time[user].items():
            times.append(t["end"] - t["start"])

        return func(times)

    def store_task_queue_time(self, user, task, time):
        queue_time = time - task.ready_time
        self.job_queue_time[user][task.id] += queue_time

    def get_queuing_time(self, local_func=mean, global_func=mean):
        """
        get how long jobs wait in ready queue
        """
        queue_times = [
            self.get_local_queuing_time(user, local_func)
            for user in self.job_queue_time.keys()
        ]

        return global_func(queue_times)

    def get_local_queuing_time(self, user, local_func=mean):
        """
        get how long tasks are queued by user
        """
        queue_times = []

        for _, queue_time in self.job_queue_time[user].items():
            queue_times.append(queue_time)

        return local_func(queue_times)
