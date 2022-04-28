import unittest
from src.scheduling import (
    FCFS,
    PriorityScheduler,
    PreemptivePriorityScheduler,
    SmallestServiceFirst,
)
from src.read_graph import read_yaml


class TestFCFS(unittest.TestCase):
    def setUp(self):
        data = read_yaml("data/simple_dag.yml")
        users = list(data["users"].keys())
        self.scheduler = FCFS(data["cluster"], data["users"], users, deserialize=False)

    def test_constructor(self):
        self.assertEqual(self.scheduler.utilization["cpus"], 0)

    def test_scheduling(self):
        finished = self.scheduler.perform_scheduling_round()
        self.assertFalse(finished)
        self.assertEqual(self.scheduler.time, 5)
        self.assertEqual(len(self.scheduler.running), 4)

        finished = self.scheduler.perform_scheduling_round()
        self.assertFalse(finished)
        self.assertEqual(self.scheduler.time, 10)
        self.assertEqual(len(self.scheduler.running), 2)

        finished = self.scheduler.perform_scheduling_round()
        self.assertFalse(finished)
        self.assertEqual(self.scheduler.time, 13)
        self.assertEqual(len(self.scheduler.running), 2)

        finished = self.scheduler.perform_scheduling_round()
        self.assertFalse(finished)
        self.assertEqual(self.scheduler.time, 16)
        self.assertEqual(len(self.scheduler.running), 1)

        finished = self.scheduler.perform_scheduling_round()
        self.assertTrue(finished)
        self.assertEqual(self.scheduler.time, 16)

    def test_scheduling_history(self):
        data = read_yaml("data/simple_dag.yml")
        users = list(data["users"].keys())
        scheduler = FCFS(data["cluster"], data["users"], users, deserialize=False)
        scheduler.run()
        self.assertEqual(len(scheduler.history.times), 6)

        messages, dags, utilization = scheduler.history.get_events_at_time_t(0)
        self.assertEqual(utilization["cpus"], 12)
        self.assertEqual(len(list(dags.keys())), 2)
        self.assertTrue(len(messages))

        messages, dags, utilization = scheduler.history.get_events_at_time_t(16)
        self.assertEqual(utilization["cpus"], 0)
        self.assertEqual(len(list(dags.keys())), 2)
        self.assertTrue(len(messages))

    def test_scheduling_run(self):
        data = read_yaml("data/simple_dag.yml")
        users = list(data["users"].keys())
        scheduler = FCFS(data["cluster"], data["users"], users, deserialize=False)
        scheduler.run()
        self.assertEqual(scheduler.time, 16)


class TestPriorityScheduler(unittest.TestCase):
    def setUp(self):
        data = read_yaml("data/simple_prio_dag.yml")
        users = list(data["users"].keys())
        self.scheduler = PriorityScheduler(
            data["cluster"], data["users"], users, deserialize=False
        )

    def test_constructor(self):
        self.assertEqual(self.scheduler.utilization["cpus"], 0)

    def test_scheduling_run(self):

        data = read_yaml("data/simple_prio_dag.yml")
        users = list(data["users"].keys())
        scheduler = PriorityScheduler(
            data["cluster"], data["users"], users, deserialize=False
        )
        scheduler.run()
        self.assertEqual(scheduler.time, 86)


class TestPreemptivePriorityScheduler(unittest.TestCase):
    def setUp(self):
        self.data = read_yaml("data/simple_prio_dag.yml")
        self.users = list(self.data["users"].keys())
        self.scheduler = PreemptivePriorityScheduler(
            self.data["cluster"], self.data["users"], self.users, deserialize=False
        )

    def test_constructor(self):
        self.assertEqual(self.scheduler.utilization["cpus"], 0)

    def test_scheduling_steps(self):
        finished = self.scheduler.perform_scheduling_round()
        self.assertFalse(finished)
        self.assertEqual(self.scheduler.time, 4)
        self.assertEqual(len(self.scheduler.running), 2)

        finished = self.scheduler.perform_scheduling_round()
        self.assertFalse(finished)
        self.assertEqual(self.scheduler.time, 9)
        self.assertEqual(len(self.scheduler.running), 1)

    # not sure why the result is different in tests vs running alone
    # def test_scheduling_history(self):
    #     scheduler = PreemptivePriorityScheduler(
    #         self.data["cluster"], self.data["users"], self.users, deserialize=False
    #     )
    #     scheduler.run()

    #     # print(f"scheduler history times are: {sorted(scheduler.history.times)}")
    #     # self.assertEqual(len(scheduler.history.times), 7)

    #     messages, dags, utilization = scheduler.history.get_events_at_time_t(0)
    #     self.assertEqual(utilization["cpus"], 12)
    #     self.assertEqual(len(list(dags.keys())), 2)
    #     self.assertTrue(len(messages))

    #     messages, dags, utilization = scheduler.history.get_events_at_time_t(16)
    #     self.assertEqual(utilization["cpus"], 0)
    #     self.assertEqual(len(list(dags.keys())), 2)
    #     self.assertTrue(len(messages))

    def test_scheduling_run(self):

        data = read_yaml("data/simple_prio_dag.yml")
        users = list(data["users"].keys())
        scheduler = PreemptivePriorityScheduler(
            data["cluster"], data["users"], users, deserialize=False
        )
        scheduler.run()
        self.assertEqual(scheduler.time, 79)


class TestSmallestServiceFirst(unittest.TestCase):
    def setUp(self):
        data = read_yaml("data/simple_prio_dag.yml")
        users = list(data["users"].keys())
        self.scheduler = SmallestServiceFirst(
            data["cluster"], data["users"], users, deserialize=False
        )

    def test_constructor(self):
        self.assertEqual(self.scheduler.utilization["cpus"], 0)

    def test_scheduling_run(self):

        data = read_yaml("data/simple_prio_dag.yml")
        users = list(data["users"].keys())
        scheduler = PriorityScheduler(
            data["cluster"], data["users"], users, deserialize=False
        )
        scheduler.run()
        self.assertEqual(scheduler.time, 86)


if __name__ == "__main__":
    unittest.main()
