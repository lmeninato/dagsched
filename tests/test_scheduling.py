import unittest
from src.scheduling import FCFS, PreemptivePriorityScheduler
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
        self.assertEqual(len(self.scheduler.history.times), 6)

        messages, dags, utilization = self.scheduler.history.get_events_at_time_t(0)
        self.assertEqual(utilization["cpus"], 12)
        self.assertEqual(len(list(dags.keys())), 2)
        self.assertTrue(len(messages))

        messages, dags, utilization = self.scheduler.history.get_events_at_time_t(16)
        self.assertEqual(utilization["cpus"], 0)
        self.assertEqual(len(list(dags.keys())), 2)
        self.assertTrue(len(messages))

    def test_scheduling_run(self):
        data = read_yaml("data/simple_dag.yml")
        users = list(data["users"].keys())
        scheduler = FCFS(data["cluster"], data["users"], users, deserialize=False)
        scheduler.run()
        self.assertEqual(scheduler.time, 16)


class TestPreemptivePriorityScheduler(unittest.TestCase):
    def setUp(self):
        data = read_yaml("data/simple_prio_dag.yml")
        users = list(data["users"].keys())
        self.scheduler = PreemptivePriorityScheduler(
            data["cluster"], data["users"], users, deserialize=False
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

    def test_scheduling_history(self):
        self.assertEqual(len(self.scheduler.history.times), 6)

        messages, dags, utilization = self.scheduler.history.get_events_at_time_t(0)
        self.assertEqual(utilization["cpus"], 12)
        self.assertEqual(len(list(dags.keys())), 2)
        self.assertTrue(len(messages))

        messages, dags, utilization = self.scheduler.history.get_events_at_time_t(16)
        self.assertEqual(utilization["cpus"], 0)
        self.assertEqual(len(list(dags.keys())), 2)
        self.assertTrue(len(messages))

    def test_scheduling_run(self):

        data = read_yaml("data/simple_prio_dag.yml")
        users = list(data["users"].keys())
        scheduler = PreemptivePriorityScheduler(
            data["cluster"], data["users"], users, deserialize=False
        )
        scheduler.run()
        self.assertEqual(scheduler.time, 79)


if __name__ == "__main__":
    unittest.main()
