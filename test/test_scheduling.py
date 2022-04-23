import unittest
from src.scheduling import FCFS
from src.read_graph import read_yaml

data = read_yaml("data/simple_dag.yml")
users = list(data["users"].keys())


class TestFCFS(unittest.TestCase):

    scheduler = FCFS(data["cluster"], data["users"], users, deserialize=False)

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
        self.scheduler = FCFS(data["cluster"], data["users"], users, deserialize=False)
        self.scheduler.run()
        self.assertEqual(self.scheduler.time, 16)


if __name__ == "__main__":
    unittest.main()
