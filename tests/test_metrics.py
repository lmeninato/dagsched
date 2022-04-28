import unittest
from src.read_graph import read_yaml
from src.scheduling import FCFS


class TestSchedulingMetrics(unittest.TestCase):
    """
    pretty dumb tests for now, mostly just a sanity check
    """

    def setUp(self):
        data = read_yaml("data/simple_dag.yml")
        users = list(data["users"].keys())
        scheduler = FCFS(data["cluster"], data["users"], users, deserialize=False)
        scheduler.run()
        self.scheduler = scheduler
        self.metrics = scheduler.metrics

    def test_history(self):
        metrics = self.scheduler.history.get_metrics(-1)
        self.assertEqual(len(metrics.arrivals), 2)

    def test_makespan(self):
        ms = self.metrics.get_makespan()
        self.assertGreater(ms, 0)

        ms = self.metrics.get_local_makespan("test_user")
        self.assertGreater(ms, 0)

    def test_jct(self):
        ms = self.metrics.get_jct()
        self.assertGreater(ms, 0)

        ms = self.metrics.get_local_jct("test_user")
        self.assertGreater(ms, 0)

    def test_queuing_metrics(self):
        ms = self.metrics.get_queuing_time()
        self.assertGreaterEqual(ms, 0)

        ms = self.metrics.get_local_queuing_time("test_user")
        self.assertGreaterEqual(ms, 0)
