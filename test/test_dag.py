import unittest
from src.dag import DAG
from src.read_graph import read_yaml


class TestSimpleDag(unittest.TestCase):

    data = read_yaml("data/simple_dag.yaml")
    tasks = data["users"]["test_user"]
    dag = DAG(tasks)

    def test_dag_constructor(self):
        self.assertEqual(self.dag.name, "Test")

    def test_dag_nodes(self):
        self.assertEqual(len(self.dag.graph.nodes()), 4)

    def test_dag_edges(self):
        self.assertEqual(len(self.dag.graph.edges()), 3)


if __name__ == "__main__":
    unittest.main()
