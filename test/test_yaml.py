import unittest
from src.read_graph import read_yaml


class TestSimpleDag(unittest.TestCase):
    def test_read_yaml(self):
        data = read_yaml("data/simple_dag.yaml")
        self.assertEqual(data["users"]["test_user"]["name"], "Test")


if __name__ == "__main__":
    unittest.main()
