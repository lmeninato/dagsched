import unittest
from src.mlfq import MultiLevelFeedbackQueue


class TestMLFQ(unittest.TestCase):

    mlfq = MultiLevelFeedbackQueue()

    def test_constructor(self):
        self.assertEqual(self.mlfq.size, 0)

    def test_queue_works(self):
        self.mlfq.put("abc", 1)
        self.mlfq.put("bcd", 2)

        self.assertEqual(self.mlfq.size, 2)

        item = self.mlfq.get()

        self.assertEqual(self.mlfq.size, 1)
        self.assertEqual(item, "bcd")

        item = self.mlfq.get()

        self.assertEqual(self.mlfq.size, 0)
        self.assertEqual(item, "abc")

    def test_throw_underflow_error(self):
        self.assertEqual(self.mlfq.size, 0)
        self.assertRaises(ValueError, self.mlfq.get)

    def test_peek(self):
        self.assertEqual(self.mlfq.size, 0)
        self.assertRaises(ValueError, self.mlfq.peek)

        self.mlfq.put("abc", 1)
        self.mlfq.put("bcd", 2)

        item = self.mlfq.peek()

        self.assertEqual(item, "bcd")
        self.assertEqual(self.mlfq.size, 2)

        self.mlfq.get()
        self.mlfq.get()

        self.assertEqual(self.mlfq.size, 0)
