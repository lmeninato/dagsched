from collections import defaultdict, deque


class MultiLevelFeedbackQueue:
    def __init__(self) -> None:
        self.levels = defaultdict(deque)
        self.size = 0

    def put(self, item, priority):
        self.size += 1
        self.levels[priority].appendleft(item)

    def get(self):
        if not self.size:
            raise ValueError("Size of MLFQ is 0")

        max_prio = max(self.levels.keys())
        result = self.levels[max_prio].pop()
        self.size -= 1

        if not len(self.levels[max_prio]):
            del self.levels[max_prio]
        return result

    def peek(self):
        if not self.size:
            raise ValueError("Size of MLFQ is 0")

        max_prio = max(self.levels.keys())
        result = self.levels[max_prio][-1]  # grab last element
        return result