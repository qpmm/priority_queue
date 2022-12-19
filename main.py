from dataclasses import dataclass, astuple
from bisect import insort_right
from operator import attrgetter, le
from typing import Optional, List
from unittest import TestCase, main as run_tests


@dataclass
class Resources:
    ram: int
    cpu_cores: int
    gpu_count: int

    def __lt__(self, other):
        return sum(astuple(self)) < sum(astuple(other))

    def satisfy(self, available_resources: 'Resources') -> bool:
        return all(map(le, astuple(self), astuple(available_resources)))


@dataclass
class Task:
    id: int
    priority: int
    resources: Resources
    content: str
    result: str

    def __lt__(self, other):
        return (self.priority, other.resources) < (other.priority, self.resources)


class TaskQueue:
    def __init__(self):
        # Due to comparison methods provided by `Task` and `Resources` classes,
        # tasks will be automatically sorted by priority (asc) and resources (desc),
        # so that the most critical and the least resources consuming task is placed in the end of the `list`.
        # This way searching and removal operations will be more efficient.
        # Given that the queue is expected to contain only thousands of tasks, `list` seems to be an optimal choice.
        self.queue: List[Task] = []

    def add_task(self, task: Task) -> None:
        insort_right(self.queue, task)

    def get_task(self, available_resources: Resources) -> Optional[Task]:
        if not self.queue:
            return None

        indices = range(len(self.queue))
        for i, task in zip(reversed(indices), reversed(self.queue)):
            if task.resources.satisfy(available_resources):
                return self.queue.pop(i)

        return None


class TaskQueueTest(TestCase):
    def setUp(self):
        self.task_queue = TaskQueue()

    def add_tasks(self) -> None:
        tasks = [
            Task(1, 1, Resources(3, 1, 2), '', ''),
            Task(2, 4, Resources(1, 1, 5), '', ''),
            Task(3, 6, Resources(2, 8, 0), '', ''),
            Task(4, 6, Resources(8, 1, 0), '', ''),
            Task(5, 7, Resources(2, 2, 2), '', ''),
            Task(6, 2, Resources(2, 1, 2), '', ''),
            Task(7, 7, Resources(6, 6, 6), '', ''),
            Task(8, 7, Resources(4, 4, 4), '', '')
        ]

        for task in tasks:
            self.task_queue.add_task(task)

    def test_queue_sorting(self):
        self.add_tasks()

        # Expected order of tasks:
        # Task(1, 1, Resources(3, 1, 2), '', '')
        # Task(6, 2, Resources(2, 1, 2), '', '')
        # Task(2, 4, Resources(1, 1, 5), '', '')
        # Task(3, 6, Resources(2, 8, 0), '', '')
        # Task(4, 6, Resources(8, 1, 0), '', '')
        # Task(7, 7, Resources(6, 6, 6), '', '')
        # Task(8, 7, Resources(4, 4, 4), '', '')
        # Task(5, 7, Resources(2, 2, 2), '', '')

        sorted_tasks_ids = list(map(attrgetter('id'), self.task_queue.queue))
        self.assertEqual(sorted_tasks_ids, [1, 6, 2, 3, 4, 7, 8, 5])

    def test_get_task_from_empty_queue(self):
        task = self.task_queue.get_task(Resources(0, 0, 0))
        self.assertIsNone(task)

    def test_get_task(self):
        self.add_tasks()

        task = self.task_queue.get_task(Resources(-1, -1, -1))
        self.assertIsNone(task)

        task = self.task_queue.get_task(Resources(1, 1, 10))
        self.assertEqual(task.id, 2)

        task = self.task_queue.get_task(Resources(3, 3, 3))
        self.assertEqual(task.id, 5)

        task = self.task_queue.get_task(Resources(3, 3, 3))
        self.assertEqual(task.id, 6)


if __name__ == '__main__':
    run_tests()
