from cluster import *
import pytest


class TestApplication:
    def test_tasks(self):
        app = Application("app", "cmd", 8)

        assert len(app.tasks) == 8


class TestNode:
    def test_add_task(self):
        node = Node("test", 4)
        app1 = Application("app1", "cmd", 8)

        assert len(node.tasks) == 0

        node.add_task(app1.tasks[0])

        assert len(node.tasks) == 1

    def test_add_task_error(self):
        node, _ = self.gen_node(8)
        app1 = Application("app2", "cmd", 8)

        with pytest.raises(ValueError):
            node.add_task(app1.tasks[0])

    @staticmethod
    def gen_node(task_count=4):
        node = Node("test", 8)
        apps = [
            Application("app0", "cmd", 8),
            Application("app1", "cmd", 8)
        ]

        for i in range(task_count):
            node.add_task(apps[i % 2].tasks[i // 2])

        return node, apps

    def test_applications(self):
        node, apps = self.gen_node()

        assert node.applications == apps or node.applications == apps[::-1]

    def test_available_containers(self):
        node, _ = self.gen_node(6)

        assert node.available_containers == 2

    def test_pid_to_application(self):
        node, apps = self.gen_node()

        apps[0].tasks[0].pid = "45"

        assert node.pid_to_application("45") == apps[0]

    def test_pid_to_application_error(self):
        node, apps = self.gen_node()

        with pytest.raises(ValueError):
            node.pid_to_application("45") == apps[0]


