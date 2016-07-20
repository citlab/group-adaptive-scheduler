from cluster import *
from stat_collector import DummyStatCollector
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


class TestCluster:
    @staticmethod
    def gen_cluster():
        stat_collector = DummyStatCollector(
            [
                ["A0T0", "A0T1", "A0T2"],
                ["A0T3", "A1T0"],
                ["A1T1"],
                ["A1T2", "A1T3", "A0T4", "A0T5"],
            ],
            lambda process_pattern, server: process_pattern
        )
        cluster = Cluster("Node{}", 4, 4, stat_collector)
        apps = [
            Application("app0", "cmd", 8),
            Application("app1", "cmd", 8)
        ]

        for i, app in enumerate(apps):
            for j, task in enumerate(app.tasks):
                task.container = "A{}T{}".format(i, j)

        cluster.nodes[0].add_task(apps[0].tasks[0])
        cluster.nodes[0].add_task(apps[0].tasks[1])
        cluster.nodes[0].add_task(apps[0].tasks[2])

        cluster.nodes[1].add_task(apps[0].tasks[3])
        cluster.nodes[1].add_task(apps[1].tasks[0])

        cluster.nodes[2].add_task(apps[1].tasks[1])

        cluster.nodes[3].add_task(apps[1].tasks[2])
        cluster.nodes[3].add_task(apps[1].tasks[3])
        cluster.nodes[3].add_task(apps[0].tasks[4])
        cluster.nodes[3].add_task(apps[0].tasks[5])

        return cluster, stat_collector, apps

    def test_apps_usage(self):
        cluster, stat_collector, apps = self.gen_cluster()

        nodes_apps = [[], [], [], []]
        nodes_apps[0].append(apps[0])
        nodes_apps[0].append(apps[0])
        nodes_apps[0].append(apps[0])

        nodes_apps[1].append(apps[0])
        nodes_apps[1].append(apps[1])

        nodes_apps[2].append(apps[1])

        nodes_apps[3].append(apps[1])
        nodes_apps[3].append(apps[1])
        nodes_apps[3].append(apps[0])
        nodes_apps[3].append(apps[0])

        mean_usage = stat_collector.mean_usage(cluster.nodes)

        expected_result = []
        for i in range(4):
            expected_result.append(
                (nodes_apps[i], mean_usage[i].tolist())
            )

        print(expected_result)

        result = []
        for r in cluster.apps_usage():
            result.append((r[0], r[1].tolist()))

        assert expected_result == result



