import pytest
from cluster import *
from stat_collector import DummyStatCollector
from resource_manager import DummyRM


class TestApplication:
    def test_tasks(self):
        app = Application("app", 8)

        assert len(app.tasks) == 8


class TestNode:
    def test_add_task(self):
        node = Node("test", 4)
        app1 = Application("app1", 8)

        assert len(node.tasks) == 0

        node.add_task(app1.tasks[0])

        assert len(node.tasks) == 1

    def test_add_task_error(self):
        node, _ = self.gen_node(8)
        app1 = Application("app2", 8)

        with pytest.raises(ValueError):
            node.add_task(app1.tasks[0])

    @staticmethod
    def gen_node(task_count=4):
        node = Node("test", 8)
        apps = [
            Application("app0", 8),
            Application("app1", 8)
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


class TestCluster:
    @staticmethod
    def gen_cluster():
        stat_collector = DummyStatCollector()
        rm = DummyRM(
            n_nodes=4,
            n_containers=4
        )

        cluster = Cluster(rm, stat_collector)

        apps = [
            Application("app0", 8),
            Application("app1", 8),
            Application("app1", 8),
            Application("app2", 8),
        ]

        for i, app in enumerate(apps):
            app.id = i

        apps[0].is_running = True
        apps[1].is_running = True
        apps[2].is_running = True
        apps[3].is_running = False

        cluster.nodes[0].add_task(apps[0].tasks[0])
        cluster.nodes[0].add_task(apps[0].tasks[1])
        cluster.nodes[0].add_task(apps[3].tasks[0])

        cluster.nodes[1].add_task(apps[0].tasks[2])
        cluster.nodes[1].add_task(apps[1].tasks[0])

        cluster.nodes[2].add_task(apps[1].tasks[1])
        cluster.nodes[2].add_task(apps[2].tasks[0])

        cluster.nodes[3].add_task(apps[1].tasks[2])
        cluster.nodes[3].add_task(apps[1].tasks[3])
        cluster.nodes[3].add_task(apps[0].tasks[3])
        cluster.nodes[3].add_task(apps[0].tasks[4])

        return cluster, apps

    def test_apps_usage(self):
        cluster, apps = self.gen_cluster()

        nodes_apps = [[], [], [], []]
        nodes_apps[0].append(apps[0])

        nodes_apps[1].append(apps[0])
        nodes_apps[1].append(apps[1])

        nodes_apps[2].append(apps[1])
        nodes_apps[2].append(apps[2])

        nodes_apps[3].append(apps[1])
        nodes_apps[3].append(apps[0])

        mean_usage = cluster.stat_collector.mean_usage(cluster.nodes)

        expected_result = []
        for i in range(4):
            expected_result.append(
                (nodes_apps[i], mean_usage[i].tolist())
            )

        for i, result in enumerate(cluster.apps_usage()):
            assert expected_result[i][1] == result[1].tolist()
            assert len(expected_result[i][0]) == len(result[0])
            for app in expected_result[i][0]:
                assert result[0].index(app) >= 0




