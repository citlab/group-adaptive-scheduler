import pytest
from cluster import *
from resource_manager import DummyRM
from stat_collector import DummyStatCollector


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

        cluster.nodes["N0"].add_task(apps[0].tasks[0])
        cluster.nodes["N0"].add_task(apps[0].tasks[1])
        cluster.nodes["N0"].add_task(apps[3].tasks[0])

        cluster.nodes["N1"].add_task(apps[0].tasks[2])
        cluster.nodes["N1"].add_task(apps[1].tasks[0])

        cluster.nodes["N2"].add_task(apps[1].tasks[1])
        cluster.nodes["N2"].add_task(apps[2].tasks[0])

        cluster.nodes["N3"].add_task(apps[1].tasks[2])
        cluster.nodes["N3"].add_task(apps[1].tasks[3])
        cluster.nodes["N3"].add_task(apps[0].tasks[3])
        cluster.nodes["N3"].add_task(apps[0].tasks[4])

        return cluster, apps

    def test_nodes_apps(self):
        cluster, apps = self.gen_cluster()

        expected_result = {
            "N0": [apps[0]],
            "N1": [apps[0], apps[1]],
            "N2": [apps[1], apps[2]],
            "N3": [apps[1], apps[0]],
        }

        result = cluster.nodes_apps()
        for address, applications in expected_result.items():
            assert set(result[address]) == set(applications)

    def test_apps_usage(self):
        cluster, apps = self.gen_cluster()

        nodes_apps = cluster.nodes_apps()
        mean_usage = cluster.stat_collector.mean_usage(cluster.nodes)

        expected_result = []
        for address in cluster.nodes.keys():
            expected_result.append(
                (nodes_apps[address], mean_usage[address].tolist())
            )

        for i, result in enumerate(cluster.apps_usage()):
            assert set(expected_result[i][0]) == set(result[0])
            assert expected_result[i][1] == result[1].tolist()




