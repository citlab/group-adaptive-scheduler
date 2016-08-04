import pytest
from cluster import *
from application import DummyApplication
from resource_manager import DummyRM
from stat_collector import DummyStatCollector


class TestNode:
    def test_add_container(self):
        node = Node("test", 4)
        app1 = DummyApplication()

        assert len(node.containers) == 0

        node.add_container(app1.containers[0])

        assert len(node.containers) == 1

    def test_add_container_error(self):
        node, _ = self.gen_node(8)
        app1 = DummyApplication()

        with pytest.raises(ValueError):
            node.add_container(app1.containers[0])

    @staticmethod
    def gen_node(task_count=4):
        node = Node("test", 8)
        apps = [
            DummyApplication(id="A", name="App0", is_running=True),
            DummyApplication(id="B", name="App1", is_running=True),
            DummyApplication(id="C", name="App1", is_running=True)
        ]

        for i in range(task_count):
            node.add_container(apps[i % 3].containers[i // 3])

        return node, apps

    def test_applications(self):
        node, apps = self.gen_node()

        assert 3 == len(node.applications())
        assert 2 == len(node.applications(by_name=True))

    def test_available_containers(self):
        node, _ = self.gen_node(6)

        assert node.available_containers() == 2


class TestCluster:
    @staticmethod
    def gen_cluster():
        stat_collector = DummyStatCollector()
        rm = DummyRM(
            n_nodes=4,
            n_containers=4
        )

        return Cluster(rm, stat_collector)

    @staticmethod
    def gen_cluster_with_apps():
        cluster = TestCluster.gen_cluster()

        apps = [
            DummyApplication(name="app0", id="0", is_running=True),
            DummyApplication(name="app1", id="1", is_running=True),
            DummyApplication(name="app1", id="2", is_running=True),
            DummyApplication(name="app2", id="3", is_running=False),
        ]

        cluster.nodes["N0"].add_container(apps[0].containers[0])
        cluster.nodes["N0"].add_container(apps[0].containers[1])
        cluster.nodes["N0"].add_container(apps[3].containers[0])

        cluster.nodes["N1"].add_container(apps[0].containers[2])
        cluster.nodes["N1"].add_container(apps[1].containers[0])

        cluster.nodes["N2"].add_container(apps[1].containers[1])
        cluster.nodes["N2"].add_container(apps[2].containers[0])

        cluster.nodes["N3"].add_container(apps[1].containers[2])
        cluster.nodes["N3"].add_container(apps[1].containers[3])
        cluster.nodes["N3"].add_container(apps[0].containers[3])
        cluster.nodes["N3"].add_container(apps[0].containers[4])

        return cluster, apps

    def test_nodes_apps(self):
        cluster, apps = self.gen_cluster_with_apps()

        expected_result = {
            "N0": [apps[0]],
            "N1": [apps[0], apps[1]],
            "N2": [apps[1], apps[2]],
            "N3": [apps[1], apps[0]],
        }

        result = cluster.node_running_apps()
        for address, applications in expected_result.items():
            assert set(result[address]) == set(applications)

    def test_apps_usage(self):
        cluster, apps = self.gen_cluster_with_apps()

        nodes_apps = cluster.node_running_apps()

        expected_result = []
        for address in cluster.nodes.keys():
            expected_result.append(
                (nodes_apps[address], None)
            )

        for i, result in enumerate(cluster.apps_usage()):
            assert set(expected_result[i][0]) == set(result[0])
            assert isinstance(result[1], Usage)

    def test_applications_without_full_node(self):
        cluster, apps = self.gen_cluster_with_apps()

        expected_weights = {
            apps[0].name: 2,
            apps[1].name: 2,
        }

        applications, weights = cluster.applications(with_full_nodes=False, by_name=True)

        assert set([apps[0], apps[1]]) == set(applications) or set([apps[0], apps[2]]) == set(applications)
        for i, app in enumerate(applications):
            assert expected_weights[app.name] == weights[i]

    def test_applications_with_full_node(self):
        cluster, apps = self.gen_cluster_with_apps()

        expected_weights = {
            apps[0].name: 3,
            apps[1].name: 3,
            apps[3].name: 0,
        }

        applications, weights = cluster.applications(with_full_nodes=True, by_name=True)

        assert set([apps[0], apps[1]]) == set(applications) or set([apps[0], apps[2]]) == set(applications)
        for i, app in enumerate(applications):
            assert expected_weights[app.name] == weights[i]

    def test_available_containers(self):
        cluster, apps = self.gen_cluster_with_apps()
        cluster.nodes["N0"].add_container(apps[0])

        assert 4 * 4 - (11 + 1) == cluster.available_containers()

    def test_has_application_scheduled(self):
        cluster = self.gen_cluster()
        assert not cluster.has_application_scheduled()

        cluster, _ = self.gen_cluster_with_apps()
        assert cluster.has_application_scheduled()





