from application import *
from resource_manager import DummyRM
from cluster import Node
import time
import pytest


class TestApplication:
    def test_tasks(self):
        app = DummyApplication(n_tasks=8)

        assert len(app.tasks) == 8

    def test_start(self):
        app = DummyApplication()
        for container in app.containers:
            container.node = Node("N", 8)
        rm = DummyRM(apps_running={"A1": True})
        a = []

        def on_finish(app: Application):
            a.append(app.name)

        app.start(rm, on_finish, 0.1)
        time.sleep(0.2)
        rm.apps_finished["A1"] = True
        time.sleep(0.4)

        assert [app.name] == a
        assert app.is_running

    def test_not_correctly_scheduled(self):
        app = DummyApplication()
        rm = DummyRM()

        with pytest.raises(NotCorrectlyScheduledError):
            app.start(rm)

    def test_is_a_copy_of(self):
        app = DummyApplication(id=123, is_running=False)
        app1 = DummyApplication(id=456)
        app2 = DummyApplication(is_running=True)
        app3 = DummyApplication(name="app3")
        app4 = DummyApplication(n_tasks=7)
        app5 = DummyApplication(data_set='2')

        assert app1.is_a_copy_of(app)
        assert app2.is_a_copy_of(app)
        assert not app3.is_a_copy_of(app)
        assert not app4.is_a_copy_of(app)
        assert not app5.is_a_copy_of(app)

    def test_copy(self):
        app = DummyApplication()
        c_app = app.copy()

        assert c_app.is_a_copy_of(app)


class TestFlinkApplication:
    @staticmethod
    def gen_app():
        app = SparkApplication("app", 8, "jar", ["arg1", "TEMP"], tm=1536, jar_class="JarClassK")
        app.id = "flink"

        nodes = []
        for i, container in enumerate(app.containers):
            node = Node("N{}".format(i), 8)
            container.node = node
            nodes.append(node)

        app.node = Node("N_APP_M", 8)

        return app, nodes

    def test_hosts(self):
        app, nodes = self.gen_app()
        expected_hosts = [n.address for n in nodes]

        assert expected_hosts == app.tasks_hosts()

    def test_command_line(self):
        app, _ = self.gen_app()

        expected_cmd = [
            "$FLINK_HOME/bin/flink",
            "run",
            "-m yarn-cluster",
            "-ynm {}_{}".format(app.name, app.data_set),
            "-yn 8",
            "-yD fix.container.hosts=" + ",".join(app.tasks_hosts()),
            "-ytm 1536",
            "-c JarClassK",
            "jar",
            "arg1",
            "TEMP",
            "1> apps_log/{}.log".format(app.id)
        ]
        cmd = app.command_line()
        for i in range(10):
            assert expected_cmd[i] == cmd[i]

        assert "hdfs:///tmp/" in cmd[10]
        assert expected_cmd[11] in cmd[11]

    def test_is_a_copy_of(self):
        app = SparkApplication("app", 8, jar="jar", args=["arg1"])
        app1 = SparkApplication("app", 8, jar="jar", args=["arg1"])
        app2 = SparkApplication("app", 8, jar="jar2", args=["arg1"])
        app3 = SparkApplication("app", 8, jar="jar", args=["arg3"])

        assert app1.is_a_copy_of(app)
        assert not app2.is_a_copy_of(app)
        assert not app3.is_a_copy_of(app)

    def test_copy(self):
        app = SparkApplication("app", 8, jar="jar", args=["arg1"])
        c_app = app.copy()

        assert c_app.is_a_copy_of(app)

