from application import *
from resource_manager import DummyRM
from cluster import Node
import time
import pytest


class TestApplication:
    def test_tasks(self):
        app = DummyApplication("app", 8)

        assert len(app.tasks) == 8

    def test_start(self):
        app = DummyApplication("app", 8)
        app.id = 789456
        for task in app.tasks:
            task.node = Node("N", 8)
        rm = DummyRM(apps_running={app.id: True})
        a = []

        def on_finish(app: Application):
            a.append(app.name)

        app.start(rm, on_finish, 0.1)
        time.sleep(0.3)

        assert [app.name] == a
        assert app.is_running

    def test_not_correctly_scheduled(self):
        app = DummyApplication("app", 8)
        rm = DummyRM()

        with pytest.raises(NotCorrectlyScheduledError):
            app.start(rm)


class TestFlinkApplication:
    @staticmethod
    def gen_app():
        app = FlinkApplication("app", 8, "jar", ["arg1", "arg2"])

        nodes = []
        for i, task in enumerate(app.tasks):
            node = Node("N{}".format(i), 8)
            task.node = node
            nodes.append(node)

        return app, nodes

    def test_hosts(self):
        app, nodes = self.gen_app()
        expected_hosts = [n.address for n in nodes]

        assert expected_hosts == app.hosts()

    def test_command_line(self):
        app, _ = self.gen_app()

        expected_cmd = [
            "$FLINK_HOME/bin/flink",
            "run",
            "-m yarn-cluster",
            "-yn 8",
            "-yD fix.container.hosts=" + ",".join(app.hosts()),
            "jar",
            "arg1",
            "arg2"
        ]

        assert expected_cmd == app.command_line()

