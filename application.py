import subprocess
import time
import typing
from threading import Thread
from complementarity import Job
from resource_manager import ResourceManager


class NotCorrectlyScheduledError(Exception):
    pass


class Application(Job):
    def __init__(self, name, n_tasks):
        super().__init__(name)
        self.id = None
        self.is_running = False
        self.tasks = [Task(self) for i in range(n_tasks)]
        self.thread = None

    def __str__(self):
        return "{} : {}".format(self.name, self.id)

    def start(self, resource_manager: ResourceManager, on_finish=None, sleep_during_loop=2):
        for task in self.tasks:
            if task.node is None:
                raise NotCorrectlyScheduledError(
                    "A task of the application {} is not scheduled on a node".format(self.name)
                )

        self.thread = Thread(target=self._run, args=[resource_manager, on_finish, sleep_during_loop])
        self.thread.start()

    def command_line(self) -> typing.List[str]:
        return [""]

    def _run(self, resource_manager: ResourceManager, on_finish, sleep_during_loop):
        process = subprocess.Popen(self.command_line())

        while process.poll() is None:
            if not self.is_running and resource_manager.is_application_running(self.id):
                self.is_running = True

            time.sleep(sleep_during_loop)

        if callable(on_finish):
            on_finish(self)


class Task:
    def __init__(self, application: Application):
        self.application = application
        self.container = None
        self.node = None
        self.pid = None


class DummyApplication(Application):
    def command_line(self):
        return ["sleep", "0.1"]


class FlinkApplication(Application):
    def __init__(self, name, n_task, jar, args):
        super().__init__(name, n_task)
        self.jar = jar
        self.args = args

    def command_line(self):
        cmd = [
            "$FLINK_HOME/bin/flink",
            "run",
            "-m yarn-cluster",
            "-yn {}".format(len(self.tasks)),
            "-yD fix.container.hosts=" + ",".join(self.hosts()),
            self.jar
        ]
        return cmd + self.args

    def hosts(self):
        hosts = []
        for task in self.tasks:
            hosts.append(task.node.address)

        return hosts
