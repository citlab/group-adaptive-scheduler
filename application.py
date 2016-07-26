import subprocess
import time
from typing import List
from threading import Thread
from resource_manager import ResourceManager


class NotCorrectlyScheduledError(Exception):
    pass


class Application:
    def __init__(self, name, n_tasks):
        self.name = name
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

    def command_line(self) -> List[str]:
        return [""]

    def _run(self, resource_manager: ResourceManager, on_finish, sleep_during_loop):
        process = subprocess.Popen(self.command_line())

        while process.poll() is None:
            if not self.is_running and resource_manager.is_application_running(self.id):
                self.is_running = True

            time.sleep(sleep_during_loop)

        if callable(on_finish):
            on_finish(self)

    def copy(self):
        return Application(self.name, len(self.tasks))

    def is_a_copy_of(self, application):
        return application.name == self.name and len(self.tasks) == len(application.tasks)


class Task:
    def __init__(self, application: Application):
        self.application = application
        self.container = None
        self.node = None
        self.pid = None


class DummyApplication(Application):
    def __init__(self, name="app", n_tasks=8, app_id="id", is_running=False):
        super().__init__(name, n_tasks)
        self.id = app_id
        self.is_running = is_running

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

    def copy(self):
        return FlinkApplication(self.name, len(self.tasks), self.jar, self.args)

    def is_a_copy_of(self, application):
        return super().is_a_copy_of(application) and self.jar == application.jar and self.args == application.args

