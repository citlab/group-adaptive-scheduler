import subprocess
import time
from typing import List
from threading import Thread
from resource_manager import ResourceManager
from abc import ABCMeta, abstractmethod


class NotCorrectlyScheduledError(Exception):
    pass


class Container(metaclass=ABCMeta):
    def __init__(self):
        self.container_id = None
        self.node = None
        self.pid = None
        self.is_negligible = False

    @property
    @abstractmethod
    def application(self):
        pass


class Application(Container):
    def __init__(self, name, n_tasks):
        super().__init__()
        self.name = name
        self.n_tasks = n_tasks
        self.id = None
        self.is_running = False
        self.tasks = [Task(self) for i in range(self.n_tasks)]
        self.thread = None
        self.is_negligible = True
        self.n_containers = self.n_tasks + 1

    @property
    def application(self):
        return self

    def __str__(self):
        return "{} ({})".format(self.id, self.name)

    def start(self, resource_manager: ResourceManager, on_finish=None, sleep_during_loop=2):
        self.id = resource_manager.next_application_id()
        print("Start Application {}".format(self))

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
        subprocess.run(" ".join(self.command_line()), shell=True)

        while not resource_manager.is_application_finished(self.id):
            if not self.is_running and resource_manager.is_application_running(self.id):
                self.is_running = True

            time.sleep(sleep_during_loop)

        print("Application {} has finished".format(self))

        if callable(on_finish):
            on_finish(self)

    def copy(self):
        return Application(self.name, len(self.tasks))

    def is_a_copy_of(self, application):
        return application.name == self.name and len(self.tasks) == len(application.tasks)


class Task(Container):
    def __init__(self, application: Application):
        super().__init__()
        self.app = application

    @property
    def application(self):
        return self.app


class DummyApplication(Application):
    def __init__(self, name="app", n_tasks=8, id="id", is_running=False):
        super().__init__(name, n_tasks)
        self.id = id
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
            "-ynm " + self.name,
            "-yn {}".format(len(self.tasks)),
            "-yD fix.container.hosts={tasks_host}@@fix.am.host={am_host}".format(
                tasks_host=",".join(self.tasks_hosts()),
                am_host=self.node.address
            ),
            self.jar
        ]
        cmd += self.args
        cmd.append("1> {}.log".format(self.id))

        return cmd

    def tasks_hosts(self):
        hosts = []
        for task in self.tasks:
            hosts.append(task.node.address)

        return hosts

    def copy(self):
        return FlinkApplication(self.name, len(self.tasks), self.jar, self.args)

    def is_a_copy_of(self, application):
        return super().is_a_copy_of(application) and self.jar == application.jar and self.args == application.args

