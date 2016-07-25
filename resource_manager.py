import subprocess
import time
import typing
from abc import ABCMeta, abstractmethod
from threading import Thread
from yarn_api_client import ResourceManager as YarnResourceManager
from complementarity import Job


class ResourceManager(metaclass=ABCMeta):
    @abstractmethod
    # [(node_name, n_containers)]
    def nodes(self) -> typing.List[typing.Tuple[str, int]]:
        pass

    @abstractmethod
    def next_application_id(self) -> str:
        pass

    @abstractmethod
    def is_application_running(self, application_id: str) -> bool:
        pass


class DummyRM(ResourceManager):
    def __init__(self, n_nodes=4, n_containers=8, node_pattern="N{}", app_pattern="A{}", apps_running=None,
                 apps_submitted=0):
        self.n_nodes = n_nodes
        self.n_containers = n_containers
        self.node_pattern = node_pattern
        self.app_pattern = app_pattern
        self.apps_running = {} if apps_running is None else apps_running
        self.apps_submitted = apps_submitted

    def nodes(self):
        nodes = []
        for i in range(self.n_nodes):
            nodes.append((self.node_pattern.format(i), self.n_containers))
        return nodes

    def next_application_id(self):
        self.apps_submitted += 1
        return self.app_pattern.format(self.apps_submitted)

    def is_application_running(self, application_id: str):
        return self.apps_running.get(application_id, False)


class YarnRM(YarnResourceManager, ResourceManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cluster_started_on = self.cluster_information().data['clusterInfo']['startedOn']
        self.__next_app_id = self.cluster_metrics()['clusterMetrics']['appsSubmitted']

    def nodes(self):
        nodes = []

        for _, node in self.cluster_nodes().data.items():
            nodes.append((node['nodeHostName'], node['availableVirtualCores']))

        return nodes

    def next_application_id(self):
        self.__next_app_id += 1
        return "application_{}_{:04}".format(self.cluster_started_on, self.__next_app_id)

    def is_application_running(self, application_id):
        return self.cluster_application(application_id).data['app']['state'] == "RUNNING"


class Application(Job):
    def __init__(self, name, n_tasks):
        super().__init__(name)
        self.id = None
        self.is_running = False
        self.tasks = [Task(self) for i in range(n_tasks)]
        self.thread = None

    def __str__(self):
        return "{} : {}".format(self.name, self.id)

    def start(self, resource_manager: ResourceManager, on_finish=None):
        self.thread = Thread(target=self._run, args=[resource_manager, on_finish])
        self.thread.start()

    def command_line(self) -> typing.List[str]:
        return [""]

    def _run(self, resource_manager: ResourceManager, on_finish):
        process = subprocess.Popen(self.command_line())

        while process.poll() is None:
            if not self.is_running and resource_manager.is_application_running(self.id):
                self.is_running = True

            time.sleep(2)

        if callable(on_finish):
            on_finish(self)


class Task:
    def __init__(self, application: Application):
        self.application = application
        self.container = None
        self.node = None
        self.pid = None


class NotCorrectlyScheduledError(Exception):
    pass


class FlinkApplication(Application):
    def __init__(self, name, n_task, jar, args):
        super().__init__(name, n_task)
        self.jar = jar
        self.args = args

    def command_line(self):
        cmd = ["$FLINK_HOME/bin/flink", "run", "-m yarn-cluster", "-yn {}".format(len(self.tasks))]
        cmd += ["-yq " + ",".join(self.hosts())]
        cmd += [self.jar] + self.args
        return cmd

    def hosts(self):
        hosts = []

        try:
            for task in self.tasks:
                hosts.append(task.node.address)
        except AttributeError:
            raise NotCorrectlyScheduledError(
                "A task of the application {} is not scheduled on a node".format(self.name)
            )

        return hosts

