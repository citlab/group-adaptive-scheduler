from abc import ABCMeta, abstractmethod
from yarn_api_client import ResourceManager as YarnResourceManager
from typing import Dict
from threading import Lock


class ResourceManager(metaclass=ABCMeta):
    @abstractmethod
    # [(node_name, n_containers)]
    def nodes(self) -> Dict[str, int]:
        pass

    @abstractmethod
    def next_application_id(self) -> str:
        pass

    @abstractmethod
    def is_application_running(self, application_id: str) -> bool:
        pass

    @abstractmethod
    def is_application_finished(self, application_id: str) -> bool:
        pass


class DummyRM(ResourceManager):
    def __init__(self, n_nodes=4, n_containers=8, node_pattern="N{}", app_pattern="A{}", apps_running=None,
                 apps_submitted=0, apps_finished=None):
        self.n_nodes = n_nodes
        self.n_containers = n_containers
        self.node_pattern = node_pattern
        self.app_pattern = app_pattern
        self.apps_running = {} if apps_running is None else apps_running
        self.apps_finished = {} if apps_finished is None else apps_finished
        self.apps_submitted = apps_submitted

    def nodes(self):
        nodes = {}
        for i in range(self.n_nodes):
            nodes[self.node_pattern.format(i)] = self.n_containers
        return nodes

    def next_application_id(self):
        self.apps_submitted += 1
        return self.app_pattern.format(self.apps_submitted)

    def is_application_running(self, application_id: str):
        return self.apps_running.get(application_id, False)

    def is_application_finished(self, application_id: str):
        return self.apps_finished.get(application_id, False)


class Yarn(YarnResourceManager, ResourceManager):
    def __init__(self, address, port=8088, timeout=30):
        super().__init__(address=address, port=port, timeout=timeout)
        self.cluster_started_on = self.cluster_information().data['clusterInfo']['startedOn']
        self.__next_app_id = self.cluster_metrics().data['clusterMetrics']['appsSubmitted']
        self.lock = Lock()

    def nodes(self):
        nodes = {}

        for node in self.cluster_nodes().data['nodes']['node']:
            nodes[node['nodeHostName']] = node['availableVirtualCores']

        return nodes

    def next_application_id(self):
        self.__next_app_id += 1
        return "application_{}_{:04}".format(self.cluster_started_on, self.__next_app_id)

    def is_application_running(self, application_id):
        self.lock.acquire()
        try:
            output = self.cluster_application(application_id).data['app']['state'] == "RUNNING"
        except BaseException as e:
            print(e)
            output = False

        self.lock.release()
        return output

    def is_application_finished(self, application_id):
        self.lock.acquire()

        try:
            output = self.cluster_application(application_id).data['app']['state'] == "FINISHED"
        except BaseException as e:
            print(e)
            output = False

        self.lock.release()
        return output


