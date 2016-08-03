from stat_collector import StatCollector, Server
from resource_manager import ResourceManager
from application import Application, Container
from typing import List, Tuple
import numpy as np
from tabulate import tabulate
import operator


class Node(Server):
    def __init__(self, address: str, n_containers: int):
        super().__init__(address)
        self.n_containers = n_containers
        self.containers = []

    def add_container(self, container: Container):
        if self.available_containers() < 1:
            raise ValueError("No container is available")

        if container.node is not None:
            raise ValueError("Container has already been scheduled on a node")

        self.containers.append(container)
        container.node = self

    def remove_application(self, app: Application):
        staying_containers = []
        for container in self.containers:
            if container.application == app:
                container.node = None
            else:
                staying_containers.append(container)
        self.containers = staying_containers

    def applications(self):
        apps = {}

        for container in self.containers:
            if not container.is_negligible:
                apps[container.application.id] = container.application

        return list(apps.values())

    def available_containers(self):
        return self.n_containers - len(self.containers)

    def is_empty(self):
        return len(self.containers) == 0


class Cluster:
    def __init__(self, resource_manager: ResourceManager, stat_collector: StatCollector):
        self.resource_manager = resource_manager
        self.stat_collector = stat_collector
        self.nodes = {}

        for address, n_containers in self.resource_manager.nodes().items():
            self.nodes[address] = Node(address, n_containers)

    def apps_usage(self) -> List[Tuple[List[Application], np.ndarray]]:
        mean_usage = self.stat_collector.mean_usage(self.nodes)
        nodes_applications = self.node_running_apps()
        
        apps_usage = []
        for address in self.nodes.keys():
            apps_usage.append(
                (nodes_applications[address], mean_usage[address])
            )
        
        return apps_usage

    def empty_nodes(self):
        return [node for node in self.nodes.values() if node.is_empty()]

    def non_full_nodes(self):
        return [node for node in self.nodes.values() if node.available_containers() > 0]

    def node_running_apps(self, with_full_nodes=True):
        apps = {}

        for address, node in self.nodes.items():
            if with_full_nodes or node.available_containers() > 0:
                apps[address] = [app for app in node.applications() if app.is_running]
            
        return apps

    def available_containers(self):
        return sum(n.available_containers() for n in self.nodes.values())

    def applications(self):
        apps = []
        for node in self.nodes.values():
            apps += node.applications()
        return set(apps)

    def non_full_node_applications(self):
        apps = []
        for node in self.nodes.values():
            if node.available_containers() > 0:
                apps += node.applications()
        return set(apps)

    def remove_applications(self, application: Application):
        for node in self.nodes.values():
            node.remove_application(application)

    def print_nodes(self):
        headers = ["Nodes", "Applications"]
        rows = []

        for address, node in self.nodes.items():
            rows.append([address, ",".join(map(str, node.applications()))])

        sorted_rows = sorted(
            rows,
            key=operator.itemgetter(0)
        )

        print(tabulate(sorted_rows, headers, tablefmt='pipe'))


