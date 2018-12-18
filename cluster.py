from stat_collector import StatCollector, Server, Usage
from resource_manager import ResourceManager
from application import Application, Container
from typing import List, Tuple
from tabulate import tabulate
import operator


class Node(Server):
    def __init__(self, address: str, n_containers: int):
        super().__init__(address)
        self.n_containers = n_containers
        self.containers = []
        print("Init new node: {} - container number: {}".format(address, n_containers))

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

    def applications(self, by_name=False, is_running=False):
        apps = {}

        for container in self.containers:
            if not container.is_negligible and (container.application.is_running or not is_running):
                key = getattr(container.application, 'name' if by_name else 'id')
                apps[key] = container.application

        return list(apps.values())

    def available_containers(self):
        return self.n_containers - len(self.containers)

    def is_empty(self):
        return len(self.containers) == 0


class Cluster:
    def __init__(self, resource_manager: ResourceManager, stat_collector: StatCollector, application_master, node_containers=None):
        self.resource_manager = resource_manager
        self.stat_collector = stat_collector
        self.nodes = {}
        self.application_master = application_master

        for address, n_containers in self.resource_manager.nodes().items():
            if address == self.application_master: # Don't place job on node running application master
                continue
            self.nodes[address] = Node(address, n_containers if node_containers is None else node_containers)

    def apps_usage(self) -> List[Tuple[List[Application], Usage]]:
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
                apps[address] = node.applications(is_running=True)
            
        return apps

    def available_containers(self):
        return sum(n.available_containers() for n in self.nodes.values())

    def applications(self, with_full_nodes=True, by_name=False):
        apps = {}
        for node in self.nodes.values():
            if node.available_containers() > 0 or with_full_nodes:
                for app in node.applications(by_name=by_name, is_running=True):
                    key = getattr(app, 'name' if by_name else 'id')
                    if key in apps:
                        apps[key][1] += 1
                    else:
                        apps[key] = [app, 1]
        return zip(*apps.values()) if len(apps) > 0 else ([], [])

    def has_application_scheduled(self):
        for node in self.nodes.values():
            if len(node.applications()) > 0:
                return True
        return False

    def has_application_running(self):
        for node in self.nodes.values():
            if len(node.applications(is_running=True)) > 0:
                return True
        return False

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


