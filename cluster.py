from stat_collector import StatCollector, Server
from resource_manager import ResourceManager
from application import Application, Task
from typing import List, Tuple
import numpy as np


class Node(Server):
    def __init__(self, address: str, n_containers: int):
        super().__init__(address)
        self.n_containers = n_containers
        self.tasks = []

    def add_task(self, task: Task):
        if self.available_containers < 1:
            raise ValueError("Not enough containers to schedule a task")
        self.tasks.append(task)
        task.node = self

    def remove_application(self, app: Application):
        staying_tasks = []
        for task in self.tasks:
            if task.application != app:
                staying_tasks.append(task)
            else:
                task.node = None
        self.tasks = staying_tasks

    @property
    def applications(self):
        apps = {}

        for task in self.tasks:
            apps[task.application.name] = task.application

        return list(apps.values())

    @property
    def available_containers(self):
        return self.n_containers - len(self.tasks)


class Cluster:
    def __init__(self, resource_manager: ResourceManager, stat_collector: StatCollector):
        self.resource_manager = resource_manager
        self.stat_collector = stat_collector
        self.nodes = {}

        for address, n_containers in self.resource_manager.nodes():
            self.nodes[address] = Node(address, n_containers)

    def apps_usage(self) -> List[Tuple[List[Application], np.ndarray]]:
        mean_usage = self.stat_collector.mean_usage(self.nodes)
        nodes_applications = self.nodes_apps()
        
        apps_usage = []
        for address in self.nodes.keys():
            apps_usage.append(
                (nodes_applications[address], mean_usage[address])
            )
        
        return apps_usage

    def nodes_apps(self):
        nodes_applications = {}

        for address, node in self.nodes.items():
            apps = {}
            for task in node.tasks:
                if task.application.is_running:
                    apps[task.application.id] = task.application
            nodes_applications[address] = list(apps.values())
            
        return nodes_applications




