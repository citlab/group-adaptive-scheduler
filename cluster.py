from stat_collector import StatCollector
from complementarity import Job


class Application(Job):
    def __init__(self, name, cmd_line, n_containers):
        super().__init__(name)
        self.cmd_line = cmd_line
        self.id = None
        self.tasks = [Task(self) for i in range(n_containers)]


class Task:
    def __init__(self, application: Application):
        self.application = application
        self.pid = None
        self.container = None


class Node:
    def __init__(self, name, n_containers):
        self.name = name
        self.n_containers = n_containers
        self.tasks = []

    def add_task(self, task: Task):
        if self.available_containers() < 1:
            raise ValueError("Not enough containers to schedule a task")
        self.tasks.append(task)

    def remove_application(self, app: Application):
        self.tasks = [task for task in self.tasks if task.application != app]

    @property
    def applications(self):
        apps = {}

        for task in self.tasks:
            apps[task.application.name] = task.application

        return apps.values()

    def available_containers(self):
        return self.n_containers - len(self.tasks)

    def pid_to_application(self, pid):
        for task in self.tasks:
            if task.pid is not None and task.pid == pid:
                return task.application
        raise ValueError("No application found")


class Cluster:
    def __init__(self, node_name_pattern, n_nodes, n_containers, stat_collector: StatCollector):
        self.node_names = [node_name_pattern.format(i) for i in range(n_nodes)]
        self.nodes = [Node(name, n_containers) for name in self.node_names]
        self.stat_collector = stat_collector

    def apps_usage(self):
        mean_usage = self.stat_collector.mean_usage(self.node_names)
        nodes_applications = self.__running_apps()
        
        apps_usage = []
        for i in range(len(self.nodes)):
            apps_usage.append(
                (nodes_applications[i], mean_usage[i])
            )
        
        return apps_usage

    def __running_apps(self):
        self.__update_task_pid()
        running_pids = self.stat_collector.running_processes_pid(self.node_names)
        nodes_applications = []
        
        for i, node in enumerate(self.nodes):
            applications = []
            for pid in running_pids[i]:
                try:
                    applications.append(node.pid_to_application(pid))
                except ValueError:
                    pass
            nodes_applications.append(applications)
            
        return nodes_applications

    def __update_task_pid(self):
        for node in self.nodes:
            for task in node.tasks:
                if task.pid is None:
                    task.pid = self.stat_collector.get_pid(task.container, node.name)




