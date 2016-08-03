from abc import ABCMeta, abstractmethod
from sklearn.cross_validation import LeaveOneOut
from cluster import Cluster
from application import Application
from complementarity import ComplementarityEstimation
from repeated_timer import RepeatedTimer
from threading import Lock
from typing import List
import time
import numpy as np


class NoApplicationCanBeScheduled(BaseException):
    pass


class Scheduler(metaclass=ABCMeta):
    def __init__(self, estimation: ComplementarityEstimation, cluster: Cluster, update_interval=60):
        self.queue = []
        self.estimation = estimation
        self.cluster = cluster
        self._timer = RepeatedTimer(update_interval, self.update_estimation)
        self.scheduler_lock = Lock()
        self.started_at = None
        self.stopped_at = None

    def start(self):
        self.schedule()
        self._timer.start()
        self.started_at = time.time()

    def stop(self):
        self._timer.cancel()
        self.stopped_at = time.time()

    def update_estimation(self):
        for (apps, usage) in self.cluster.apps_usage():
            if len(apps) > 0:
                rate = self.usage2rate(usage)
                for rest, out in LeaveOneOut(len(apps)):
                    self.estimation.update_app(apps[out[0]], [apps[i] for i in rest], rate)
        self.estimation.print()

    @staticmethod
    def usage2rate(usage):
        return usage[0] + usage[1] + usage[2]

    def add(self, app: Application):
        self.queue.append(app)

    def add_all(self, apps: List[Application]):
        self.queue.extend(apps)

    def schedule(self):
        while len(self.queue) > 0:
            try:
                app = self.schedule_application()
            except NoApplicationCanBeScheduled:
                print("No Application can be scheduled right now")
                self.cluster.print_nodes()
                break
            app.start(self.cluster.resource_manager, self._on_app_finished)

    def _on_app_finished(self, app: Application):
        self.scheduler_lock.acquire()
        self.cluster.remove_applications(app)
        if len(self.queue) == 0 and len(self.cluster.applications()) == 0:
            self.stop()
            self.on_stop()
        else:
            self.schedule()
        self.scheduler_lock.release()

    def on_stop(self):
        delta = self.stopped_at - self.started_at
        print("Queue took {:.0f}'{:.0f} to complete".format(delta // 60, delta % 60))
        self.estimation.save('estimation')

    @abstractmethod
    def schedule_application(self) -> Application:
        pass


class RoundRobin(Scheduler):
    def schedule_application(self):
        app = self.queue[0]
        self.place_empty_first(app)
        return self.queue.pop(0)

    def place_containers(self, app: Application):
        if app.n_containers > self.cluster.available_containers():
            raise NoApplicationCanBeScheduled

        n_containers_scheduled = 0
        while n_containers_scheduled < app.n_containers:
            for node in self.cluster.nodes.values():
                if node.available_containers() > 0:
                    if n_containers_scheduled < app.n_tasks:
                        node.add_container(app.tasks[n_containers_scheduled])
                        n_containers_scheduled += 1
                    # add application master
                    elif n_containers_scheduled < app.n_containers:
                        node.add_container(app)
                        return

    def place_quarter(self, app: Application):
        if app.n_containers > self.cluster.available_containers():
            raise NoApplicationCanBeScheduled

        empty_nodes = self.cluster.empty_nodes()

        n_containers_scheduled = 0
        while len(empty_nodes) > 0 and n_containers_scheduled < app.n_containers:
            n_containers_scheduled += self.place(app, empty_nodes.pop())

        half_nodes = list(filter(
            lambda n: n.available_containers() == n.n_containers / 2,
            self.cluster.nodes.values()
        ))

        while len(half_nodes) > 0 and n_containers_scheduled < app.n_containers:
            n_containers_scheduled += self.place(app, half_nodes.pop())

    @staticmethod
    def place(app, node, n_containers=4):
        if n_containers <= 0:
            raise ValueError("Can not place {} containers".format(n_containers))
        # print("Place {} on {} ({})".format(app, node, node.available_containers()))

        n = len([t for t in app.tasks if t.node is not None])
        n += 1 if app.node is not None else 0

        for k in range(n, n + n_containers):
            if k < app.n_tasks:
                node.add_container(app.tasks[k])
            elif k < app.n_containers:
                node.add_container(app)
                break

        return k - n + 1

    def place_empty_first(self, app: Application):
        if app.n_containers > self.cluster.available_containers():
            raise NoApplicationCanBeScheduled

        empty_nodes = self.cluster.empty_nodes()

        n_containers_scheduled = 0
        # while len(empty_nodes) > 0 and n_containers_scheduled < app.n_containers:
        #     n_containers_scheduled += self.place(app, empty_nodes.pop())

        while n_containers_scheduled < app.n_containers:
            nodes = [
                n for n in self.cluster.non_full_nodes()
                if len(n.applications()) == 0 or n.applications()[0] != app
            ]
            node = nodes[np.random.randint(0, len(nodes))]
            n_containers_scheduled += self.place(app, node)


class Adaptive(RoundRobin):
    def __init__(self, jobs_to_peek=5, **kwargs):
        super().__init__(**kwargs)
        self.jobs_to_peek = jobs_to_peek

    def schedule_application(self):
        scheduled_apps = self.cluster.non_full_node_applications()
        available_containers = self.cluster.available_containers()
        index = list(range(min(self.jobs_to_peek, len(self.queue))))

        while len(index) > 0:
            best_i = self.estimation.best_app_index(
                scheduled_apps,
                [self.queue[i] for i in index]
            )
            print("Best app is {}".format(best_i))
            if self.queue[best_i].n_containers <= available_containers:
                self.place_application(self.queue[best_i])
                return self.queue.pop(best_i)
            index.pop(best_i)

        raise NoApplicationCanBeScheduled

    def place_application(self, app: Application):
        empty_nodes = self.cluster.empty_nodes()

        n_containers_scheduled = 0
        while len(empty_nodes) > 0 and n_containers_scheduled < app.n_containers:
            n_containers_scheduled += self.place(app, empty_nodes.pop())

        while n_containers_scheduled < app.n_containers:
            best_address = self.estimation.best_node_index(
                self.cluster.node_running_apps(with_full_nodes=False),
                app
            )
            n_containers_scheduled += self.place(app, self.cluster.nodes[best_address])
