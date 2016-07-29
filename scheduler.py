from abc import ABCMeta, abstractmethod
from sklearn.cross_validation import LeaveOneOut
from cluster import Cluster
from application import Application
from complementarity import ComplementarityEstimation
from repeated_timer import RepeatedTimer
from threading import Lock
from typing import List


class NoApplicationCanBeScheduled(BaseException):
    pass


class Scheduler(metaclass=ABCMeta):
    def __init__(self, estimation: ComplementarityEstimation, cluster: Cluster, update_interval=60):
        self.queue = []
        self.estimation = estimation
        self.cluster = cluster
        self._timer = RepeatedTimer(update_interval, self.update_estimation)
        self.scheduler_lock = Lock()

    def update_estimation(self):
        for (apps, usage) in self.cluster.apps_usage():
            if len(apps) > 0:
                rate = self.usage2rate(usage)
                for rest, out in LeaveOneOut(len(apps)):
                    self.estimation.update_app(apps[out][0], apps[rest], rate)

    @staticmethod
    def usage2rate(usage):
        return usage[0] + usage[1] + 0.3 * usage[2]

    def add(self, app: Application):
        self.queue.append(app)

    def add_all(self, apps: List[Application]):
        self.queue.extend(apps)

    def run(self):
        self.scheduler_lock.acquire()
        while len(self.queue) > 0:
            try:
                app = self.schedule_application()
            except NoApplicationCanBeScheduled:
                print("No Application can be scheduled right now")
                break
            app.start(self.cluster.resource_manager, self._on_app_finished)
        self.scheduler_lock.release()

    def _on_app_finished(self, app: Application):
        self.cluster.remove_applications(app)
        self.run()

    @abstractmethod
    def schedule_application(self) -> Application:
        pass


class RoundRobin(Scheduler):
    def schedule_application(self):
        app = self.queue[0]
        if app.n_containers > self.cluster.available_containers():
            raise NoApplicationCanBeScheduled

        i = 0
        while i < app.n_containers:
            for node in self.cluster.nodes.values():
                if node.available_containers() > 0:
                    if i < app.n_tasks:
                        node.add_container(app.tasks[i])
                        i += 1
                    # add application master
                    elif i < app.n_containers:
                        node.add_container(app)

        return self.queue.pop(0)

