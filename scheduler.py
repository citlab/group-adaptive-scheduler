from abc import ABCMeta, abstractmethod
from sklearn.cross_validation import LeaveOneOut
from cluster import Cluster
from application import Application
from complementarity import ComplementarityEstimation
from repeated_timer import RepeatedTimer


class NoApplicationCanBeScheduled(BaseException):
    pass


class Scheduler(metaclass=ABCMeta):
    def __init__(self, estimation: ComplementarityEstimation, cluster: Cluster, update_interval=60):
        self.queue = []
        self.estimation = estimation
        self.cluster = cluster
        self._timer = RepeatedTimer(update_interval, self.update_estimation)

    def update_estimation(self):
        for (apps, usage) in self.cluster.apps_usage():
            if len(apps) > 0:
                rate = self.usage2rate(usage)
                for rest, out in LeaveOneOut(len(apps)):
                    self.estimation.update_app(apps[out][0], apps[rest], rate)

    @staticmethod
    def usage2rate(usage):
        return usage.sum()

    def add(self, app: Application):
        self.queue.append(app)

    def update_schedule(self):
        while True:
            try:
                app = self.schedule_application()
            except NoApplicationCanBeScheduled:
                return
            app.start(self.cluster.resource_manager, self._on_app_finished)

    def _on_app_finished(self, app: Application):
        print("Application {} has finished".format(app))
        self.update_schedule()

    @abstractmethod
    def schedule_application(self) -> Application:
        pass


class RoundRobin(Scheduler):
    def schedule_application(self):
        tasks = self.queue[0].tasks
        n = len(tasks)
        if n > self.cluster.available_containers:
            raise NoApplicationCanBeScheduled

        i = 0
        for node in self.cluster.nodes.values():
            while node.available_containers > 0 and i < n:
                node.add_task(tasks[i])
                i += 1

        return self.queue.pop(0)

