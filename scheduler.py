from abc import ABCMeta, abstractmethod

from sklearn.cross_validation import LeaveOneOut

from cluster import Cluster
from application import Application
from complementarity import ComplementarityEstimation
from repeated_timer import RepeatedTimer


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
                    self.estimation.update_job(apps[out][0], apps[rest], rate)

    @staticmethod
    def usage2rate(usage):
        return usage.sum()

    def add(self, app: Application):
        self.queue.append(app)

    @abstractmethod
    def schedule(self):
        pass


class QueueModificationScheduler(Scheduler):
    def __init__(self, *args, running_jobs=2, jobs_to_peek=5):
        super().__init__(*args)
        self.running_jobs = running_jobs
        self.jobs_to_peek = jobs_to_peek

    def schedule(self):
        n = len(jobs)
        scheduled_jobs = [jobs.pop(0)]

        while len(scheduled_jobs) < n:
            index = self.estimation.best_job_index(
                scheduled_jobs[-self.running_jobs:],
                jobs[:self.jobs_to_peek]
            )
            scheduled_jobs.append(jobs.pop(index))

        return scheduled_jobs

