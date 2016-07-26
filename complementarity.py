import numpy as np
from abc import ABCMeta, abstractmethod
import operator
from typing import Dict, List


class Job:
    def __init__(self, name):
        self.name = str(name)
        
    def __str__(self):
        return self.name


class ComplementarityEstimation(metaclass=ABCMeta):
    def __init__(self, recurrent_jobs: List[Job]):
        self.shape = (len(recurrent_jobs), len(recurrent_jobs))
        self.jobs = recurrent_jobs
        self.index = {}
        self.reverse_index = {}
        for i, job in enumerate(recurrent_jobs):
            self.index[job.name] = i
            self.reverse_index[i] = job.name

    @abstractmethod
    def best_job_index(self, scheduled_jobs: List[Job], jobs: List[Job]) -> int:
        pass

    @abstractmethod
    def best_node_index(self, nodes_jobs: Dict[str, List[Job]], job_to_schedule: Job) -> str:
        pass

    @abstractmethod
    def update_job(self, job: Job, concurrent_jobs: List[Job], rate: float):
        pass

    def indices(self, jobs: List[Job]) -> List[int]:
        if isinstance(jobs, Job):
            jobs = [jobs]
        return [self.index[j.name] for j in jobs]

    def job_ids(self, indices: List[int]) -> List[Job]:
        if not isinstance(indices, list):
            indices = [indices]
        return [self.reverse_index[i] for i in indices]


class EpsilonGreedyEstimation(ComplementarityEstimation):
    def __init__(self, recurrent_jobs, initial_average=0., epsilon=0.1):
        super().__init__(recurrent_jobs)
        self.epsilon = epsilon
        self.average = np.full(self.shape, float(initial_average))
        self.update_count = np.ones(self.shape)

    def update_job(self, job, concurrent_jobs, rate):
        ix = np.ix_(self.indices(job), self.indices(concurrent_jobs))

        self.update_count[ix] += 1
        self.average[ix] += (rate - self.average[ix]) / self.update_count[ix]

    def best_job_index(self, scheduled_jobs, jobs):
        rates = self.expected_rates(scheduled_jobs, jobs)
        ix = np.argsort(rates)

        return self.__greedy(ix)

    def expected_rates(self, jobs, jobs_to_schedule):
        return self.average[np.ix_(
            self.indices(jobs),
            self.indices(jobs_to_schedule)
        )].sum(axis=0)

    def __greedy(self, items):
        if np.random.uniform() < self.epsilon:
            return items[np.random.randint(0, len(items) - 1)]
        return items[-1]

    def best_node_index(self, nodes_jobs, job_to_schedule):
        sorted_nodes_jobs = sorted(
            map(
                lambda node_jobs: (nodes_jobs[0], self.expected_rates(node_jobs[1], job_to_schedule)[0]),
                nodes_jobs.items()
            ),
            key=operator.itemgetter(1)
        )
        sorted_nodes = list(map(operator.itemgetter(0), sorted_nodes_jobs))

        return self.__greedy(sorted_nodes)


class GradientEstimation(ComplementarityEstimation):
    def __init__(self, recurrent_jobs, alpha=0.01, initial_average=1.5):
        super().__init__(recurrent_jobs)
        self.alpha = alpha
        self.average = np.full(self.shape[0], float(initial_average))
        self.update_count = np.ones(self.shape[0])
        self.preferences = np.zeros(self.shape)

    def update_job(self, job, concurrent_jobs, rate):
        job = self.indices(job)
        concurrent_jobs = self.indices(concurrent_jobs)

        self.update_count[job] += 1
        self.average[job] += (rate - self.average[job]) / self.update_count[job]

        other_jobs = np.delete(list(self.index.values()), concurrent_jobs)
        ap_concurrent = self.__action_probabilities(job, concurrent_jobs)
        ap_other = self.__action_probabilities(job, other_jobs)

        constant = self.alpha * (rate - self.average[job])

        ix = np.ix_(job, concurrent_jobs)
        self.preferences[ix] += constant * (1 - ap_concurrent)

        ix = np.ix_(job, other_jobs)
        self.preferences[ix] -= constant * ap_other

    def __action_probabilities(self, jobs_index, concurrent_jobs_index):
        exp = np.exp(self.preferences[jobs_index])

        return exp[:, concurrent_jobs_index] / exp.sum()

    def best_job_index(self, scheduled_jobs, jobs):
        p = self.normalized_action_probabilities(scheduled_jobs, jobs)
        return self.__choose(np.arange(len(jobs)), p=p)

    def normalized_action_probabilities(self, jobs, jobs_to_schedule):
        p = self.__action_probabilities(self.indices(jobs), self.indices(jobs_to_schedule)).sum(axis=0)
        return p / p.sum()

    @staticmethod
    def __choose(items, p):
        indices = np.arange(len(items))
        return items[np.random.choice(indices, p=p)]

    def best_node_index(self, nodes_jobs, job_to_schedule):
        n = len(nodes_jobs)
        p = np.zeros(n)
        nodes = []
        for i, (node_name, jobs) in enumerate(nodes_jobs.items()):
            p[i] = self.normalized_action_probabilities(jobs, job_to_schedule)
            nodes.append(node_name)
        p /= p.sum()

        return self.__choose(nodes, p)


