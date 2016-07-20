import numpy as np
from abc import ABCMeta, abstractmethod
import operator


class Job:
    def __init__(self, name):
        self.name = str(name)
        
    def __str__(self):
        return self.name


class ComplementarityEstimation(metaclass=ABCMeta):
    def __init__(self, recurrent_jobs):
        self.shape = (len(recurrent_jobs), len(recurrent_jobs))
        self.jobs = recurrent_jobs
        self.index = {}
        self.reverse_index = {}
        for i, job in enumerate(recurrent_jobs):
            self.index[job.name] = i
            self.reverse_index[i] = job.name

    @abstractmethod
    def argsort_jobs(self, scheduled_jobs, jobs):
        pass

    @abstractmethod
    # nodes_jobs: [(node_name, jobs)]
    def sort_nodes(self, nodes_jobs, job_to_schedule):
        pass

    @abstractmethod
    def update_job(self, job, concurrent_jobs, rate):
        pass

    def indices(self, jobs):
        if isinstance(jobs, Job):
            jobs = [jobs]
        return [self.index[j.name] for j in jobs]

    def job_ids(self, indices):
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

    def argsort_jobs(self, scheduled_jobs, jobs):
        rates = self.expected_rates(scheduled_jobs, jobs)
        return self.__gen_list(np.argsort(rates).tolist())

    def __gen_list(self, sorted_list):
        result = []
        n = len(sorted_list)

        for i in range(n):
            if np.random.uniform() < self.epsilon:
                result.append(sorted_list.pop(np.random.randint(0, n - 1)))
            else:
                result.append(sorted_list.pop())

        return result

    def expected_rates(self, jobs, jobs_to_schedule):
        return self.average[np.ix_(
            self.indices(jobs),
            self.indices(jobs_to_schedule)
        )].sum(axis=0)

    def sort_nodes(self, nodes_jobs, job_to_schedule):
        sorted_nodes = sorted(
            map(
                lambda node_jobs: (nodes_jobs[0], self.expected_rates(node_jobs[1], job_to_schedule)[0]),
                nodes_jobs
            ),
            key=operator.itemgetter(1)
        )

        return self.__gen_list(list(map(operator.itemgetter(0), sorted_nodes)))


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

    def argsort_jobs(self, scheduled_jobs, jobs):
        p = self.normalized_action_probabilities(scheduled_jobs, jobs)
        return self.__gen_list(np.arange(len(jobs)), p=p)

    def normalized_action_probabilities(self, jobs, jobs_to_schedule):
        p = self.__action_probabilities(self.indices(jobs), self.indices(jobs_to_schedule)).sum(axis=0)
        return p / p.sum()

    @staticmethod
    def __gen_list(objects, p):
        n = p.shape[0]
        indices = np.arange(n)
        result = []

        for i in range(n):
            index = np.random.choice(indices, p=p)
            result.append(objects[index][0])
            p[index] = 0
            p /= p.sum()

        return result

    def sort_nodes(self, nodes_jobs, job_to_schedule):
        n = len(nodes_jobs)
        p = np.zeros(n)
        for i, (node_name, jobs) in enumerate(nodes_jobs):
            p[i] = self.normalized_action_probabilities(jobs, job_to_schedule)
        p /= p.sum()

        return self.__gen_list(nodes_jobs, p)


