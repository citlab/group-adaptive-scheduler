import numpy as np
from abc import ABCMeta, abstractmethod
import operator
from typing import Dict, List
from application import Application


class ComplementarityEstimation(metaclass=ABCMeta):
    def __init__(self, recurrent_apps: List[Application]):
        self.shape = (len(recurrent_apps), len(recurrent_apps))
        self.apps = recurrent_apps
        self.index = {}
        self.reverse_index = {}
        for i, app in enumerate(recurrent_apps):
            self.index[app.name] = i
            self.reverse_index[i] = app.name

    @abstractmethod
    def best_app_index(self, scheduled_apps: List[Application], apps: List[Application]) -> int:
        pass

    @abstractmethod
    def best_node_index(self, nodes_apps: Dict[str, List[Application]], app_to_schedule: Application) -> str:
        pass

    @abstractmethod
    def update_app(self, app: Application, concurrent_apps: List[Application], rate: float):
        pass

    def indices(self, apps: List[Application]) -> List[int]:
        if isinstance(apps, Application):
            apps = [apps]
        return [self.index[j.name] for j in apps]

    def app_ids(self, indices: List[int]) -> List[Application]:
        if not isinstance(indices, list):
            indices = [indices]
        return [self.reverse_index[i] for i in indices]


class EpsilonGreedyEstimation(ComplementarityEstimation):
    def __init__(self, recurrent_apps, initial_average=0., epsilon=0.1):
        super().__init__(recurrent_apps)
        self.epsilon = epsilon
        self.average = np.full(self.shape, float(initial_average))
        self.update_count = np.ones(self.shape)

    def update_app(self, app, concurrent_apps, rate):
        ix = np.ix_(self.indices(app), self.indices(concurrent_apps))

        self.update_count[ix] += 1
        self.average[ix] += (rate - self.average[ix]) / self.update_count[ix]

    def best_app_index(self, scheduled_apps, apps):
        rates = self.expected_rates(scheduled_apps, apps)
        ix = np.argsort(rates)

        return self.__greedy(ix)

    def expected_rates(self, apps, apps_to_schedule):
        return self.average[np.ix_(
            self.indices(apps),
            self.indices(apps_to_schedule)
        )].sum(axis=0)

    def __greedy(self, items):
        if np.random.uniform() < self.epsilon:
            return items[np.random.randint(0, len(items) - 1)]
        return items[-1]

    def best_node_index(self, nodes_apps, app_to_schedule):
        sorted_nodes_apps = sorted(
            map(
                lambda node_apps: (nodes_apps[0], self.expected_rates(node_apps[1], app_to_schedule)[0]),
                nodes_apps.items()
            ),
            key=operator.itemgetter(1)
        )
        sorted_nodes = list(map(operator.itemgetter(0), sorted_nodes_apps))

        return self.__greedy(sorted_nodes)


class GradientEstimation(ComplementarityEstimation):
    def __init__(self, recurrent_apps, alpha=0.01, initial_average=1.5):
        super().__init__(recurrent_apps)
        self.alpha = alpha
        self.average = np.full(self.shape[0], float(initial_average))
        self.update_count = np.ones(self.shape[0])
        self.preferences = np.zeros(self.shape)

    def update_app(self, app, concurrent_apps, rate):
        app = self.indices(app)
        concurrent_apps = self.indices(concurrent_apps)

        self.update_count[app] += 1
        self.average[app] += (rate - self.average[app]) / self.update_count[app]

        other_apps = np.delete(list(self.index.values()), concurrent_apps)
        ap_concurrent = self.__action_probabilities(app, concurrent_apps)
        ap_other = self.__action_probabilities(app, other_apps)

        constant = self.alpha * (rate - self.average[app])

        ix = np.ix_(app, concurrent_apps)
        self.preferences[ix] += constant * (1 - ap_concurrent)

        ix = np.ix_(app, other_apps)
        self.preferences[ix] -= constant * ap_other

    def __action_probabilities(self, apps_index, concurrent_apps_index):
        exp = np.exp(self.preferences[apps_index])

        return exp[:, concurrent_apps_index] / exp.sum()

    def best_app_index(self, scheduled_apps, apps):
        p = self.normalized_action_probabilities(scheduled_apps, apps)
        return self.__choose(np.arange(len(apps)), p=p)

    def normalized_action_probabilities(self, apps, apps_to_schedule):
        p = self.__action_probabilities(self.indices(apps), self.indices(apps_to_schedule)).sum(axis=0)
        return p / p.sum()

    @staticmethod
    def __choose(items, p):
        indices = np.arange(len(items))
        return items[np.random.choice(indices, p=p)]

    def best_node_index(self, nodes_apps, app_to_schedule):
        n = len(nodes_apps)
        p = np.zeros(n)
        nodes = []
        for i, (node_name, apps) in enumerate(nodes_apps.items()):
            p[i] = self.normalized_action_probabilities(apps, app_to_schedule)
            nodes.append(node_name)
        p /= p.sum()

        return self.__choose(nodes, p)

