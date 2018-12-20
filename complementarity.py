import numpy as np
from abc import ABCMeta, abstractmethod
import operator
from typing import Dict, List
from application import Application
import os
import errno
from pprint import pprint
from tabulate import tabulate
from job_group_data import JobGroupData


class ComplementarityEstimation(metaclass=ABCMeta):
    def __init__(self, recurrent_apps: List[Application]):
        self.shape = (len(recurrent_apps), len(recurrent_apps))
        self.apps = recurrent_apps
        self.index = {}
        self.reverse_index = {}
        # Loop with auto index through list of
        for i, app in enumerate(sorted(recurrent_apps, key=lambda a: a.name)):
            self.index[app.name] = i
            self.reverse_index[i] = app.name

    @abstractmethod
    def best_app_index(self, scheduled_apps: List[Application], apps: List[Application],
                       scheduled_apps_weight: np.ndarray = None) -> int:
        pass

    @abstractmethod
    def best_node_index(self, nodes_apps: Dict[str, List[Application]], app_to_schedule: Application) -> str:
        pass

    @abstractmethod
    def update_app(self, app: Application, concurrent_apps: List[Application], rate: float):
        pass

    @abstractmethod
    def save(self, folder):
        pass

    @abstractmethod
    def load(self, folder):
        pass

    @abstractmethod
    def print(self):
        pass

    def __str__(self):
        return type(self).__name__

    def indices(self, apps: List[Application]) -> List[int]:
        if isinstance(apps, Application):
            apps = [apps]
        return [self.index[j.name] for j in apps]

    def app_ids(self, indices: List[int]) -> List[Application]:
        if not isinstance(indices, list):
            indices = [indices]
        return [self.reverse_index[i] for i in indices]

    def _save(self, folder, filename, matrix):
        try:
            os.makedirs(folder)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        np.save("{}/{}.npy".format(folder, filename), matrix)
        with open("{}/{}_axes.txt".format(folder, filename), "w") as f:
            for i in range(len(self.reverse_index)):
                f.write(self.reverse_index[i] + "\n")


class EpsilonGreedy(ComplementarityEstimation):
    def __init__(self, recurrent_apps, initial_average=0., epsilon=0.1):
        super().__init__(recurrent_apps)
        self.epsilon = epsilon
        self.average = np.full(self.shape, float(initial_average))
        self.update_count = np.full(self.shape, 0 if initial_average == 0 else 1, dtype=np.int64)

    def update_app(self, app, concurrent_apps, rate):
        ix = np.ix_(self.indices(app), self.indices(concurrent_apps))

        self.update_count[ix] += 1
        self.average[ix] += (rate - self.average[ix]) / self.update_count[ix]

    def best_app_index(self, scheduled_apps, apps, scheduled_apps_weight=None):
        if len(scheduled_apps) == 0:
            return 0

        rates = self.expected_rates(scheduled_apps, apps)

        if np.unique(rates).size == 1:
            ix = list(range(len(apps)))[::-1]
        else:
            ix = np.argsort(rates)

        return self.__greedy(ix)

    def expected_rates(self, apps, apps_to_schedule, apps_weight=None):
        avg = self.average[np.ix_(
            self.indices(apps),
            self.indices(apps_to_schedule)
        )]
        if apps_weight is not None:
            avg = (avg.T * apps_weight).T

        return avg.sum(axis=0)

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
        rates = list(map(operator.itemgetter(1), sorted_nodes_apps))
        if np.unique(rates).size == 1:
            return nodes_apps.keys()[0]

        sorted_addresses = list(map(operator.itemgetter(0), sorted_nodes_apps))
        return self.__greedy(sorted_addresses)

    def save(self, folder):
        self._save(folder, "average", self.average)
        self._save(folder, "ucount", self.update_count)

    def load(self, folder):
        self.average = np.load("{}/average.npy".format(folder))
        self.update_count = np.load("{}/ucount.npy".format(folder))

    def print(self):
        rows = []
        headers = ["Preferences"] + list(self.reverse_index.values())
        for i, name in self.reverse_index.items():
            rows.append([name] + self.average[i].tolist())

        print(tabulate(rows, headers, tablefmt='pipe'))


class Gradient(ComplementarityEstimation):
    def __init__(self, recurrent_apps, alpha=0.01, initial_average=0):
        super().__init__(recurrent_apps)
        self.alpha = alpha
        self.average = np.full(self.shape[0], float(initial_average))
        self.update_count = np.full(self.shape[0], 0 if initial_average == 0 else 1, dtype=np.int64)
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

        return (exp[:, concurrent_apps_index].T / exp.sum(axis=1)).T

    def best_app_index(self, scheduled_apps, apps, scheduled_apps_weight=None):
        if len(scheduled_apps) == 0:
            return np.random.randint(0, len(apps))
        return self.__choose(
            np.arange(len(apps)),
            self.normalized_action_probabilities(scheduled_apps, apps, scheduled_apps_weight)
        )

    def normalized_action_probabilities(self, apps, apps_to_schedule, apps_weight=None):
        p = self.__action_probabilities(self.indices(apps), self.indices(apps_to_schedule))
        if apps_weight is not None:
            p = (p.T * apps_weight).T
        p = p.sum(axis=0)
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

        return self.__choose(nodes, p / p.sum())

    def save(self, folder):
        self._save(folder, "average", self.average)
        self._save(folder, "preferences", self.preferences)
        self._save(folder, "ucount", self.update_count)

    def load(self, folder):
        self.average = np.load("{}/average.npy".format(folder))
        self.update_count = np.load("{}/ucount.npy".format(folder))
        self.preferences = np.load("{}/preferences.npy".format(folder))

    def print(self):
        apps_name = list(self.reverse_index.values())
        print(tabulate(
            [
                ["Average"] + self.average.tolist(),
                ["Count"] + self.update_count.tolist(),
            ],
            apps_name,
            tablefmt='pipe'
        ))

        rows = []
        headers = ["Preferences"] + apps_name
        for i, name in self.reverse_index.items():
            rows.append([name] + self.preferences[i].tolist())

        print(tabulate(rows, headers, tablefmt='pipe'))


class GroupGradient(Gradient):
    def __init__(self, recurrent_apps: List[Application], alpha=0.01, initial_average=0):
        super().__init__(recurrent_apps)
        self.shape = (len(JobGroupData.groups), len(JobGroupData.groups))
        self.apps = recurrent_apps
        self.index = {}
        self.reverse_index = {}
        # Loop with auto index through list of
        for i, app in enumerate(sorted(recurrent_apps, key=lambda a: a.name)):
            index = JobGroupData.groupIndexes[app.name]
            self.index[app.name] = index
            self.reverse_index[index] = JobGroupData.group_names[index]

        self.alpha = alpha
        self.average = np.full(self.shape[0], float(initial_average))
        self.update_count = np.full(self.shape[0], 0 if initial_average == 0 else 1, dtype=np.int64)
        self.preferences = np.zeros(self.shape)

    def update_app(self, app, concurrent_apps, rate):
        print("+++++++++++ Complementarity Update_app()")
        print("+++++++++++ App to update: {}".format(str(app)))
        print("+++++++++++ Concurrent apps with above app: {}".format(str(concurrent_apps)))
        app = self.indices(app)
        concurrent_apps = self.indices(concurrent_apps)
        print("+++++++++++ Apps to update (indices): {}".format(str(app)))
        print("+++++++++++ Concurrent apps with above app (indices): {}".format(str(concurrent_apps)))

        self.update_count[app] += 1
        self.average[app] += (rate - self.average[app]) / self.update_count[app]

        other_apps = np.delete(list(set(self.index.values())), concurrent_apps)
        print("+++++++++++ Other apps: {}".format(str(other_apps)))
        ap_concurrent = self.__action_probabilities(app, concurrent_apps)
        ap_other = self.__action_probabilities(app, other_apps)
        print("+++++++++++ ap_concurrent: {}".format(str(ap_concurrent)))
        print("+++++++++++ ap_other: {}".format(str(ap_other)))

        constant = self.alpha * (rate - self.average[app])

        ix = np.ix_(app, concurrent_apps)
        print("+++++++++++ ix (app, concurrent_apps): {}".format(str(ix)))
        self.preferences[ix] += constant * (1 - ap_concurrent)

        ix = np.ix_(app, other_apps)
        print("+++++++++++ ix (app, other_apps): {}".format(str(ix)))
        self.preferences[ix] -= constant * ap_other

    def __str__(self):
        return type(self).__name__

    def best_app_index(self, scheduled_apps, apps, scheduled_apps_weight=None):
        if len(scheduled_apps) == 0 or len(scheduled_apps) == 2:
            return -1, -1
        probabilities = self.normalized_action_probabilities(scheduled_apps, apps, scheduled_apps_weight)
        selected_app_group = self.__choose(
            np.arange(len(probabilities)),
            probabilities
        )
        # Select which exist job group to co-located with new job
        selected_ongoing_job = np.argmax(self.preferences, axis=0)[selected_app_group]
        #print("-----------App group to schedule next = {}".format(selected_app_group))
        print("-----------Ongoing group to schedule with = {}".format(selected_ongoing_job))
        print("-----------Preference matrix = {}".format(self.preferences[:,selected_app_group]))
        max_preference = -100
        selected_ongoing_job = -1
        for app in scheduled_apps:
            index = JobGroupData.groupIndexes[app.name]
            if self.preferences[:,selected_app_group][index] > max_preference:
                max_preference = self.preferences[:,selected_app_group][index]
                selected_ongoing_job = index
        print("-----------App group to schedule next = {}".format(selected_app_group))

        return selected_app_group, selected_ongoing_job

    def __action_probabilities(self, apps_index, concurrent_apps_index):
        exp = np.exp(self.preferences[apps_index])

        return (exp[:, concurrent_apps_index].T / exp.sum(axis=1)).T

    def normalized_action_probabilities(self, apps, apps_to_schedule, apps_weight=None):
        p = self.__action_probabilities(list(set(self.indices(apps))), list(set(self.indices(apps_to_schedule))))
        # if apps_weight is not None:
        #     p = (p.T * apps_weight).T
        p = p.sum(axis=0)
        return p / p.sum()

    @staticmethod
    def __choose(items, p):
        indices = np.arange(len(items))
        return items[np.random.choice(indices, p=p)]

    def save(self, folder):
        self._save(folder, "average", self.average)
        self._save(folder, "preferences", self.preferences)
        self._save(folder, "ucount", self.update_count)

    def load(self, folder):
        self.average = np.load("{}/average.npy".format(folder))
        self.update_count = np.load("{}/ucount.npy".format(folder))
        self.preferences = np.load("{}/preferences.npy".format(folder))

    def print(self):
        apps_name = list(self.reverse_index.values())
        print(tabulate(
            [
                ["Average"] + self.average.tolist(),
                ["Count"] + self.update_count.tolist(),
            ],
            apps_name,
            tablefmt='pipe'
        ))

        rows = []
        headers = ["Preferences"] + apps_name
        for i, name in self.reverse_index.items():
            rows.append([name] + self.preferences[i].tolist())

        print(tabulate(rows, headers, tablefmt='pipe'))
