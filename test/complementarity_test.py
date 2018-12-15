from complementarity import *
from application import DummyApplication
import numpy as np

jobs = np.array([
    DummyApplication("TPCH21", 0),
    DummyApplication("SVM", 1),
    DummyApplication("LogisticRegression", 2),
    DummyApplication("KMeans", 3),
    DummyApplication("WordCount", 4)
])


class TestIncrementalEstimation:

    def run(self):
        self.test_update_job()

    def test_update_job(self):
        estimation = EpsilonGreedy(jobs, initial_average=1)

        estimation.update_app(jobs[0], jobs[[1, 2]], 5)
        expected_data = np.array([
            [1, 3, 3],
            [1, 1, 1],
            [1, 1, 1]
        ])

        assert expected_data.tolist() == estimation.average.tolist()

        estimation.update_app(jobs[0], jobs[[1, 2]], 7)
        expected_data = np.array([
            [1, 13 / 3, 13 / 3],
            [1, 1, 1],
            [1, 1, 1]
        ])

        assert np.allclose(expected_data, estimation.average)

    def test_expected_rate(self):
        estimation = EpsilonGreedy(jobs, initial_average=1)

        estimation.average = np.array([
            [1, 3, 3],
            [1, 4, 1],
            [5, 1, 1]
        ])

        expected_result = [1, 4]

        assert expected_result == estimation.expected_rates(jobs[1], jobs[[0, 1]]).tolist()


class TestGradientEstimation:

    def main(self):
        print("python main function")
        self.test_action_probability()

    def test_action_probability(self):
        estimation = GroupGradient(jobs)

        estimation.preferences = np.array([
            [0, 5, 1],
            [5, 0, 0],
            [1, 0, 0],
        ])

        e = np.exp(estimation.preferences[0])
        expected_result = np.array([
            e[1] / e.sum(),
            e[2] / e.sum()
        ])
        expected_result /= expected_result.sum()

        new_job_index, old_job_index = estimation.best_app_index([jobs[2], jobs[3], jobs[4]], [jobs[0], jobs[1]])

        assert np.allclose(expected_result, new_job_index)

    def test_update_job(self):
        estimation = GroupGradient(jobs, alpha=0.1, initial_average=1.)

        estimation.preferences = np.array([
            [0, 5, 1],
            [5, 0, 0],
            [1, 0, 0],
        ], dtype=np.dtype('float64'))

        e = np.exp(estimation.preferences[0])
        action_probabilities = e / e.sum()

        action_probabilities[1] = 1 - action_probabilities[1]
        delta = 0.1 * (2 - 1.5) * action_probabilities

        expected_preferences = estimation.preferences
        expected_preferences[0] += delta

        estimation.update_app(jobs[0], jobs[[1]], 2.)

        assert np.allclose(expected_preferences, estimation.preferences)


if __name__ == '__main__':
    TestGradientEstimation().main()
