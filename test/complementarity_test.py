from complementarity import *
import numpy as np

jobs = np.array([
    Job(0),
    Job(1),
    Job(2),
])


class TestIncrementalEstimation:
    def test_update_job(self):
        estimation = EpsilonGreedyEstimation(jobs, initial_average=1)

        estimation.update_job(jobs[0], jobs[[1, 2]], 5)
        expected_data = np.array([
            [1, 3, 3],
            [1, 1, 1],
            [1, 1, 1]
        ])

        assert expected_data.tolist() == estimation.average.tolist()

        estimation.update_job(jobs[0], jobs[[1, 2]], 7)
        expected_data = np.array([
            [1, 13/3, 13/3],
            [1, 1, 1],
            [1, 1, 1]
        ])

        assert np.allclose(expected_data, estimation.average)

    def test_expected_rate(self):
        estimation = EpsilonGreedyEstimation(jobs, initial_average=1)

        estimation.average = np.array([
            [1, 3, 3],
            [1, 4, 1],
            [5, 1, 1]
        ])

        expected_result = [1, 4]

        assert expected_result == estimation.expected_rates(jobs[1], jobs[[0, 1]]).tolist()


class TestGradientEstimation:
    def test_action_probability(self):
        estimation = GradientEstimation(jobs)

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

        assert np.allclose(expected_result, estimation.normalized_action_probabilities(jobs[0], jobs[[1, 2]]))

    def test_update_job(self):
        estimation = GradientEstimation(jobs, alpha=0.1, initial_average=1.)

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

        estimation.update_job(jobs[0], jobs[[1]], 2.)

        assert np.allclose(expected_preferences, estimation.preferences)
