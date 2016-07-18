from influxdb import InfluxDBClient as BaseInfluxDBClient
from abc import ABCMeta, abstractmethod
import numpy as np


class StatCollector(metaclass=ABCMeta):
    # numpy array of shape (len(nodes), len(resources)) is expected
    @abstractmethod
    def mean_usage(self, nodes, time_interval=60):
        pass

    # list of list of shape (len(nodes), len(pids)) is expected
    @abstractmethod
    def running_processes_pid(self, nodes, time_interval=60):
        pass

    @abstractmethod
    def get_pid(self, pattern, node):
        pass


class DummyStatCollector(StatCollector):
    def mean_usage(self, nodes, time_interval=60):
        return np.ones((len(nodes), 3))

    def running_processes_pid(self, nodes, time_interval=60):
        return [[i, i ** 2] for i, n in enumerate(nodes)]

    def get_pid(self, pattern, node):
        return node.name + pattern


class InfluxDBClient(StatCollector):
    def __init__(self, host, port, username, password, db):
        self.client = BaseInfluxDBClient(
            host=host,
            port=port,
            username=username,
            password=password,
            database=db
        )

    def mean_usage(self, nodes, time_interval=60):
        mem = self._mean_query("used_percent", "mem", time_interval, nodes)
        cpu = self._mean_query("usage_nice", "cpu", time_interval, nodes)

        results = np.zeros((len(nodes), 2))
        for i, node in enumerate(nodes):
            results[i, 0] = next(mem.get_points(tags={'host': node})).get('mean')
            results[i, 1] = next(cpu.get_points(tags={'host': node})).get('mean')

        return results

    def _mean_query(self, field, measurement, time_in_sec, nodes):
        query_template = """
            SELECT mean({field})
            FROM {measurement}
            WHERE time > now() - {time_in_sec}s
            AND host =~ /^({hosts})$/
            GROUP BY host
        """
        return self.client.query(query_template.format(
            field=field,
            measurement=measurement,
            time_in_sec=int(time_in_sec),
            hosts="|".join(nodes)
        ))

    def container_pid_running(self, nodes):
        pass

    def _pid_query(self, field, measurement, time_in_sec, nodes):
        query_template = """
            SELECT pid
            FROM procstat
            WHERE time > now() - {time_in_sec}s
            AND host =~ /^({hosts})$/
            GROUP BY host
        """
        return self.client.query(query_template.format(
            field=field,
            measurement=measurement,
            time_in_sec=int(time_in_sec),
            hosts="|".join(nodes)
        ))
