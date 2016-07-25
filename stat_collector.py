from influxdb import InfluxDBClient as BaseInfluxDBClient
from abc import ABCMeta, abstractmethod
import numpy as np
import typing


class Server:
    def __init__(self, address):
        self.address = address


class StatCollector(metaclass=ABCMeta):
    # numpy array of shape (len(servers), len(resources)) is expected
    @abstractmethod
    def mean_usage(self, servers: typing.List[Server], time_interval: int = 60) -> np.ndarray:
        pass


class DummyStatCollector(StatCollector):
    def mean_usage(self, servers, time_interval=60):
        return np.ones((len(servers), 3))


class InfluxDBClient(StatCollector):
    def __init__(self, host, port, username, password, db):
        self.client = BaseInfluxDBClient(
            host=host,
            port=port,
            username=username,
            password=password,
            database=db
        )

    def mean_usage(self, servers, time_interval=60):
        mem = self._mean_query("used_percent", "mem", time_interval, servers)
        cpu = self._mean_query("usage_nice", "cpu", time_interval, servers)

        results = np.zeros((len(servers), 2))
        for i, server in enumerate(servers):
            results[i, 0] = next(mem.get_points(tags={'host': server.address})).get('mean')
            results[i, 1] = next(cpu.get_points(tags={'host': server.address})).get('mean')

        return results

    def _mean_query(self, field, measurement, time_in_sec, servers):
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
            hosts="|".join([s.address for s in servers])
        ))
