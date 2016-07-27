from influxdb import InfluxDBClient as BaseInfluxDBClient
from abc import ABCMeta, abstractmethod
import numpy as np
from typing import Dict


class Server:
    def __init__(self, address):
        self.address = address


class StatCollector(metaclass=ABCMeta):
    @abstractmethod
    def mean_usage(self, servers: Dict[str, Server], time_interval: int = 60) -> Dict[str, np.ndarray]:
        pass


class DummyStatCollector(StatCollector):
    def mean_usage(self, servers, time_interval=60):
        return {k: np.ones(3) for k, _ in servers.items()}


class InfluxDBClient(StatCollector):
    def __init__(self, host, port=8086, username="root", password="root", db="telegraf"):
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

        results = {}
        for address, server in servers.items():
            result = np.zeros(3)
            result[0] = next(mem.get_points(tags={'host': server.address})).get('mean')
            result[1] = next(cpu.get_points(tags={'host': server.address})).get('mean')
            results[address] = result

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