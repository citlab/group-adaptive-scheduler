from influxdb import InfluxDBClient
from abc import ABCMeta, abstractmethod
import numpy as np
from typing import Dict


class Server:
    disk_max = 1e3
    net_max = 1e3
    net_interface = ''
    disk_name = ''

    def __init__(self, address):
        self.address = address


class StatCollector(metaclass=ABCMeta):
    @abstractmethod
    def mean_usage(self, servers: Dict[str, Server], time_interval: int = 60) -> Dict[str, np.ndarray]:
        pass


class DummyStatCollector(StatCollector):
    def mean_usage(self, servers, time_interval=60):
        return {k: np.ones(3) for k, _ in servers.items()}


class InfluxDB(StatCollector):
    time_format = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, address, port=8086, username="root", password="root", db="telegraf"):
        self.client = InfluxDBClient(
            host=address,
            port=port,
            username=username,
            password=password,
            database=db
        )

    def mean_usage(self, servers: Dict[str, Server], time_interval=60):
        cpu = self._cpu(time_interval, servers)
        disk = self._disk(time_interval, servers)
        net = self._net(time_interval, servers)

        results = {}
        for address, server in servers.items():
            results[address] = [
                cpu[address],
                disk[address] / server.disk_max,
                net[address] / server.net_max,
            ]

        return results

    def _cpu(self, time_in_sec, servers: Dict[str, Server]):
        query_template = """
            SELECT usage_user
            FROM cpu
            WHERE time > now() - {time_in_sec}s
            AND host =~ /^({hosts})$/
            AND cpu = 'cpu-total'
            GROUP BY host
        """
        data = self.client.query(query_template.format(
            time_in_sec=int(time_in_sec),
            hosts="|".join([address for address in servers.keys()])
        ))

        cpu = {}
        for address in servers.keys():
            cpu[address] = self._mean(data.get_points(tags={'host': address}), 'usage_user')

        return cpu

    def _disk(self, time_in_sec, servers: Dict[str, Server]):
        query_template = """
            SELECT derivative(read_bytes, 1s)
            FROM diskio
            WHERE time > now() - {time_in_sec}s
            AND "name" = '{disk_name}'
            AND host =~ /^({hosts})$/
            GROUP BY host
        """
        data = self.client.query(query_template.format(
            time_in_sec=int(time_in_sec),
            disk_name=Server.disk_name,
            hosts="|".join([address for address in servers.keys()]),
        ))

        disk = {}
        for address in servers.keys():
            disk[address] = self._mean(data.get_points(tags={'host': address}), 'derivative') / 1024 ** 2

        return disk

    def _net(self, time_in_sec, servers: Dict[str, Server]):
        query_template = """
            SELECT derivative(bytes_recv, 1s)
            FROM net
            WHERE time > now() - {time_in_sec}s
            AND interface = '{net_interface}'
            AND host =~ /^({hosts})$/
            GROUP BY host
        """
        data = self.client.query(query_template.format(
            time_in_sec=int(time_in_sec),
            net_interface=Server.net_interface,
            hosts="|".join([address for address in servers.keys()])
        ))

        net = {}
        for address in servers.keys():
            net[address] = self._mean(data.get_points(tags={'host': address}), 'derivative') / 1024 ** 2

        return net

    @staticmethod
    def _mean(points, key):
        points_sum = 0
        n = 0
        for p in points:
            n += 1
            points_sum += p[key]

        return points_sum / n
