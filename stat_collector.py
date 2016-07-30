from influxdb import InfluxDBClient
from abc import ABCMeta, abstractmethod
import numpy as np
from typing import Dict
from datetime import datetime


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
            SELECT mean(usage_nice)
            FROM cpu
            WHERE time > now() - {time_in_sec}s
            AND host =~ /^({hosts})$/
            GROUP BY host
        """
        data = self.client.query(query_template.format(
            time_in_sec=int(time_in_sec),
            hosts="|".join([address for address in servers.keys()])
        ))

        cpu = {}
        for address in servers.keys():
            cpu[address] = next(data.get_points(tags={'host': address}))['mean']

        return cpu

    def _disk(self, time_in_sec, servers: Dict[str, Server]):
        query_template = """
            SELECT read_bytes, write_bytes
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
            disk[address] = self._mean_derivative(data.get_points(tags={'host': address}))

        return disk

    def _net(self, time_in_sec, servers: Dict[str, Server]):
        query_template = """
            SELECT bytes_recv, bytes_sent
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
            net[address] = self._mean_derivative(data.get_points(tags={'host': address}))

        return net

    def _mean_derivative(self, points):
        point_a = next(points)
        point_b = point_a
        for point_b in points:
            pass

        if point_b == point_a:
            return 0

        t_a = datetime.strptime(point_a['time'], self.time_format)
        t_b = datetime.strptime(point_b['time'], self.time_format)
        time = (t_b - t_a).total_seconds()

        a = 0
        b = 0
        for key in point_a.keys():
            if key != 'time':
                a += point_a[key]
                b += point_b[key]

        return (b - a) / time / 1e9


