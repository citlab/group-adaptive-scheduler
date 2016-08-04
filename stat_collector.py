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

    def __str__(self):
        return self.address


class Usage:
    def __init__(self, cpu, io_wait, dsk_read, dsk_write, net_recv, net_sent):
        self.cpu = cpu
        self.io_wait = io_wait
        self.dsk_read = dsk_read
        self.dsk_write = dsk_write
        self.net_recv = net_recv
        self.net_sent = net_sent

    def rate(self) -> float:
        dsk = np.tanh(self.dsk_read + self.dsk_write)
        net = np.tanh(self.net_recv + self.net_sent)
        r = self.cpu + (dsk + net) * np.exp(- 5 * self.io_wait)
        return np.exp(1 + r)

    def is_not_idle(self):
        return self.cpu > 0.05 or self.io_wait > 0.05


class StatCollector(metaclass=ABCMeta):
    @abstractmethod
    def mean_usage(self, servers: Dict[str, Server], time_interval: int = 60) -> Dict[str, Usage]:
        pass


class DummyStatCollector(StatCollector):
    def mean_usage(self, servers, time_interval=60):
        return {
            k: Usage(1, 1, 1, 1, 1, 1) for k, _ in servers.items()
        }


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
            results[address] = Usage(
                cpu=cpu[address]['cpu'] / 100,
                io_wait=cpu[address]['io_wait'] / 100,
                dsk_read=disk[address]['read'] / server.disk_max,
                dsk_write=disk[address]['write'] / server.disk_max,
                net_recv=net[address]['recv'] / server.net_max,
                net_sent=net[address]['sent'] / server.net_max,
            )

        return results

    def _cpu(self, time_in_sec, servers: Dict[str, Server]):
        query_template = """
            SELECT usage_user, usage_iowait
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
            cpu[address] = {
                'cpu': self._mean(data.get_points(tags={'host': address}), 'usage_user'),
                'io_wait': self._mean(data.get_points(tags={'host': address}), 'usage_iowait')
            }

        return cpu

    def _disk(self, time_in_sec, servers: Dict[str, Server]):
        query_template = """
            SELECT derivative(read_bytes, 1s) as dsk_read, derivative(write_bytes, 1s) as dsk_write
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
        Mo = 1024 ** 2
        for address in servers.keys():
            disk[address] = {
                'read': self._mean(
                    data.get_points(tags={'host': address}),
                    'dsk_read',
                    Server.disk_max * Mo
                ) / Mo,
                'write': self._mean(
                    data.get_points(tags={'host': address}),
                    'dsk_write',
                    Server.disk_max * Mo
                ) / Mo,
            }

        return disk

    def _net(self, time_in_sec, servers: Dict[str, Server]):
        query_template = """
            SELECT derivative(bytes_recv, 1s) as net_recv, derivative(bytes_sent, 1s) as net_sent
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
        Mo = 1024 ** 2
        for address in servers.keys():
            net[address] = {
                'recv': self._mean(
                    data.get_points(tags={'host': address}),
                    'net_recv',
                    Server.net_max * Mo
                ) / Mo,
                'sent': self._mean(
                    data.get_points(tags={'host': address}),
                    'net_sent',
                    Server.net_max * Mo
                ) / Mo,
            }

        return net

    @staticmethod
    def _mean(points, key, p_max=100.):
        points_sum = 0
        n = 0
        for p in points:
            if p[key] is not None:
                n += 1
                points_sum += p[key]
                if p[key] > p_max:
                    print("/!\\ Max for {} exceed by {:.2%}".format(key, p[key] / p_max))
        if n == 0:
            return 0
        return points_sum / n
