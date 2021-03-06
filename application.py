import subprocess
import time
from typing import List
from threading import Thread

from job_group_data import JobGroupData
from resource_manager import ResourceManager
from abc import ABCMeta, abstractmethod
import uuid
import datetime


class NotCorrectlyScheduledError(Exception):
    pass


class Container(metaclass=ABCMeta):
    def __init__(self):
        self.container_id = None
        self.node = None
        self.pid = None
        # used to distinguish between the application master and the tasks container
        # the application master is considered to be negligible as it does not have
        # much influence on the resource usage
        self.is_negligible = False

    @property
    @abstractmethod
    def application(self):
        pass

    def __str__(self):
        return str(self.application)


class Application(Container):
    print_command_line = False
    experiment_name = ""

    def __init__(self, name, n_tasks, data_set=''):
        super().__init__()
        self.name = name
        self.n_tasks = n_tasks
        self.id = None
        self.is_running = False
        self.tasks = [Task(self) for i in range(self.n_tasks)]
        self.thread = None
        self.is_negligible = True
        self.n_containers = self.n_tasks
        self.containers = self.tasks
        self.data_set = data_set
        self.nodes = set()
        self.group = JobGroupData.groupIndexes[name]
        self.cluster_slot = JobGroupData.SLOT_FULL
        self.waiting_time = 0


    @property
    def application(self):
        return self

    def __str__(self):
        return "{} ({}) [{}]".format(self.id, self.name, self.waiting_time)

    def short_str(self):
        return "{} [{}]".format(self.name, self.waiting_time)

    def start(self, resource_manager: ResourceManager, on_finish=None, sleep_during_loop=5):
        self.id = resource_manager.next_application_id()
        print("Start Application {}".format(self))

        for task in self.tasks:
            if task.node is None:
                raise NotCorrectlyScheduledError(
                    "A task of the application {} is not scheduled on a node".format(self.name)
                )
            self.nodes.add(task.node.address)

        # print(self.nodes)
        print(datetime.datetime.utcnow().strftime('%Y-%m-%d"T"%H:%M:%S"Z"'))

        self.thread = Thread(target=self._run, args=[resource_manager, on_finish, sleep_during_loop])
        self.thread.start()

    def command_line(self) -> List[str]:
        return [""]

    def _run(self, resource_manager: ResourceManager, on_finish, sleep_during_loop):
        cmd = " ".join(self.command_line())
        #if self.print_command_line:
        print("Start {} with cmd: {}".format(self.id, cmd))
        subprocess.Popen(cmd, shell=True)

        self.start_at = datetime.datetime.utcnow()

        time.sleep(sleep_during_loop + 30)
        while not resource_manager.is_application_finished(self.id):
            if not self.is_running and resource_manager.is_application_running(self.id):
                self.is_running = True

            time.sleep(sleep_during_loop)

        print("Application {} has finished".format(self))

        self.end_at = datetime.datetime.utcnow()

        host_list = "|".join([address for address in self.nodes])
        export_file_name = self.id + "_" + self.name

        cmd_query_cpu = "\nmkdir /data/vinh.tran/new/expData/{}/{}" \
                        "&& influx -precision rfc3339 -username root -password root" \
                        " -database 'telegraf' -host 'localhost' -execute 'SELECT usage_user,usage_iowait " \
                        "FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' AND host =~ /{}/  " \
                        "AND cpu = '\\''cpu-total'\\'' GROUP BY host' -format 'csv' > /data/vinh.tran/new/expData/{}/{}/cpu_{}.csv" \
            .format(self.experiment_name,
                    export_file_name,
                    self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    host_list,
                    self.experiment_name,
                    export_file_name,
                    self.name)
        print(cmd_query_cpu)
        # subprocess.Popen(cmd_query_cpu, shell=True)

        cmd_query_cpu_mean = "\ninflux -precision rfc3339 -username root -password root" \
                        " -database 'telegraf' -host 'localhost' -execute 'SELECT mean(usage_user) as \"mean_cpu_percent\",mean(usage_iowait) as \"mean_io_wait\" " \
                        "FROM \"telegraf\".\"autogen\".\"cpu\" WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' AND host =~ /{}/  " \
                        "AND cpu = '\\''cpu-total'\\'' GROUP BY time(10s)' -format 'csv' > /data/vinh.tran/new/expData/{}/{}/cpu_{}_mean.csv" \
            .format(self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    host_list,
                    self.experiment_name,
                    export_file_name,
                    self.name)
        print(cmd_query_cpu_mean)

        cmd_query_mem = "\ninflux -precision rfc3339 -username root -password root " \
                        "-database 'telegraf' -host 'localhost' -execute 'SELECT used_percent " \
                        "FROM \"telegraf\".\"autogen\".\"mem\" WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' AND host =~ /{}/  " \
                        "GROUP BY host' -format 'csv' > /data/vinh.tran/new/expData/{}/{}/mem_{}.csv" \
            .format(self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    host_list,
                    self.experiment_name,
                    export_file_name,
                    self.name)
        print(cmd_query_mem)

        cmd_query_mem_mean = "\ninflux -precision rfc3339 -username root -password root " \
                        "-database 'telegraf' -host 'localhost' -execute 'SELECT mean(used_percent) " \
                        "FROM \"telegraf\".\"autogen\".\"mem\" WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' AND host =~ /{}/  " \
                        "GROUP BY time(10s)' -format 'csv' > /data/vinh.tran/new/expData/{}/{}/mem_{}_mean.csv" \
            .format(self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    host_list,
                    self.experiment_name,
                    export_file_name,
                    self.name)
        print(cmd_query_mem_mean)

        cmd_query_disk = "\ninflux -precision rfc3339 -username root -password root " \
                        "-database 'telegraf' -host 'localhost' -execute 'SELECT sum(read_bytes),sum(write_bytes) " \
                        "FROM (SELECT derivative(last(\"read_bytes\"),1s) as \"read_bytes\",derivative(last(\"write_bytes\"),1s) as \"write_bytes\",derivative(last(\"io_time\"),1s) as \"io_time\" " \
                        "FROM \"telegraf\".\"autogen\".\"diskio\" WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' AND host =~ /{}/  " \
                        "GROUP BY \"host\",\"name\",time(10s)) WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' GROUP BY host,time(10s)' -format 'csv' > /data/vinh.tran/new/expData/{}/{}/disk_{}.csv" \
            .format(self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    host_list,
                    self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.experiment_name,
                    export_file_name,
                    self.name)
        print(cmd_query_disk)

        cmd_query_disk_mean = "\ninflux -precision rfc3339 -username root -password root " \
                         "-database 'telegraf' -host 'localhost' -execute 'SELECT sum(read_bytes),sum(write_bytes) " \
                         "FROM (SELECT derivative(last(\"read_bytes\"),1s) as \"read_bytes\",derivative(last(\"write_bytes\"),1s) as \"write_bytes\",derivative(last(\"io_time\"),1s) as \"io_time\" " \
                         "FROM \"telegraf\".\"autogen\".\"diskio\" WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' AND host =~ /{}/  " \
                         "GROUP BY \"host\",\"name\",time(10s)) WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' GROUP BY time(10s)' -format 'csv' > /data/vinh.tran/new/expData/{}/{}/disk_{}_mean.csv" \
            .format(self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    host_list,
                    self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.experiment_name,
                    export_file_name,
                    self.name)
        print(cmd_query_disk_mean)

        cmd_query_net = "\ninflux -precision rfc3339 -username root -password root " \
                         "-database 'telegraf' -host 'localhost' -execute 'SELECT sum(download_bytes),sum(upload_bytes) FROM (SELECT  derivative(first(\"bytes_recv\"),1s) " \
                         "as \"download_bytes\",derivative(first(\"bytes_sent\"),1s) as \"upload_bytes\"" \
                         "FROM \"telegraf\".\"autogen\".\"net\" WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' AND host =~ /{}/  " \
                         "GROUP BY \"host\",time(10s)) WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' GROUP BY host,time(10s)' -format 'csv' > /data/vinh.tran/new/expData/{}/{}/net_{}.csv" \
            .format(self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    host_list,
                    self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.experiment_name,
                    export_file_name,
                    self.name)
        print(cmd_query_net)

        cmd_query_net_mean = "\ninflux -precision rfc3339 -username root -password root " \
                        "-database 'telegraf' -host 'localhost' -execute 'SELECT sum(download_bytes),sum(upload_bytes) FROM (SELECT  derivative(first(\"bytes_recv\"),1s) " \
                        "as \"download_bytes\",derivative(first(\"bytes_sent\"),1s) as \"upload_bytes\"" \
                        "FROM \"telegraf\".\"autogen\".\"net\" WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' AND host =~ /{}/  " \
                        "GROUP BY \"host\",time(10s)) WHERE time > '\\''{}'\\'' and time < '\\''{}'\\'' GROUP BY time(10s)' -format 'csv' > /data/vinh.tran/new/expData/{}/{}/net_{}_mean.csv" \
            .format(self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    host_list,
                    self.start_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.end_at.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    self.experiment_name,
                    export_file_name,
                    self.name)
        print(cmd_query_net_mean)

        subprocess.Popen(cmd_query_cpu + " && " + cmd_query_mem + " && " + cmd_query_disk + " && " + cmd_query_net + " && "
                         + cmd_query_cpu_mean + " && " + cmd_query_mem_mean + " && " + cmd_query_disk_mean + " && " + cmd_query_net_mean, shell=True)

        time.sleep(1)

        with open("/data/vinh.tran/new/expData/{}/{}/cmd_{}.txt".format(self.experiment_name, export_file_name, self.name), 'a') as file:
            file.write("{}\n\n{}\n\n{}\n\n{}\n\n\n\n{}\n\n{}\n\n{}\n\n{}\n".
                       format(cmd_query_cpu, cmd_query_mem, cmd_query_disk, cmd_query_net,
                              cmd_query_cpu_mean, cmd_query_mem_mean, cmd_query_disk_mean, cmd_query_net_mean))

        if callable(on_finish):
            on_finish(self)

    def copy(self):
        return Application(self.name, len(self.tasks), data_set=self.data_set)

    def is_a_copy_of(self, application):
        return application.name == self.name and len(self.tasks) == len(application.tasks) \
               and self.data_set == application.data_set


class Task(Container):
    def __init__(self, application: Application):
        super().__init__()
        self.app = application

    @property
    def application(self):
        return self.app


class DummyApplication(Application):
    def __init__(self, name="app", n_tasks=8, id="id", is_running=False, data_set='1'):
        super().__init__(name, n_tasks, data_set=data_set)
        self.id = id
        self.is_running = is_running

    def command_line(self):
        return ["sleep", "0.1"]


class SparkApplication(Application):
    def __init__(self, name, n_task, jar, args, jar_class=None, tm=None, **kwargs):
        super().__init__(name, n_task, **kwargs)
        self.jar = jar
        self.jar_class = jar_class
        self.tm = tm
        self.args = args

    def command_line(self):
        cmd = [
            "$SPARK_HOME/bin/spark-submit",
            "--master yarn",
            "--deploy-mode cluster",
            "--num-executors {}".format(len(self.tasks)),
            "--name {}".format(self.name)
            #"--conf spark.yarn.executor.nodeLabelExpression=\"{}\"".format(self.cluster_slot)
            # "-ynm {}_{}".format(self.name, self.data_set),
            # "-yn {}".format(len(self.tasks)),
            # "-yD fix.container.hosts={tasks_host}".format(
            #     tasks_host=",".join(self.tasks_hosts()),
            # ),
            # "-yDfix.am.host={am_host}".format(
            #     am_host=self.node.address
            # )
        ]
        if self.cluster_slot is not JobGroupData.SLOT_FULL:
            cmd.append("--conf spark.yarn.executor.nodeLabelExpression=\"{}\"".format(self.cluster_slot))

        if self.tm is not None:
            cmd.append("-ytm {}".format(self.tm))

        if self.jar_class is not None:
            cmd.append("--class {}".format(self.jar_class))

        cmd.append(self.jar)

        for arg in self.args:
            if "TEMP" in arg:
                cmd.append(arg.replace('TEMP', 'hdfs:///tmp/' + str(uuid.uuid4()).replace('-', '')))
            elif 'DATASET' in arg:
                cmd.append(arg.replace('DATASET', self.data_set))
            else:
                cmd.append(arg)

        cmd.append("1> apps_log/{}.log".format(self.id + "_" + self.name))

        return cmd

    def tasks_hosts(self):
        hosts = []
        for task in self.tasks:
            hosts.append(task.node.address)

        return hosts

    def copy(self):
        return SparkApplication(
            self.name,
            len(self.tasks),
            self.jar,
            self.args,
            jar_class=self.jar_class,
            tm=self.tm,
            data_set=self.data_set
        )

    def is_a_copy_of(self, application):
        return super().is_a_copy_of(application) and self.jar == application.jar \
               and self.args == application.args and self.jar_class == application.jar_class \
               and self.tm == application.tm
