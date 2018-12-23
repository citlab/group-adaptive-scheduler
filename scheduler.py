from abc import ABCMeta, abstractmethod
from sklearn.cross_validation import LeaveOneOut
from cluster import Cluster, Node
from application import Application
from complementarity import ComplementarityEstimation
from job_group_data import JobGroupData
from repeated_timer import RepeatedTimer
from threading import Lock
from typing import List
import time
import numpy as np


class NoApplicationCanBeScheduled(BaseException):
    pass


class Scheduler(metaclass=ABCMeta):
    def __init__(self, estimation: ComplementarityEstimation, cluster: Cluster, update_interval=60):
        self.queue = []
        self.estimation = estimation
        self.cluster = cluster
        self._timer = RepeatedTimer(update_interval, self.update_estimation)
        self.scheduler_lock = Lock()
        self.started_at = None
        self.stopped_at = None
        self.print_estimation = False

    def start(self):
        self.schedule()
        self._timer.start()
        self.started_at = time.time()

    def stop(self):
        self._timer.cancel()
        self.stopped_at = time.time()

    def update_estimation(self):
        for (apps, usage) in self.cluster.apps_usage():
            if len(apps) > 0 and usage.is_not_idle():
                for rest, out in LeaveOneOut(len(apps)):
                    self.estimation.update_app(apps[out[0]], [apps[i] for i in rest], usage.rate())
        if self.print_estimation:
            self.estimation.print()

    def add(self, app: Application):
        self.queue.append(app)

    def add_all(self, apps: List[Application]):
        self.queue.extend(apps)

    def schedule(self):
        while len(self.queue) > 0:
            try:
                app = self.schedule_application()
            except NoApplicationCanBeScheduled:
                print("No Application can be scheduled right now")
                break
            app.start(self.cluster.resource_manager, self._on_app_finished)
            time.sleep(1) # add a slight delay so jobs could be submitted to yarn in order
        self.cluster.print_nodes()

    def schedule_application(self) -> Application:
        app = self.get_application_to_schedule()
        if app.n_containers > self.cluster.available_containers():
            self.queue = [app] + self.queue
            raise NoApplicationCanBeScheduled

        self.place_containers(app)

        return app

    def _on_app_finished(self, app: Application):
        self.scheduler_lock.acquire()
        self.cluster.remove_applications(app)
        if len(self.queue) == 0 and self.cluster.has_application_scheduled() == 0:
            self.stop()
            self.on_stop()
        else:
            self.schedule()
        self.scheduler_lock.release()

    def on_stop(self):
        delta = self.stopped_at - self.started_at
        print("Queue took {:.0f}'{:.0f} to complete".format(delta // 60, delta % 60))
        self.estimation.save('estimation')

    def get_application_to_schedule(self) -> Application:
        app = self.queue[0]
        if app.n_containers > self.cluster.available_containers():
            raise NoApplicationCanBeScheduled
        return self.queue.pop(0)

    @abstractmethod
    def place_containers(self, app: Application):
        pass

    def _place_random(self, app: Application, n_containers=4):
        nodes = self.cluster.non_full_nodes()
        good_nodes = [
            n for n in nodes
            if len(n.applications()) == 0 or n.applications()[0] != app
        ]
        if len(good_nodes) == 0:
            good_nodes = nodes
        node = good_nodes[np.random.randint(0, len(good_nodes))]
        return self._place(app, node, n_containers)

    @staticmethod
    def _place(app: Application, node: Node, n_containers=4):
        if n_containers <= 0:
            raise ValueError("Can not place {} containers".format(n_containers))
        # print("Place {} on {} ({})".format(app, node, node.available_containers()))

        n = len([t for t in app.tasks if t.node is not None])
        n += 1 if app.node is not None else 0

        for k in range(n, n + n_containers):
            if k < app.n_containers:
                node.add_container(app.containers[k])
                print("Place a task of {} on node {}".format(app, node))

        return k - n + 1


class Random(Scheduler):
    def place_containers(self, app):
        n_containers_scheduled = 0

        while n_containers_scheduled < app.n_containers:
            n_containers_scheduled += self._place_random(app)


class EstimationBenchmark(Random):
    def __init__(self, estimations: List[ComplementarityEstimation], **kwargs):
        super().__init__(estimation=estimations[0], **kwargs)
        self.estimations = estimations

    def update_estimation(self):
        for (apps, usage) in self.cluster.apps_usage():
            if len(apps) > 0 and usage.is_not_idle():
                for rest, out in LeaveOneOut(len(apps)):
                    for estimation in self.estimations:
                        estimation.update_app(apps[out[0]], [apps[i] for i in rest], usage.rate())
        for estimation in self.estimations:
            print(str(estimation))
            estimation.print()

    def on_stop(self):
        delta = self.stopped_at - self.started_at
        print("Queue took {:.0f}'{:.0f} to complete".format(delta // 60, delta % 60))
        for estimation in self.estimations:
            estimation.save(str(estimation))


class RoundRobin(Scheduler):
    def place_containers(self, app: Application):
        empty_nodes = self.cluster.empty_nodes()

        n_containers_scheduled = 0
        print("App {} requires {} containers".format(app, app.n_containers))
        while len(empty_nodes) > 0 and n_containers_scheduled < app.n_containers:
            n_containers_scheduled += self._place(app, empty_nodes.pop())

        while n_containers_scheduled < app.n_containers:
            n_containers_scheduled += self._place_random(app)


class Adaptive(RoundRobin):
    def __init__(self, jobs_to_peek=5, **kwargs):
        super().__init__(**kwargs)
        self.jobs_to_peek = jobs_to_peek
        self.print_estimation = True

    def get_application_to_schedule(self):
        scheduled_apps, scheduled_apps_weight = self.cluster.applications(by_name=True)
        available_containers = self.cluster.available_containers()
        index = list(range(min(self.jobs_to_peek, len(self.queue))))

        while len(index) > 0:
            best_i = self.estimation.best_app_index(
                scheduled_apps,
                [self.queue[i] for i in index],
                scheduled_apps_weight
            )

            best_app = self.queue[best_i]

            if best_app.n_containers <= available_containers:
                print("Best app is {} ({}) of queue {}".format(
                    best_app.name,
                    best_i,
                    ",".join([self.queue[i].name for i in index])
                ))
                return self.queue.pop(best_i)

            index.pop(best_i)

        raise NoApplicationCanBeScheduled


class GroupAdaptive(RoundRobin):
    def __init__(self, jobs_to_peek=10, **kwargs):
        super().__init__(**kwargs)
        self.jobs_to_peek = jobs_to_peek
        self.print_estimation = True

    def schedule_application(self) -> Application:
        print("GroupAdaptive-schedule_application()")
        if self.cluster.available_containers()==0:
            raise NoApplicationCanBeScheduled
        app, existing_group = self.get_application_to_schedule()
        print("Marking self.get_app_to_schedule()")
        if app.n_containers > self.cluster.available_containers():
            self.queue = [app] + self.queue
            raise NoApplicationCanBeScheduled

        self.place_containers_with_group(app, existing_group)

        return app

    def place_containers_with_group(self, app: Application, existing_group):
        print("App {} requires {} containers".format(app, app.n_containers))

        if existing_group == -1:
            print("No preferred group to schedule with, check if can schedule on slot 1")
            chosen_slot = JobGroupData.SLOT_1
            if self.cluster.has_application_running():
                print("There are already job running, scheduling on slot 2")
                chosen_slot = JobGroupData.SLOT_2
            app.cluster_slot = chosen_slot
            for address,node in self.cluster.nodes.items():
                if JobGroupData.cluster_slots_index[address] == chosen_slot:
                    self._place(app, node, 4)
        else:
            print("The chosen existing group to co-locate is: {}".format(existing_group))
            co_located_app = None
            running_apps, running_apps_weight = self.cluster.applications(with_full_nodes=False, by_name=True)
            #print(running_apps.__str__())
            for running_app in running_apps:
                if JobGroupData.groupIndexes[running_app.name] == existing_group:
                    print("Choose app {} of group {} to co-locate".format(running_app.name, existing_group))
                    co_located_app = running_app
                    break
            if co_located_app is not None:
                print("The chosen slot to place new job is {}".format(co_located_app.cluster_slot))
                app.cluster_slot = co_located_app.cluster_slot
                for address, node in self.cluster.nodes.items():
                    #print(co_located_app.nodes)
                    #print(address)
                    if address in co_located_app.nodes:
                        self._place(app, node, 4)

        # n_containers_scheduled = 0
        # print("App {} requires {} containers".format(app, app.n_containers))
        # while len(empty_nodes) > 0 and n_containers_scheduled < app.n_containers:
        #     n_containers_scheduled += self._place(app, empty_nodes.pop())
        #
        # while n_containers_scheduled < app.n_containers:
        #     n_containers_scheduled += self._place_random(app)

    def get_application_to_schedule(self):
        global best_i
        scheduled_apps, scheduled_apps_weight = self.cluster.applications(with_full_nodes=False, by_name=True)
        #for app in scheduled_apps:
        #    print(app.__str__())
        available_containers = self.cluster.available_containers()
        index = list(range(min(self.jobs_to_peek, len(self.queue))))
        best_app = None

        while len(index) > 0:
            best_group_to_schedule, best_group_existing = self.estimation.best_app_index(
                scheduled_apps,
                [self.queue[i] for i in index],
                scheduled_apps_weight
            )

            if best_group_to_schedule == -1:
                print("No app is scheduling, pick randomly")
                best_app = self.queue.pop(np.random.randint(0, len(index)))
                print("Choose randomly app {} to schedule".format(best_app.name))
                return best_app, best_group_existing
            else:
                # Pick app from the best group to schedule
                print("Queue to consider: {}".format(",".join([self.queue[i].name for i in index])))
                print("Best app group to schedule: {}".format(best_group_to_schedule))
                print("Best app group existing: {}".format(best_group_existing))
                # print("Index = {}".format(index))
                list_best_jobs_indexes = []
                for i in index:
                    print("Job {} index = {}".format(self.queue[i].name, JobGroupData.groupIndexes[self.queue[i].name]))
                    if JobGroupData.groupIndexes[self.queue[i].name] == best_group_to_schedule:
                        print("Add job {} to list of best apps to choose from best group".format(self.queue[i].name))
                        list_best_jobs_indexes.append(i)
                best_i = list_best_jobs_indexes[np.random.randint(0, len(list_best_jobs_indexes))]
                best_app = self.queue[best_i]
                # print("Best app group to schedule: {}".format(best_group_to_schedule))
                # print("Best app group existing: {}".format(best_group_existing))
                print("Best app is {} ({}) of queue {}".format(
                    best_app.name,
                    best_group_to_schedule,
                    ",".join([self.queue[i].name for i in index])
                ))
                #print("Best app n_containers = {} | available_containers = {}".format(best_app.n_containers,
                #                                                                      available_containers))
            if best_app is None:
                raise NoApplicationCanBeScheduled

            if best_app.n_containers <= available_containers:
                #print("Best app group to schedule: {}".format(best_group_to_schedule))
                #print("Best app group existing: {}".format(best_group_existing))
                #print("Best app is {} ({}) of queue {}".format(
                #    best_app.name,
                #    best_group_to_schedule,
                #    ",".join([self.queue[i].name for i in index])
                #))
                return self.queue.pop(best_i), best_group_existing

            index.pop(best_i)

        raise NoApplicationCanBeScheduled
