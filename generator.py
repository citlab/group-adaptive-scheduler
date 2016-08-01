import yaml
import stat_collector
import resource_manager
from cluster import Cluster
import numpy as np
from yarn_workloader import Jobs, Experiment


def cluster(yaml_source):
    config = yaml.load(yaml_source)
    rm = getattr(resource_manager, config['resource_manager']['type'])(
        **config['resource_manager'].get('kwargs', {})
    )
    stat = getattr(stat_collector, config['stat_collector']['type'])(
        **config['stat_collector'].get('kwargs', {})
    )
    stat_collector.Server.disk_max = config['server']['disk_max']
    stat_collector.Server.net_max = config['server']['net_max']
    stat_collector.Server.disk_name = config['server']['disk_name']
    stat_collector.Server.net_interface = config['server']['net_interface']

    return Cluster(rm, stat)


def experiment(jobs_xml, n_jobs):
    jobs = Jobs(jobs_xml)
    n = len(jobs)
    app_names = jobs.names()

    applications = []
    for i in range(n_jobs):
        applications.append(
            jobs[app_names[np.random.randint(0, n)]]
        )

    return Experiment(applications=applications)


def scheduler(scheduler_class, estimation_class, exp_xml, jobs_xml, config_yaml, estimation_kwargs=None):
    jobs = Jobs(jobs_xml)
    exp = Experiment(exp_xml, jobs=jobs)

    _scheduler = scheduler_class(
        estimation=estimation_class(jobs.applications(), **({} if estimation_kwargs is None else estimation_kwargs)),
        cluster=cluster(config_yaml)
    )
    _scheduler.add_all(exp.applications)

    return _scheduler
