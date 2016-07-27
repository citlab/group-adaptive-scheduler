import argparse
import sys
from yarn_workloader import Jobs, Experiment
import numpy as np


def run(args):
    print(args)


def gen(args):
    jobs = Jobs(xml_str=args.jobs_xml.read())
    n = len(jobs)
    app_names = jobs.names()

    applications = []
    for i in range(args.n_jobs):
        applications.append(
            jobs[app_names[np.random.randint(0, n)]]
        )

    exp = Experiment(applications=applications)
    args.output.write(exp.to_xml())


parser = argparse.ArgumentParser(
    prog="pyScheduler",
    description="Schedule Application on a Cluster"
)
subparsers = parser.add_subparsers()
parser_run = subparsers.add_parser("run", help="Run an experiment")
parser_run.set_defaults(func=run)
parser_gen = subparsers.add_parser("gen", help="Generate experiment")
parser_gen.set_defaults(func=gen)

parser_run.add_argument(
    "experiment_xml",
    metavar="exp.xml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the experiment.xml"
)

parser_run.add_argument(
    "jobs_xml",
    metavar="jobs.xml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the jobs.xml"
)

parser_run.add_argument(
    "-rm",
    dest="resource_manager",
    type=str,
    nargs="?",
    help="address(:port) of the Resource Manager server",
    required=True
)

parser_run.add_argument(
    "-db",
    dest="stats_collector",
    type=str,
    nargs="?",
    help="address(:port) of the InfluxDB server",
    required=True
)

parser_gen.add_argument(
    "jobs_xml",
    metavar="jobs.xml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the jobs.xml"
)

parser_gen.add_argument(
    "-n",
    dest="n_jobs",
    type=int,
    nargs="?",
    help="number of jobs in the experiment",
    default=10
)

parser_gen.add_argument(
    "-o",
    dest="output",
    type=argparse.FileType('w+'),
    nargs="?",
    help="output path",
    default="experiment.xml"
)

if len(sys.argv) == 1:
    parser.print_help()
    sys.exit(1)

args = parser.parse_args()

args.func(args)
