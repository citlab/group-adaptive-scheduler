import argparse
import sys
import generator
import scheduler
import complementarity
import subprocess
from application import Application
from scheduler import Scheduler
from datetime import datetime


def run(args):
    scheduler_class = getattr(scheduler, args.scheduler)
    estimation_class = getattr(complementarity, args.estimation)
    Scheduler.jobs_to_peek_arg = args.jobs_to_peek
    s = generator.scheduler(
        scheduler_class=scheduler_class,
        estimation_class=estimation_class,
        exp_xml_str=args.experiment_xml.read(),
        jobs_xml_str=args.jobs_xml.read(),
        config_yaml=args.config_yaml
    )
    Application.print_command_line = args.pcmd
    Application.experiment_name = "experiment_" + datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + args.experiment_name
    print("Experiment folder = {}".format(Application.experiment_name))
    subprocess.Popen("mkdir /data/vinh.tran/new/expData/{}".format(Application.experiment_name), shell=True)

    #Scheduler.jobs_to_peek_arg = args.jobs_to_peek

    if args.estimation_parameters is not None:
        s.estimation.load(args.estimation_parameters)

    if args.estimation_folder is not None:
        s.estimation.output_folder = args.estimation_folder

    s.start()


def gen(args):
    exp = generator.experiment(args.jobs_xml.read(), args.n_jobs)
    args.output.write(exp.to_xml())


def estimation_bench(args):
    s = generator.estimations_bench(
        exp_xml_str=args.experiment_xml.read(),
        jobs_xml_str=args.jobs_xml.read(),
        config_yaml=args.config_yaml
    )
    s.start()

parser = argparse.ArgumentParser(
    prog="pyScheduler",
    description="Schedule Application on a Cluster"
)
subparsers = parser.add_subparsers()
parser_run = subparsers.add_parser("run", help="Run an experiment")
parser_run.set_defaults(func=run)
parser_estimations = subparsers.add_parser("estimations", help="Print the different estimations for an experiment")
parser_estimations.set_defaults(func=estimation_bench)
parser_gen = subparsers.add_parser("gen", help="Generate an experiment from jobs list")
parser_gen.set_defaults(func=gen)

# RUN
parser_run.add_argument(
    "config_yaml",
    metavar="config.yaml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the config.yaml"
)

parser_run.add_argument(
    "jobs_xml",
    metavar="jobs.xml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the jobs.xml"
)

parser_run.add_argument(
    "experiment_xml",
    metavar="exp.xml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the experiment.xml"
)

parser_run.add_argument(
    "-s",
    dest="scheduler",
    type=str,
    nargs="?",
    help="scheduling strategy",
    default="RoundRobin",
    choices=["RoundRobin", "Adaptive", "Random", "GroupAdaptive"]
)

parser_run.add_argument(
    "-e",
    dest="estimation",
    type=str,
    nargs="?",
    help="complementarity estimation strategy",
    default="Gradient",
    choices=["EpsilonGreedy", "Gradient", "GroupGradient"]
)

parser_run.add_argument(
    "-ep",
    dest="estimation_parameters",
    type=str,
    nargs="?",
    help="complementarity estimation parameters folder",
)

parser_run.add_argument(
    "-eo",
    dest="estimation_folder",
    type=str,
    nargs="?",
    help="estimation data folder",
    default="estimation"
)

parser_run.add_argument(
    "-en",
    dest="experiment_name",
    type=str,
    nargs="?",
    help="experiment name",
    default="first_roundrobin"
)

parser_run.add_argument(
    "-jtp",
    dest="jobs_to_peek",
    type=int,
    nargs="?",
    help="number of jobs for scheduler to peek",
    default=7
)

parser_run.add_argument(
    "--pcmd",
    help="Print or not command lines",
    action='store_true'
)

# ESTIMATION BENCH
parser_estimations.add_argument(
    "config_yaml",
    metavar="config.yaml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the config.yaml"
)

parser_estimations.add_argument(
    "jobs_xml",
    metavar="jobs.xml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the jobs.xml"
)

parser_estimations.add_argument(
    "experiment_xml",
    metavar="exp.xml",
    type=argparse.FileType('r'),
    nargs="?",
    help="path to the experiment.xml"
)

# GEN
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
