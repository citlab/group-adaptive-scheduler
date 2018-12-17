import argparse
import sys
import generator
import scheduler
import complementarity
from application import Application


def run(args):
    scheduler_class = getattr(scheduler, args.scheduler)
    estimation_class = getattr(complementarity, args.estimation)
    s = generator.scheduler(
        scheduler_class=scheduler_class,
        estimation_class=estimation_class,
        exp_xml_str=args.experiment_xml.read(),
        jobs_xml_str=args.jobs_xml.read(),
        config_yaml=args.config_yaml
    )
    Application.print_command_line = args.pcmd

    if args.estimation_parameters is not None:
        s.estimation.load(args.estimation_parameters)

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
