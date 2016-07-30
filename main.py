import argparse
import sys
import generator
import scheduler
import complementarity


def run(args):
    scheduler_class = getattr(scheduler, args.scheduler)
    estimation_class = getattr(complementarity, args.estimation)
    s = generator.scheduler(
        scheduler_class=scheduler_class,
        estimation_class=estimation_class,
        exp_xml=args.experiment_xml,
        jobs_xml=args.jobs_xml,
        config_yaml=args.config_yaml
    )
    s.start()


def gen(args):
    exp = generator.experiment(args.jobs_xml.read(), args.n_jobs)
    args.output.write(exp.to_xml())


parser = argparse.ArgumentParser(
    prog="pyScheduler",
    description="Schedule Application on a Cluster"
)
subparsers = parser.add_subparsers()
parser_run = subparsers.add_parser("run", help="Run an experiment")
parser_run.set_defaults(func=run)
parser_gen = subparsers.add_parser("gen", help="Generate an experiment from jobs list")
parser_gen.set_defaults(func=gen)

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
    choices=["RoundRobin"]
)

parser_run.add_argument(
    "-e",
    dest="estimation",
    type=str,
    nargs="?",
    help="complementarity estimation strategy",
    default="EpsilonGreedy",
    choices=["EpsilonGreedy", "Gradient"]
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
