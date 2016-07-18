import xml.etree.ElementTree as ET
from complementarity import Complementarity
from xml.dom import minidom


def generate_experiment(path, jobs):
    suite = ET.Element('suite')
    experiment = ET.SubElement(suite, 'experiment')
    experiment.set('name', 'generated_experiment')
    for job in jobs:
        j = ET.SubElement(experiment, 'job')
        j.set('name', job.name)
        j.text = '0'
    xmlstr = minidom.parseString(ET.tostring(suite, 'utf-8')).toprettyxml(indent="   ")
    with open(path, "w", encoding='utf-8') as f:
        f.write(xmlstr)


def import_rates(path, jobs, complementarity: Complementarity, rate_transform=lambda x: x[0]):
    jobs_map = {}
    for j in jobs:
        jobs_map[j.name] = j

    with open(path) as f:
        for line in f:
            jobs_name, rate = line.split(':')
            scheduled_jobs = [jobs_map[name] for name in jobs_name.strip().split(' ')]
            rate = rate_transform(list(map(float, rate.strip().split(' '))))
            print(scheduled_jobs)
            complementarity.update(scheduled_jobs, rate)
