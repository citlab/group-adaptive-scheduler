from xml.dom import minidom
import xml.etree.ElementTree as ET
from application import Application, FlinkApplication


class Experiment:
    def __init__(self, applications=None, name="generated_experiment", path=None, jobs_path=None):
        self.name = name
        self.applications = [] if None else applications

        if path is not None and jobs_path is not None:
            with open(path, 'r') as exp, open(jobs_path, 'r') as jobs:
                self.read(exp.read(), jobs.read())

    def read(self, xml_str, jobs_xml_str):
        experiment = ET.fromstring(xml_str).find('experiment')
        self.name = experiment.get('name', self.name)

        self.applications = []
        jobs = Jobs()
        jobs.read(jobs_xml_str)

        for job in experiment.iter('job'):
            self.applications.append(jobs[job.get('name')])

    def to_xml(self):
        suite = ET.Element('suite')
        experiment = ET.SubElement(suite, 'experiment')
        experiment.set('name', self.name)
        for job in self.applications:
            j = ET.SubElement(experiment, 'job')
            j.set('name', job.name)
            j.text = '0'
        return minidom.parseString(ET.tostring(suite, 'utf-8')).toprettyxml(indent="   ")


class Jobs:
    def __init__(self, applications=None, path=None):
        self._data = {}

        if applications is not None:
            for app in applications:
                self._data[app.name] = applications

        if path is not None:
            with open(path, 'r') as f:
                self.read(f.read())

    def read(self, xml_str):
        jobs = ET.fromstring(xml_str)

        for job in jobs.iter('job'):
            app = xml_to_flink_application(job)
            self._data[app.name] = app

    def __getitem__(self, item) -> Application:
        return self._data[item].copy()


def xml_to_flink_application(job: ET.Element) -> FlinkApplication:
    name = job.get('name')
    n_task = 0

    for arg in job.find('runner/arguments').iter('argument'):
        if arg.get('name') == 'yn':
            n_task = int(arg.text)

    if n_task == 0:
        raise ValueError("runner/arguments/argument with name = yn was not found")

    jar = job.find('jar/path').text.strip()
    args = []
    for arg in job.find('jar/arguments').iter('argument'):
        args.append("{} {}".format(arg.get('name', ''), arg.text).strip())

    return FlinkApplication(name, n_task, jar, args)
