from xml.dom import minidom
import xml.etree.ElementTree as ET
from application import Application, FlinkApplication


class Jobs:
    def __init__(self, xml=None, applications=None):
        self._data = {}

        if applications is not None:
            for app in applications:
                self._data[app.name] = applications

        if xml is not None:
            self.__read(ET.parse(xml).getroot())

    def read(self, xml_str: str):
        self.__read(ET.fromstring(xml_str))

    def __read(self, jobs):
        for job in jobs.iter('job'):
            app = xml_to_flink_application(job)
            self._data[app.name] = app

    def __getitem__(self, item) -> Application:
        return self._data[item].copy()

    def __len__(self):
        return len(self._data)

    def names(self):
        return list(self._data.keys())

    def applications(self):
        return list(self._data.values())


def xml_to_flink_application(job: ET.Element) -> FlinkApplication:
    name = job.get('name')
    n_task = 0
    tm = None
    jar_class = None

    for arg in job.find('runner/arguments').iter('argument'):
        if arg.get('name') == 'yn':
            n_task = int(arg.text)
        if arg.get('name') == 'ytm':
            tm = int(arg.text)
        if arg.get('name') == 'c':
            jar_class = arg.text

    if n_task == 0:
        raise ValueError("runner/arguments/argument with name = yn was not found")

    jar = job.find('jar/path').text.strip()
    args = []
    for arg in job.find('jar/arguments').iter('argument'):
        args.append("{} {}".format(arg.get('name', ''), arg.text).strip())

    return FlinkApplication(name, n_task, jar, args, jar_class=jar_class, tm=tm)


class Experiment:
    def __init__(self, xml=None, applications=None, name="generated_experiment",
                 jobs_xml=None, jobs: Jobs = None):
        self.name = name
        self.applications = [] if None else applications

        if xml is not None and (jobs_xml is not None or jobs is not None):
            if jobs is None:
                jobs = Jobs(xml=jobs_xml)
            self.__read(ET.parse(xml).getroot().find('experiment'), jobs)

    def read(self, xml_str, jobs: Jobs):
        self.__read(ET.fromstring(xml_str).find('experiment'), jobs)

    def __read(self, experiment, jobs: Jobs):
        self.name = experiment.get('name', self.name)
        self.applications = []

        for job in experiment.iter('job'):
            app = jobs[job.get('name')]
            app.data_set = job.get('dataset', '')
            self.applications.append(app)

    def to_xml(self):
        suite = ET.Element('suite')
        experiment = ET.SubElement(suite, 'experiment')
        experiment.set('name', self.name)
        for job in self.applications:
            j = ET.SubElement(experiment, 'job')
            j.set('name', job.name)
            if job.data_set:
                j.set('dataset', job.data_set)
            j.text = '0'
        return minidom.parseString(ET.tostring(suite, 'utf-8')).toprettyxml(indent="   ")
