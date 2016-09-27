from yarn_workloader import *
import xml.etree.ElementTree as ET


class Xml2Application:
    expected_app = FlinkApplication(
            name="tpch-1-full",
            n_task=159,
            jar="/home/test/tests/tpch/test.jar",
            jar_class="JarClassK",
            tm=1536,
            args=[
                "hdfs:///data/tpch/1T/lineitem.tbl",
                "--arg2 hdfs:///data/tpch/1T/customer.tbl",
                "hdfs:///data/tpch/1T/orders.tbl",
                "TEMP"
            ]
    )

    def test_xml_to_flink_application(self):
        with open('test/dummies/jobs.xml') as xml_file:
            xml = ET.parse(xml_file).getroot('jobs').find('job')
            flink_app = xml_to_flink_application(xml)

        assert self.expected_app.is_a_copy_of(flink_app)


class TestJobs:
    expected_apps = {
        "tpch-1-full": Xml2Application.expected_app,
        "tpch-1": FlinkApplication(
            name="tpch-1",
            n_task=70,
            jar="/home/test/tests/tpch/test.jar",
            args=[]
        )
    }

    def test_read(self):
        with open('test/dummies/jobs.xml') as xml:
            jobs = Jobs(xml)

        for name, app in jobs._data.items():
            assert self.expected_apps[name].is_a_copy_of(app)


class TestExperiment:
    def test_read(self):
        with open('test/dummies/jobs.xml') as jobs_xml, open('test/dummies/experiment.xml') as exp_xml:
            exp = Experiment(exp_xml, jobs_xml=jobs_xml)

        assert exp.name == "tpch-coco"
        app0 = TestJobs.expected_apps['tpch-1-full'].copy()
        app0.data_set = '5'
        assert exp.applications[0].is_a_copy_of(app0)
        assert exp.applications[1].is_a_copy_of(TestJobs.expected_apps['tpch-1'])
        assert exp.applications[2].is_a_copy_of(TestJobs.expected_apps['tpch-1-full'])

    def test_to_xml(self):
        app0 = TestJobs.expected_apps['tpch-1-full'].copy()
        app0.data_set = '5'
        exp = Experiment(
            name="tpch-coco",
            applications=[
                app0,
                TestJobs.expected_apps['tpch-1'].copy(),
                TestJobs.expected_apps['tpch-1-full'].copy(),
            ]
        )
        with open('test/dummies/experiment.xml') as exp_file:
            expected_xml = exp_file.read()

        assert expected_xml == exp.to_xml().strip()




