from yarn_workloader import *
import xml.etree.ElementTree as ET


class Xml2Application:
    flink_job = """
        <job name="tpch-1-full">
            <runner>
                <name>flink</name>
                <arguments>
                    <argument name="m">yarn-cluster</argument>
                    <argument name="yn">159</argument>
                </arguments>
            </runner>
            <jar>
                <path>/home/test/tests/tpch/test.jar </path>
                <arguments>
                    <argument>hdfs:///data/tpch/1T/lineitem.tbl</argument>
                    <argument name="--arg2">hdfs:///data/tpch/1T/customer.tbl</argument>
                    <argument>hdfs:///data/tpch/1T/orders.tbl</argument>
                    <argument>hdfs:///user/test/tpch/result</argument>
                </arguments>
            </jar>
        </job>
    """

    def test_xml_to_flink_application(self):
        flink_app = xml_to_flink_application(ET.fromstring(self.flink_job))

        expected_args = [
            "hdfs:///data/tpch/1T/lineitem.tbl",
            "--arg2 hdfs:///data/tpch/1T/customer.tbl",
            "hdfs:///data/tpch/1T/orders.tbl",
            "hdfs:///user/test/tpch/result"
        ]

        assert flink_app.name == "tpch-1-full"
        assert len(flink_app.tasks) == 159
        assert flink_app.jar == "/home/test/tests/tpch/test.jar"
        assert expected_args == flink_app.args


class TestJobs:
    jobs = """
        <jobs>
            <job name="tpch-1-full">
                <runner>
                    <name>flink</name>
                    <arguments>
                        <argument name="m">yarn-cluster</argument>
                        <argument name="yn">159</argument>
                    </arguments>
                </runner>
                <jar>
                    <path>/home/test/tests/tpch/test.jar </path>
                    <arguments>
                        <argument>hdfs:///data/tpch/1T/lineitem.tbl</argument>
                        <argument name="--arg2">hdfs:///data/tpch/1T/customer.tbl</argument>
                        <argument>hdfs:///data/tpch/1T/orders.tbl</argument>
                        <argument>hdfs:///user/test/tpch/result</argument>
                    </arguments>
                </jar>
            </job>
            <job name="tpch-1">
                <runner>
                    <name>flink</name>
                    <arguments>
                        <argument name="m">yarn-cluster</argument>
                        <argument name="yn">70</argument>
                        <argument name="yqu">default</argument>
                    </arguments>
                </runner>
                <jar>
                    <path>/home/test/tests/tpch/test.jar </path>
                    <arguments>
                    </arguments>
                </jar>
            </job>
        </jobs>
        """

    expected_apps = {
        "tpch-1-full": FlinkApplication(
            name="tpch-1-full",
            n_task=159,
            jar="/home/test/tests/tpch/test.jar",
            args=[
                "hdfs:///data/tpch/1T/lineitem.tbl",
                "--arg2 hdfs:///data/tpch/1T/customer.tbl",
                "hdfs:///data/tpch/1T/orders.tbl",
                "hdfs:///user/test/tpch/result"
            ]
        ),
        "tpch-1": FlinkApplication(
            name="tpch-1",
            n_task=70,
            jar="/home/test/tests/tpch/test.jar",
            args=[]
        )
    }

    def test_read(self):
        jobs = Jobs()
        jobs.read(self.jobs)

        for name, app in jobs._data.items():
            assert self.expected_apps[name].is_a_copy_of(app)


class TestExperiment:
    suite = """
<suite>
   <experiment name="tpch-coco">
      <job name="tpch-1-full">0</job>
      <job name="tpch-1">0</job>
      <job name="tpch-1-full">0</job>
   </experiment>
</suite>
"""

    def test_read(self):
        exp = Experiment()
        exp.read(self.suite, TestJobs.jobs)

        assert exp.name == "tpch-coco"
        assert exp.applications[0].is_a_copy_of(TestJobs.expected_apps['tpch-1-full'])
        assert exp.applications[1].is_a_copy_of(TestJobs.expected_apps['tpch-1'])
        assert exp.applications[2].is_a_copy_of(TestJobs.expected_apps['tpch-1-full'])

    def test_to_xml(self):
        exp = Experiment(
            name="tpch-coco",
            applications=[
                TestJobs.expected_apps['tpch-1-full'].copy(),
                TestJobs.expected_apps['tpch-1'].copy(),
                TestJobs.expected_apps['tpch-1-full'].copy(),
            ]
        )
        expected_xml = '<?xml version="1.0" ?>\n' + self.suite.strip()

        assert expected_xml == exp.to_xml().strip()




