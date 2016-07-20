from repeated_timer import *
import time


class TestRepeatedTimer:
    def test_run(self):
        result = []

        def action():
            result.append(1)

        timer = RepeatedTimer(0.1, action)
        timer.start()
        time.sleep(0.31)
        timer.cancel()

        expected_result = [1 for i in range(3)]

        assert expected_result == result

