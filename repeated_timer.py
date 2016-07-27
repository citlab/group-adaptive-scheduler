from threading import Timer


class RepeatedTimer(Timer):
    def run(self):
        self.finished.wait(self.interval)
        if not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.run()
