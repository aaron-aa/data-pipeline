import os
import tempfile


class Temp(object):
    def __init__(self, name):
        self.path = os.path.join(tempfile.gettempdir(), name.replace("/", "-"))

    def __enter__(self):
        return open(self.path, "wb")

    def __exit__(self, *args):
        try:
            os.remove(self.path)
        except:
            pass
