import hashlib
import json


def datasign(data):
    if data:
        md5 = hashlib.md5(json.dumps(data)).hexdigest()
        return "dp{}".format(md5)
    else:
        return ""
