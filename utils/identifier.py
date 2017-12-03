import datetime


def ID():
    return long(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
