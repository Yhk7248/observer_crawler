from datetime import datetime


def printl(*argv, **kwargs):
    date_str = datetime.now()
    print("[{}]".format(date_str), *argv, **kwargs, flush=True)
