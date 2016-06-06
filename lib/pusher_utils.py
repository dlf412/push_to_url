#!/usr/bin/env python
# encoding: utf-8

from hashlib import md5, sha512
from time import time as now

import requests
from requests import RequestException as PushError


def generate_push_url(url, apikey, invalid_sec):
    ts = int(now()) + invalid_sec
    at = md5(sha512("{}{}".format(apikey, ts)).hexdigest()).hexdigest()
    return "{}?ts={}&at={}".format(url, ts, at)


def push2customer(url, data, timeout):
    headers = {"Connection": "Keep-Alive", "Accept": "*/*"}
    if isinstance(data, dict):
        r = requests.post(url, json=data, headers=headers, timeout=timeout)
    else:
        if isinstance(data, unicode):
            # support utf8 encoding bytes only
            data = data.encode("utf-8")
        r = requests.post(url, data=data, headers=headers, timeout=timeout)
    if r.status_code != 200:
        try:
            msg = r.json()['msg']
        except:
            r.raise_for_status()
        else:
            http_error_msg = '%s Server Error: %s for url: %s, msg: %s' % (
                r.status_code, r.reason, r.url, msg)
            raise requests.HTTPError(http_error_msg)

if __name__ == "__main__":

    url = "http://127.0.0.1:8088/mw/matches"
    apikey = "#!@$%^dlf$%@!*"
    sec = 30

    push_url = generate_push_url(url, apikey, sec)
    push2customer(push_url, u'{"key1": "中文", "key2": "value2"}', sec)
    push2customer(push_url, u"<a>测试what? </a>", sec)
