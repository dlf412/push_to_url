#!/usr/bin/env python
# encoding: utf-8

tools_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(tools_dir)
lib_dir = os.path.join(base_dir, "lib")
sys.path.append(lib_dir)

import mwconfig
import os
import re
import sys

'''
'''

pusher_conf = os.path.join(base_dir, "etc", "pusher_config.py")

mw_conf = sys.argv[1]

conf = mwconfig.Mwconfig(mw_conf)

with open(pusher_conf) as f:
    pusher_conf_str = f.read()

dbpc_src = re.findall(r"(dbpc\s*=\s*{.*?})", pusher_conf_str, re.S)[0]
push_queue_src = re.findall(r"(push_queue\s*=\s*{.*?})", pusher_conf_str, re.S)[0]

redis_url_src = re.findall(r"(redis_url\s*=\s*.*)", pusher_conf_str)[0]
push_timeout_src = re.findall(r"(push_timeout\s*=\s*.*)", pusher_conf_str)[0]

monitor_host_src = re.findall(r"(monitor_host\s*=\s*.*)", pusher_conf_str)[0]
monitor_port_src = re.findall(r"(monitor_port\s*=\s*.*)", pusher_conf_str)[0]

pusher_conf_str = pusher_conf_str.replace(dbpc_src, "dbpc = {}".format(str(conf.dbpc)))
pusher_conf_str = pusher_conf_str.replace(push_queue_src, "push_queue = {}".format(str(conf.push_queue)))
pusher_conf_str = pusher_conf_str.replace(redis_url_src, 'redis_url = "{}"'.format(conf.pusher.redis_url))
pusher_conf_str = pusher_conf_str.replace(push_timeout_src, "push_timeout = {}".format(conf.pusher.push_timeout))
pusher_conf_str = pusher_conf_str.replace(monitor_host_src, 'monitor_host = "{}"'.format(conf.monitor.host))
pusher_conf_str = pusher_conf_str.replace(monitor_port_src, 'monitor_port = {}'.format(conf.monitor.port))

os.rename(pusher_conf, pusher_conf + ".bak")
with open(pusher_conf, "w") as f:
    f.write(pusher_conf_str)

