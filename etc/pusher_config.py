#!/usr/bin/env python
# encoding: utf-8


push_queue = {'url': 'amqp://guest:guest@192.168.5.229:5672', 'queue': 'mw-push_queue', 'routing_key': 'mw-push_routing_key', 'exchange': 'mw-push_exchange'}
redis_url = "redis://127.0.0.1:6379"
push_timeout = 30

# repusher's configures
repusher_interval = 30
repusher_threadpool = 10

# "DEBUG", "INFO", "WARN", "ERROR"
log_level = "INFO"

# dbpc setting
dbpc = {'heartbeat_interval': 30, 'host': '192.168.1.41', 'component_prefix': 'mw1_', 'port': 5800, 'service': 'mw'}

# monitor setting
monitor_host = "192.168.5.176"
monitor_port = 9999
