#!/usr/bin/env python
# encoding: utf-8

import os
import sys
import time
import threadpool
import threading
from threading import Event
import traceback

bin_path = os.path.dirname(os.path.abspath(__file__))
app_path = os.path.dirname(bin_path)

lib_path = os.path.join(app_path, 'lib')
etc_path = os.path.join(app_path, 'etc')

sys.path.append(lib_path)
sys.path.append(etc_path)

import pusher_config as config
from mwlogger import MwLogger
from rdao import Rdao, RdaoException


PUSH_TIMEOUT = config.push_timeout
POOLSIZE = config.repusher_threadpool
LOOP_INTERVAL = config.repusher_interval
REDIS_URL = config.redis_url
LOG_LEVEL = config.log_level

MODULE = "MWRepusher"

logger = MwLogger(MODULE, "syslog", log_level=LOG_LEVEL)

# create event handler for monitor
try:
    MONITOR_HOST = config.monitor_host
    MONITOR_PORT = config.monitor_port
    logger.create_event_handler(MONITOR_HOST, MONITOR_PORT)
except AttributeError:
    logger.info("Ignore monitor.")

def master(dao, kev):
    try:
        isking = dao.kingship(MODULE)
    except:
        logger.error(traceback.format_exc())
        os._exit(1)
    while True:
        try:
            if isking:
                logger.debug("I am king")
                isking = dao.is_still_king(MODULE)
            else:
                logger.debug("I am not king")
                isking = dao.kingship(MODULE)
            kev.set() if isking else kev.clear()
        except RdaoException as error:
            isking = False
            logger.error(traceback.format_exc())
            logger.event("redis_error", str(error), errorcode='01140301')
            kev.clear()
        time.sleep(5)

if __name__ == '__main__':

    # master and slaver, impl with redis
    dao = Rdao(REDIS_URL)
    kev = Event()
    kev.clear()
    master_thr = threading.Thread(target=master, args=(dao, kev))
    master_thr.setDaemon(True)
    master_thr.start()

    logger.info("Kingship thread running......")

    time.sleep(0.5)

    # create dbpc thread
    dbpc_thr = None
    try:
        from dbpc import dbpc
        dbpc_cfg = config.dbpc
        if dbpc_cfg:
            dbpc_thr = dbpc(dbpc_cfg['host'], dbpc_cfg['port'],
                dbpc_cfg['service'], dbpc_cfg['component_prefix'] + MODULE,
                dbpc_cfg['heartbeat_interval'])
            dbpc_thr.start()
            logger.info("dbpc thread running......")
    except (ImportError, AttributeError):
        # ignore dbpc
        logger.info("Ignore dbpc......")


    def do_push(customer_id):
        return dao.pushall(customer_id, PUSH_TIMEOUT)

    def log_result(request, result):
        logger.info("customer:{} push {} results".format(str(request.args[0]), result))

    def exception_alarm(request, exc_info):
        logger.error("customer:{} push_failed. {}".format(request.args[0], exc_info))
        # send "push_failed" event to monitor
        logger.event("push_failed", "customer:{} {}".format(request.args[0], exc_info), errorcode='01140509')

    pool = threadpool.ThreadPool(POOLSIZE)

    try:
        while True:
            kev.wait()
            customers = dao.get_all_customers()
            logger.debug("customers is {}".format(str(customers)))
            requests = threadpool.makeRequests(do_push, customers, log_result, exception_alarm)
            for req in requests:
                pool.putRequest(req)
            pool.wait()
            time.sleep(LOOP_INTERVAL)

        if dbpc_thr:
            dbpc_thr.join()
        master_thr.join()
    except:
        error_trace = traceback.format_exc()
        logger.error("I catch unknown error, exit!", exc_info=True)
        logger.event("repush_exception", error_trace, errorcode='01149900')

