#!/usr/bin/env python
# encoding: utf-8

PROGRAM_INFO = "MediaWise MWPusher 2.15.0.0"

import sys
import os
from os.path import abspath, join, dirname
PUSHER_FOLDER = abspath(join(dirname(__file__), os.pardir))
import traceback

sys.path.append(join(PUSHER_FOLDER, 'lib'))
sys.path.append(join(PUSHER_FOLDER, 'etc'))

import pusher_config as config
from rdao import Rdao, RdaoException
from pusher_utils import generate_push_url, push2customer, PushError
from mwampq import Amqp
from mwlogger import MwLogger

LOG_LEVEL      = config.log_level
REDIS_URL      = config.redis_url
PUSH_TIMEOUT   = config.push_timeout
MQ_URL         = config.push_queue['url']
MQ_EXCHANGE    = config.push_queue['exchange']
MQ_QUEUE       = config.push_queue['queue']
MQ_ROUTING_KEY = config.push_queue['routing_key']

logger         = MwLogger('mwPusher', "syslog", log_level=LOG_LEVEL)
dao            = Rdao(REDIS_URL)


class SavePushDataException(Exception): pass

# create event handler for monitor
try:
    logger.create_event_handler(config.monitor_host, config.monitor_port)
except AttributeError:
    logger.info("Ignore monitor.")

def save_push_data(body, dao):
    '''
    unreachable task will save to redis, if save failure, return to rabbitmq
    '''
    try:
        dao.save_push_data(body['customer_id'], body['push_data'])
    except:
        raise SavePushDataException(traceback.format_exc())

def save_customer_info(body, dao, customer_info):
    dao_able = True
    reach_able = True
    try:
        dao.set_customer_info(body['customer_id'], customer_info)
        reach_able = dao.reachable(body['customer_id'])
    except RdaoException:
        dao_able = False
    return dao_able, reach_able

def push(body, dao):
    '''
    send date to customer
    '''
    push_addr = generate_push_url(body['push_url'], body['apikey'], \
                                  PUSH_TIMEOUT + 10)
    push2customer(push_addr, body['push_data'], PUSH_TIMEOUT)

def valid_message(body):
    return 'push_url' in body and 'apikey' in body and \
        'customer_id' in body and 'push_data' in body

def process_task(body, message):
    '''
    rabbitmq callback message
    '''
    logger.info('rabbitmq callback message')
    if valid_message(body):
        logger.debug('receive queue info: {}'.format(body))
        try:
            customer_info = {}
            customer_info['push_url'] = body['push_url']
            customer_info['apikey'] = body['apikey']
            logger.info('set_customer_info, id:%s, push_url:%s, apikey:%s'%\
                        (body['customer_id'], body['push_url'], body['apikey']))
            dao_able, reach_able = save_customer_info(body, dao, customer_info)
            if dao_able:
                dao.incr_required_push()
                if reach_able:
                    logger.info('push to customer')
                    try:
                        push(body, dao)
                    except PushError, msg:
                        logger.info('PushError save push data to redis')
                        save_push_data(body, dao)    
                    else:            
                        dao.incr_pushed()
                else:
                    logger.info('reach_unable save push data to redis')
                    save_push_data(body, dao)
            else:
                logger.info('dao_unable push to customer directly')
                push(body, dao)
            message.ack()
        except PushError, msg:
            message.requeue()
            logger.error("push to customer except: {}".format(msg))
        except SavePushDataException, msg:
            message.requeue()
            logger.error("SavePushDataException: {}".format(msg))
            logger.event("redis_error", str(msg), errorcode='01150301')
    else:
        logger.warn('receive invalid queue info: {}'.format(body))
        message.ack()

def main():
    try:
        logger.info('mwPusher start')
        with Amqp(MQ_URL, MQ_EXCHANGE, MQ_QUEUE, MQ_ROUTING_KEY) as q:
            q.poll(process_task)
    except:
        logger.error("pusher_unhandle_except: {}".format(traceback.format_exc()))
        logger.event("unhandler_error", traceback.format_exc(), errorcode='01159900')

if __name__ == '__main__':
    main()
