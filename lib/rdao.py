#!/usr/bin/env python
# encoding: utf-8

'''
redis data access object for pusher module
'''

import redis
import datetime
import time
from redis import RedisError as RdaoException
from redis import ConnectionError, ReadOnlyError

from pusher_utils import generate_push_url, push2customer, PushError


DATA_EXPIRE = 8 * 3600 * 24
RETRY_BACKOFF = 120
MAX_DELAY = 3600


def retry(delay=3):
    '''
    for redis temporary unavailable. etc: master/slave translate
    '''
    def _wrapper(func):
        def __wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except (ConnectionError, ReadOnlyError):
                time.sleep(delay)
                self.reset()
                return func(self, *args, **kwargs)
        return __wrapper
    return _wrapper


class Rdao(object):

    def __init__(self, redis_url="redis://127.0.0.1/0"):
        self._redis_url = redis_url
        self._redis = redis.from_url(redis_url, retry_on_timeout=True)
        self._datas = "{}#datas"
        self._infos = "{}#infos"
        self._retries = "{}#retries"


    def kingship(self, module, expire_time=10):
        '''
        return True if be master
        '''
        is_king = self._redis.set(module + "_master", "", ex=expire_time, nx=True)
        return bool(is_king)

    def is_still_king(self, module, expire_time=10):
        '''
        return False if be slaver
        '''
        return bool(self._redis.set(module + "_master", "", ex=expire_time, xx=True))

    @retry()
    def save_push_data(self, customer_id, data):
        '''
        add push data at the leftest
        increase required push count
        '''
        key = self._datas.format(customer_id)
        if not self._redis.exists(key):
            self._redis.lpush(key, data)
            self._redis.expire(key, DATA_EXPIRE)
        else:
            self._redis.lpush(key, data)
        #self.incr_required_push()

    @retry()
    def push_data_ttl(self, customer_id):
        '''
        return None if not set expire time
        else return the ttl time
        '''
        key = self._datas.format(customer_id)
        return self._redis.ttl(key)

    @retry()
    def dump_push_data(self, customer_id, dump_file):
        '''
        Dump the customer push data to a file before deleting it for free memory
        Notes: It will block other commands long time when the key size is too large
        return None if customer has no any data to push, otherwise return dump_file
        '''
        key = self._datas.format(customer_id)
        dumps = self._redis.dump(key)
        if dumps:
            with open(dump_file, "wb") as fp:
                fp.write(dumps)
            return dump_file
        else:
            return None

    @retry()
    def restore_push_data(self, customer_id, dump_file):
        '''
        It is usually invoked by tools for restoring push data.
        Exception "redis.exceptions.ResponseError: Target key name is busy." will be
        raised if push_data key exists.
        You can catch the exception in a loop, retry until push_data key not exists
        '''
        key = self._datas.format(customer_id)
        with open(dump_file, "rb") as fp:
            self._redis.restore(key, 0, fp.read())

    @retry()
    def pushall(self, customer_id, timeout):
        '''
        Push customer data until exceptions raised or all data be pushed.
        Set unreachable if pushed failed.
        Set reachable if all data be pushed.
        Increase pushed count when pushed successfully one by one.
        Update retry_info.
        Using rpoplpush method and tmp key to storage pushed failed data
        for ensuring data is not lost.
        return the push data size really
        '''
        key = self._datas.format(customer_id)
        tmp_key = key + "_tmp"
        self._restore_backup_data(key, tmp_key)
        customer_info = self.get_customer_info(customer_id)
        retry_info = self.get_retry_info(customer_id)
        apikey, url = customer_info['apikey'], customer_info['push_url']

        to_push_size = self.push_data_size(customer_id)
        if to_push_size <= 0:
            return None  # None is no data to push

        if int(time.time()) <= retry_info['next_push_ts']:
            return 0 # 0 is not need to push as retry backoff

        retry_info['push_retries'] += 1
        retry_info['latest_push_ts'] = int(time.time())

        really_push_size = 0

        try:
            while 1:
                data = self._redis.rpoplpush(key, tmp_key)
                if not data:
                    break
                else:
                    # set url invalid time larger than timeout
                    push_url = generate_push_url(url, apikey, timeout + 10)
                    push2customer(push_url, data, timeout)
                    self._redis.lrem(tmp_key, data)
                    self.incr_pushed()
                    really_push_size += 1
        except PushError:
            self.set_unreachable(customer_id)
            retry_info['next_push_ts'] = int(time.time()) + min(
                RETRY_BACKOFF * pow(2, retry_info['push_retries'] - 1), MAX_DELAY)
            raise
        except:
            raise
        else:
            self._redis.expire(key, DATA_EXPIRE)
            self.set_reachable(customer_id)
            retry_info = {"latest_push_ts": 0,
                    "next_push_ts": 0,
                    "push_retries": 0}
        finally:
            self.set_retry_info(customer_id, retry_info)
            self._restore_backup_data(key, tmp_key)

        return really_push_size

    @retry()
    def _restore_backup_data(self, key, backup_key):
        for backup in self._redis.lrange(backup_key, 0, -1):
            if not self._redis.exists(key):
                self._redis.rpush(key, backup)
                # Should set expire time if key not exsits
                self._redis.expire(key, DATA_EXPIRE)
            else:
                self._redis.rpush(key, backup)
        # delete backup_key
        self._redis.delete(backup_key)

    @retry()
    def set_reachable(self, customer_id):
        key = self._infos.format(customer_id)
        self._redis.hset(key, "reachable", 1)

    @retry()
    def set_unreachable(self, customer_id):
        key = self._infos.format(customer_id)
        self._redis.hset(key, "reachable", 0)

    @retry()
    def reachable(self, customer_id):
        key = self._infos.format(customer_id)
        if not self._redis.hexists(key, "reachable"):
            return True
        return bool(int(self._redis.hget(key, "reachable")))

    @retry()
    def get_push_data(self, customer_id, size=1):
        '''
        read the rightest size data
        return a list
        You should reverse the return list if size > 2 when using it,
        because of the list is not FIFO.
        '''
        key = self._datas.format(customer_id)
        return self._redis.lrange(key, 0 - size, -1)

    @retry()
    def remove_push_data(self, customer_id, size=1):
        '''
        remove the rightest size data.
        Notes: using it after get_push_data. the size must be <=
        get_push_data's return list length. otherwise, the data where new may be lost.
        '''
        key = self._datas.format(customer_id)
        return self._redis.ltrim(key, 0, -1 - size)

    @retry()
    def push_data_size(self, customer_id):
        key = self._datas.format(customer_id)
        return self._redis.llen(key)

    @retry()
    def set_customer_info(self, customer_id, info):
        '''
        set a customer info
        info:
        {
            "push_url": $push_url.
            "apikey": $apikey,
            "reachable": 0/1
        }
        '''
        key = self._infos.format(customer_id)
        self._redis.hmset(key, info)
        self._redis.sadd("customers", customer_id)

    @retry()
    def get_customer_info(self, customer_id):
        '''
        read a customer's info
        '''
        key = self._infos.format(customer_id)
        return self._redis.hgetall(key)

    @retry()
    def _add_customer(self, customer_id):
        '''
        stroage customer_id using Sets
        '''
        self._redis.sadd("customers", customer_id)

    @retry()
    def incr_pushed(self):
        key = "{}_push_info".format(
            datetime.datetime.utcnow().strftime("%Y%m%d"))
        self._redis.hincrby(key, "pushed_cnt")

    @retry()
    def incr_required_push(self):
        key = "{}_push_info".format(
            datetime.datetime.utcnow().strftime("%Y%m%d"))
        self._redis.hincrby(key, "required_pushing_cnt")

    @retry()
    def get_pushed_cnt(self):
        '''
        return today's pushed count
        '''
        key = "{}_push_info".format(
            datetime.datetime.utcnow().strftime("%Y%m%d"))
        return int(self._redis.hget(key, "pushed_cnt"))

    @retry()
    def get_required_push_cnt(self):
        '''
        return today's required_push count
        '''
        key = "{}_push_info".format(
            datetime.datetime.utcnow().strftime("%Y%m%d"))
        return int(self._redis.hget(key, "required_pushing_cnt"))

    @retry()
    def get_push_info(self, date=None):
        '''
        date format: "yyyymmdd"
        return (required_push_cnt, pushed_cnt, pushed_rate)
        '''
        if date is None:
            key = "{}_push_info".format(
                datetime.datetime.utcnow().strftime("%Y%m%d"))
        else:
            key = "{}_push_info".format(date)

        info = self._redis.hgetall(key)
        if info:
            total, pushed = int(info.get('required_pushing_cnt', 0)), \
                int(info.get('pushed_cnt', 0))
            return (total, pushed, pushed / float(total) if total > 0 else 0)
        else:
            return (0, 0, 0.0)

    @retry()
    def count_up_push_info(self, start_date=None, end_date=None):
        '''
        count up push info from start to end
        Count up all info if start_date is None
        end_date is today if end_date is None
        '''
        keys = self._redis.keys("????????_push_info")
        keys = [key[:8] for key in keys]

        if start_date is None:
            pass
        elif end_date is None:
            keys = filter(lambda a: a >= start_date, keys)
        else:
            keys = filter(lambda a: end_date >= a >= start_date, keys)

        totals = 0
        pusheds = 0
        for key in keys:
            (total, pushed, _) = self.get_push_info(key)
            totals += total
            pusheds += pushed

        if totals > 0:
            return (totals, pusheds, pusheds / float(totals))
        else:
            return (0, 0, 0.0)

    @retry()
    def get_all_customers(self):
        return self._redis.smembers("customers")

    @retry()
    def get_retry_info(self, customer_id):
        '''
        read a customer's retry_info:
        '''
        key = self._retries.format(customer_id)
        if not self._redis.exists(key):
            return {"latest_push_ts": 0,
                    "next_push_ts": 0,
                    "push_retries": 0}
        else:
            return {key: int(value) for key, value in self._redis.hgetall(key).items()}

    @retry()
    def set_retry_info(self, customer_id, retry_info):
        '''
        set a customer's retry info
        retry_info:
        {
        "latest_push_ts"： $timestamp
        "next_push_ts": $timestamp
        "push_retries": $retries
        }
        '''
        key = self._retries.format(customer_id)
        self._redis.hmset(key, retry_info)

    @retry()
    def get_retries(self, customer_id):
        return self.get_retry_info(customer_id).get(
            "push_retries", 0)

    def reset(self):
        '''
        reset the redis client
        Raise exception if redis-server can't be connected
        '''
        del self._redis
        self._redis = redis.from_url(self._redis_url)


if __name__ == "__main__":

    # test for retry
    class Test(object):

        def __init__(self):
            self._reset = False

        @retry(0)
        def raise_exception(self):
            if not self._reset:
                print "raise exception"
                raise ConnectionError
            else:
                print "reset ok!"

        def reset(self):
            self._reset = True
            print "reset be called"

    test = Test()
    test.raise_exception()

    # test for Rdao

    rdao = Rdao()

    import threading
    from threading import Event

    kev = Event()
    kev.clear()

    def master():
        isking = rdao.kingship("test_module", expire_time=10)
        while True:
            if isking:
                kev.set()
                isking = rdao.is_still_king("test_module", expire_time=10)
            else:
                isking = rdao.kingship("test_module", expire_time=10)
                if isking:
                    kev.set()
            time.sleep(5)

    thr = threading.Thread(target=master)
    thr.setDaemon(True)
    thr.start()

    print "To be master or slaver"
    kev.wait()
    print "Be master...."

    time.sleep(10)

    methods = {key for key in dir(Rdao) if not key.startswith('_')}

    cus_id = 1
    cus_info = {
        "push_url": "http://127.0.0.1:8088/mw/matches",
        "apikey": "#!@$%^dlf$%@!*",
        "reachable": "1"  # the key is option.
    }

    rdao.get_customer_info("1")
    rdao._redis.flushdb()

    rdao.set_customer_info(cus_id, cus_info)
    assert rdao.get_customer_info(cus_id) == cus_info
    assert rdao.reachable(cus_id)
    rdao.set_unreachable(cus_id)
    assert not rdao.reachable(cus_id)
    rdao.set_reachable(cus_id)
    assert rdao.reachable(cus_id)
    assert rdao.get_customer_info(cus_id) == cus_info
    assert set(['1']) == rdao.get_all_customers()

    import json
    push_data = json.dumps({
        "task_uuid": "89a9d83kd-2k9akdfgg-kdjjf-34234",
        "matches": [
            "match1", "match2", "match3"
        ]
    })

    push_data1 = json.dumps({
        "task_uuid": "89a9d83kd-2k9akdfgg-kdjjf-34234",
        "matches": [
            "match4", "match5", "match6"
        ]
    })

    assert rdao.dump_push_data(cus_id, "/tmp/1.dump") is None

    rdao.save_push_data(cus_id, push_data)
    rdao.incr_required_push()
    rdao.save_push_data(cus_id, push_data1)
    rdao.incr_required_push()

    assert rdao.get_push_data(cus_id) == [push_data]
    assert rdao.push_data_size(cus_id) == 2
    assert rdao.push_data_ttl(cus_id) > 0

    try:
        print "======== before push"
        print rdao.get_retry_info(cus_id)
        assert "/tmp/1.dump" == rdao.dump_push_data(cus_id, "/tmp/1.dump")
        assert 0 == rdao.get_retries(cus_id)
        rdao.pushall(cus_id, 10)
        print "======== after push"
    except Exception as err:
        print "======== error push"
        print rdao.get_retry_info(cus_id)

        assert 1 == rdao.get_retries(cus_id)
        assert rdao.push_data_size(cus_id) == 2
        assert rdao.get_push_data(cus_id, 2) == [push_data1, push_data]

        try:
            rdao.restore_push_data(cus_id, "/tmp/1.dump")
        except RdaoException as err:
            assert "Target key name is busy." == str(err)

        rdao.remove_push_data(cus_id, 1)
        assert rdao.push_data_size(cus_id) == 1
        assert rdao.get_push_data(cus_id) == [push_data1]

        rdao.remove_push_data(cus_id, 1)
        assert rdao.push_data_size(cus_id) == 0
        assert rdao.get_push_data(cus_id) == []

        assert not rdao.reachable(cus_id)
        assert (2, 0, 0.0) == rdao.get_push_info()

        rdao.restore_push_data(cus_id, "/tmp/1.dump")

    else:
        print rdao.get_retry_info(cus_id)
        assert 0 == rdao.get_retries(cus_id)
        assert rdao.reachable(cus_id)
        assert rdao.push_data_size(cus_id) == 0
        assert rdao.get_push_data(cus_id) == []
        assert rdao.get_pushed_cnt() == 2
        assert rdao.get_required_push_cnt() == 2
        assert (2, 2, 1.0) == rdao.get_push_info()
        assert (2, 2, 1.0) == rdao.count_up_push_info()

        rdao.restore_push_data(cus_id, "/tmp/1.dump")
        assert rdao.get_push_data(cus_id, 2) == [push_data1, push_data]

    print "test_ok"

