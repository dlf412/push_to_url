#! /usr/bin/env python
# coding:utf-8

'''
    use for send dbpc

'''
import sys
import time
import socket
import getopt
import traceback
import threading
import logging

class dbpc(threading.Thread):
    def __init__(self, dbpc_host, dbpc_port, service, component, logger, send_interval=5):
        super(dbpc, self).__init__()
        '''
            Constructor for dbpc
            `dbpc_host`:
                DBPC server hostname or ipaddress.
            `dbpc_port`:
                DBPC server port ,PORT  must be a valid interger
            `dbpc_service`:
                send DBPC service, SERVICE must in DB service_name(enum)
            `component`:
                send DBPC component.
            `send_interval`:
                send DBPC time interval (minute)
        '''
        self.dbpc_host = dbpc_host
        self.dbpc_port = dbpc_port
        self.service = service
        self.component = component
        self.send_interval = send_interval
        self.pause_flag = False
        self.setDaemon(True)
        self.logger = logger

    def pause(self):
        self.pause_flag = True

    def resume(self):
        self.pause_flag = False

    def send(self):
        '''
            Use sock send localhost, service, component to dbpc
            Server
        '''
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            connect_value = (self.dbpc_host, self.dbpc_port)
            s.connect(connect_value)
            s.sendall('{"status":[{"service":"%s", component:"%s"}]}' % (self.service,
                self.component))
            # `ret` whill successful when dbpc server receive current dbpc message from client
            #  other will block
            ret =  s.recv(1024)
            return ret
        finally:
            s.close()

    def run(self):
        while True:
            try:
                if not self.pause_flag:
                    self.send()
                else:
                    self.logger.info("dbpc paused")
            except Exception, e:
                self.logger.error("send dbpc catch exception", exc_info=True)
            finally:
                time.sleep(int(self.send_interval))

if __name__=='__main__':
    d = dbpc("192.168.1.146",5800, "vddb123", "warmup", 10)
    d.start()
    d.join()
