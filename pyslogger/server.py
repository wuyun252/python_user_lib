# coding: utf-8
import os
import time
import socket
import pickle
import struct
import logging
import argparse
import resource
import traceback
import logging.handlers

import tornado.ioloop
import tornado.iostream
import tornado.tcpserver

import pytof
import psutil
import pycdnutil
import pyanmagent

from . import callback

from typing import Tuple


class _Report:

    _tag = ''
    _pindex = ''

    ioloop_lag_measure = pycdnutil.IOLoopLatencyRecorder(interval_seconds=1.0)
    callback_delay = 0
    n_record = 0
    n_callback_error = 0
    n_wronglevel_record = 0
    n_mal_record = 0

    @classmethod
    def report(cls):
        if len(cls._tag) > 0:
            pyanmagent.Client.report(
                tag=cls._tag,
                data_dict={cls._pindex: {
                    'n_mal_record': cls.n_mal_record,
                    'n_wronglevel_record': cls.n_wronglevel_record,
                    'n_callback_error': cls.n_callback_error,
                    'n_record': cls.n_record,
                    'callback_delay': int(1000 * cls.callback_delay)
                }},
                cal_method=pyanmagent.CalMethod.ADD)
            cls.n_record = 0
            cls.n_callback_error = 0
            cls.n_wronglevel_record = 0
            cls.n_mal_record = 0
            cls.callback_delay = 0

            pyanmagent.Client.report(
                tag=cls._tag,
                data_dict={cls._pindex: {'max_ioloop_lag': int(abs(cls.ioloop_lag_measure.max_dv(reset=True)) * 1000)}},
                cal_method=pyanmagent.CalMethod.MAX)


class CriticalLogServer(tornado.tcpserver.TCPServer):
    """
    处理致命日志记录的服务器框架
    """

    class TcpLogHandler(object):

        def __init__(self,
                     address: Tuple[str, int],
                     stream: tornado.tcpserver.IOStream,
                     critical_log_callback,
                     logger: logging.Logger):
            self._address = str(address)
            self._stream = stream
            self._critical_log_callback = critical_log_callback
            self._logger = logger

            self._logger.info('recv connection from %s' % str(self._address))
            self._read_head()

        def _read_head(self):
            try:
                self._stream.read_bytes(4, self._on_head)
            except tornado.iostream.StreamClosedError:
                self._stream.close()
                self._logger.info('%s closed' % str(self._address))

        def _read_body(self, body_len):
            try:
                self._stream.read_bytes(body_len, self._on_body)
            except tornado.iostream.StreamClosedError:
                self._logger.error('stream closed (incomplete): %s' % self._address)
                self._stream.close()

        def _on_head(self, data):
            body_len = struct.unpack('>L', data)[0]
            self._read_body(body_len)

        def _on_body(self, data):
            t_beg = time.time()
            self._logger.info('recv log from %s with size %d' % (str(self._address), len(data)))
            try:
                data_record = pickle.loads(data, errors='ignore')
                log_record = logging.makeLogRecord(data_record)
            except Exception:
                self._logger.critical('error from %s\n%s' % (self._address, traceback.format_exc()))
                _Report.n_mal_record += 1
            else:
                if log_record.levelno == logging.CRITICAL:
                    try:
                        self._critical_log_callback(self._address, log_record)
                    except Exception:
                        _Report.n_callback_error += 1
                    finally:
                        self._logger.info('log from %s' % self._address)
                        self._logger.handle(log_record)
                else:
                    _Report.n_wronglevel_record += 1
            _Report.n_record += 1
            _Report.callback_delay += time.time() - t_beg

            self._read_head()

    def __init__(self, critical_log_callback, logger: logging.Logger):
        """

        :param critical_log_callback:

            致命日志记录处理回调函数，参数为 ( 客户端地址, LogRecord 对象 )
            回调是串行执行的，所以这里注意性能，不要阻塞

        :param logger: 服务器运行日志 logger
        """
        super(CriticalLogServer, self).__init__()
        self._logger = logger
        self._critical_log_callback = critical_log_callback

    def handle_stream(self, stream: tornado.tcpserver.IOStream, address):
        self.TcpLogHandler(
            address, stream, self._critical_log_callback, self._logger)

    @classmethod
    def start_server(cls, address: str, port: int, cb, logger: logging.Logger):
        """
        启动服务器

        :param address: bind address
        :param port: listen port
        :param cb: CRITICAL log callback function
        :param logger: server logger
        """
        svr = cls(logger=logger, critical_log_callback=cb)
        svr.bind(
            port,
            address=address,
            reuse_port=hasattr(socket, "SO_REUSEPORT"))
        svr.start(num_processes=1)
        tornado.ioloop.IOLoop.instance().start()


def main():
    """
    启动日志处理服务器，接受如下命令行参数

        --interface: 服务器绑定地址

        --port: 服务器监听端口

        --log_name: 日志名称, 无须 '.log' 后缀

        --tof_key: 用于发送邮件的tof appkey

        --apd_tag: 用于监控上报, 空表示不统计

        --p_index: supervisord %(process_num)s

    默认提供 :class:`.callback.ApdStrReportCallback` 与 :class:`.callback.TofMailCallback`
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-i', '--interface', type=str, default='lo')
    arg_parser.add_argument('-p', '--port', type=int, default=6666)
    arg_parser.add_argument('-o', '--log_name', type=str, default='critical_log_server.log')
    arg_parser.add_argument('-k', '--tof_key', type=str, required=True)
    arg_parser.add_argument('-t', '--apd_tag', type=str, default='')
    arg_parser.add_argument('-c', '--p_index', type=int, default=0)
    args = arg_parser.parse_args()

    logger = logging.getLogger('pyslogger.server')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    rotating_file_handler = logging.handlers.RotatingFileHandler('%s-p%d.log' % (args.log_name, args.p_index),
                                                                 mode='a',
                                                                 maxBytes=10*(1024**2),
                                                                 backupCount=10,
                                                                 encoding='utf-8')
    rotating_file_handler.setFormatter(logging.Formatter(
        '[%(asctime)s][%(name)s][%(processName)s(%(process)d)[%(filename)s:%(lineno)d][%(levelname)s]%(message)s'))
    logger.addHandler(rotating_file_handler)

    if len(args.apd_tag) > 0:
        _Report._tag = args.apd_tag
        _Report._pindex = 'pindex=p%d' % args.p_index

        tornado.ioloop.PeriodicCallback(_Report.ioloop_lag_measure.measure, 1000).start()
        tornado.ioloop.PeriodicCallback(_Report.report, 60000).start()

    psutil.Process(pid=os.getpid()).cpu_affinity(cpus=[os.getpid() % os.cpu_count()])

    resource.setrlimit(resource.RLIMIT_NOFILE, (100000, 100000))

    cb = callback.CallbacksChain(logger=logger)
    cb.add(cb=callback.ApdStrReportCallback(logger=logger))
    cb.add(cb=callback.TofMailCallback(tof=pytof.Tof(appkey=args.tof_key, logger=logger), logger=logger))

    CriticalLogServer.start_server(
        address=pycdnutil.get_network_interface_address(interface_name=args.interface),
        port=args.port,
        cb=cb,
        logger=logger)


if __name__ == '__main__':
    main()
