# coding: utf-8
import time
import struct
import pickle
import socket
import logging
import logging.handlers

import pycl5
import pycdnutil

from typing import Tuple, Optional, Dict, Any


_NetworkInterfaces = {}
for _net_dev_name in ['eth0', 'eth1']:
    _net_dev_addr = None
    try:
        _net_dev_addr = pycdnutil.get_network_interface_address(interface_name=_net_dev_name)
    except OSError:
        pass
    _NetworkInterfaces[_net_dev_name] = _net_dev_addr


class SHandler(logging.Handler):
    """
    将 CRITICAL 级别的告警日志记录发送到指定端服务器进行处理，例子::

        log = logging.getLogger('module_name')
        log.setLevel(logging.DEBUG)

        log.addHandler(pyslogger.handler.SHandler(
            cl5_sid=(64349377, 65536),
            address=('127.0.0.1', 6666),
            opt_data={
                'mail_receivers': 'bensonliu;cloudywu',
                'apd_report_str_feature_id': 5928714
            }))

        log.info('这个消息不会发送到远端')
        for i in range(2):
            log.critical('致命日志[%d]' % i)

    连接服务器端失败期间的所有记录会被丢弃。
    """

    def __init__(self,
                 cl5_sid: Optional[Tuple[int, int]],
                 address: Optional[Tuple[str, int]],
                 opt_data: Optional[Dict[str, Any]]):
        """
        参数 `cl5_sid` 与 `address` 不能都为空，如果两个参数都有，则以 `cl5_sid` 为准

        :param cl5_sid: 日志服务端的 cl5 ID
        :param address: 日志服务端的地址
        :param opt_data: 额外向服务端发送的数据，必须满足 pickle 属性，默认包含的字段是 eth1/eth0 地址
        """
        logging.Handler.__init__(self)
        self.setLevel(logging.CRITICAL)

        self._address = address
        if cl5_sid:
            self._cl5_client = pycl5.CL5Client(
                mod_id=cl5_sid[0], cmd_id=cl5_sid[1], key=0, timeout_seconds=1.0, logger=None)
        else:
            self._cl5_client = None
            assert self._address is not None

        self._opt_data = {} if opt_data is None else opt_data
        self._opt_data.update(_NetworkInterfaces)

        self._retry_time = None
        #
        # Exponential backoff parameters.
        #
        self._retry_start = 1.0
        self._retry_max = 30.0
        self._retry_factor = 2.0

    def _do_send(self, address: Tuple[str, int], data):
        sock = socket.create_connection(address, timeout=1)
        try:
            sock.sendall(data)
        except OSError as e:
            if self._cl5_client:
                raise pycl5.CL5NetworkException(ori_e=e)
            else:
                raise
        finally:
            sock.close()

    def _send(self, s):
        now = time.time()
        # Either _retry_time is None, in which case this
        # is the first time back after a disconnect, or
        # we've waited long enough.
        if self._retry_time is None or now >= self._retry_time:
            try:
                self._retry_time = None  # next time, no delay before trying

                if self._cl5_client:
                    with self._cl5_client.get_route(timeout_seconds=None) as (host, port):
                        self._do_send(address=(host, port), data=s)
                else:
                    self._do_send(address=self._address, data=s)
            except OSError:
                # Creation failed, so set the retry time and return.
                if self._retry_time is None:
                    self.retryPeriod = self._retry_start
                else:
                    self.retryPeriod = self.retryPeriod * self._retry_factor
                    if self.retryPeriod > self._retry_max:
                        self.retryPeriod = self._retry_max
                self._retry_time = now + self.retryPeriod

    def _makePickle(self, record: logging.LogRecord) -> bytes:
        """
        Pickles the record in binary format with a length prefix, and
        returns it ready for transmission across the socket.
        """
        ei = record.exc_info
        if ei:
            # just to get traceback text into record.exc_text ...
            self.format(record)
        # See issue #14436: If msg or args are objects, they may not be
        # available on the receiving end. So we convert the msg % args
        # to a string, save it as msg and zap the args.
        d = dict(record.__dict__)
        d['msg'] = record.getMessage()
        d['args'] = None
        d['exc_info'] = None
        # Issue #25685: delete 'message' if present: redundant with 'msg'
        d.pop('message', None)
        s = pickle.dumps(d, 1)
        slen = struct.pack(">L", len(s))
        return slen + s

    def emit(self, record: logging.LogRecord):
        if record.levelno == logging.CRITICAL:
            try:
                for name, value in self._opt_data.items():
                    setattr(record, name, value)

                self._send(self._makePickle(record))
            except Exception:
                logging.Handler.handleError(self, record)
