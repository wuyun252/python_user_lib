# coding: utf-8
import time
import logging
import traceback

import pytof
import pyanmagent.apdagent

from typing import List


class CriticalLogCallbackBase:
    """
    通用回调函数的基类
    """

    def __init__(self, logger: logging.Logger):
        """

        :param logger: log object
        """
        self._logger = logger

    def __call__(self, address: str, record: logging.LogRecord):
        """
        回调函数，要求无副作用

        :param address: ip:port 客户端字符串
        :param record: 客户端发送的致命日志记录
        """
        raise NotImplementedError


class ApdStrReportCallback(CriticalLogCallbackBase):
    """
    部门网管字符串告警, 要求提供 apd_report_str_feature_id 参数
    """

    def __str__(self):
        return 'ApdStrReportCallback'

    def __call__(self, address: str, record: logging.LogRecord):
        apd_report_str_feature_id = getattr(record, 'apd_report_str_feature_id', None)
        if apd_report_str_feature_id is not None:
            try:
                pyanmagent.apdagent.APDAgent.report_str(
                    feature_id=apd_report_str_feature_id,
                    message=record.msg,
                    key_word='%s.%s' % (record.module, record.name))
            except Exception:
                self._logger.error('failed:\n%s' % traceback.format_exc())


class TofMailCallback(CriticalLogCallbackBase):
    """
    TOF邮件, 要求提供 mail_receivers 以及 mail_sender 参数

    .. note ::

        额外可提供 mail_tag 参数，作为邮件的前缀，方便归档
    """

    def __str__(self):
        return 'TofMailCallback'

    def __init__(self, tof: pytof.Tof, *args, **kwargs):
        super(TofMailCallback, self).__init__(*args, **kwargs)

        self._tof = tof

    def __call__(self, address: str, record: logging.LogRecord):
        mail_receivers = getattr(record, 'mail_receivers', None)
        mail_sender = getattr(record, 'mail_sender', None)
        if mail_receivers is None or mail_sender is None:
            return

        tag = getattr(record, 'email_tag', None)

        title = '%s[%s][%s][%s/%s]' % (
            '' if (tag is None) else ('[%s]' % tag),
            record.name,
            record.module,
            getattr(record, 'eth1', None),
            getattr(record, 'eth0', None))

        t_content = '''<table>
            <tr><td>客户端</td><td>%s</td></tr>
            <tr><td>进程/线程</td><td>%s(%s)-%s(%s)</td></tr>
            <tr><td>位置</td><td>%s-%s-%s</td></tr>
            <tr><td>级别</td><td>%s</td></tr>
            <tr><td>时间</td><td>%s</td></tr>
            <tr><td>信息</td><td><pre>%s</pre></td></tr>
        </table>'''

        content = t_content % (address,
                               record.processName,
                               record.process,
                               record.threadName,
                               record.thread,
                               record.filename,
                               record.funcName,
                               record.lineno,
                               record.levelname,
                               time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created)),
                               record.msg)

        try:
            self._tof.mail(
                sender=mail_sender,
                receivers=mail_receivers,
                title=title,
                content=content)
        except Exception:
            self._logger.error('failed to send mail:\n%s' % traceback.format_exc())


class CallbacksChain(CriticalLogCallbackBase):
    """
    回调函数链，会跳过其中失败的回调

    用例::

        logger = logging.getLogger()

        cbchain = CallbacksChain(logger)
        cbc.chain.add(ApdStrReportCallback(logger)).add(TofMailCallback(logger))
    """

    def __init__(self, *args, **kwargs):
        super(CallbacksChain, self).__init__(*args, **kwargs)

        self._cbs = []  # type: List[CriticalLogCallbackBase]

    def add(self, cb: CriticalLogCallbackBase):
        """

        :param cb: 回调函数对象
        :return: self
        :rtype: CallbacksChain
        """
        self._cbs.append(cb)
        return self

    def __call__(self, address: str, record: logging.LogRecord):
        for cb in self._cbs:
            self._logger.info('executing %s' % str(cb))
            try:
                cb(address=address, record=record)
            except Exception:
                self._logger.error('failed:\n%s' % traceback.format_exc())
