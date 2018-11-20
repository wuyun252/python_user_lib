# coding: utf-8
import logging.handlers

import pyslogger.handler


if __name__ == '__main__':
    log = logging.getLogger('panic-server-tester')
    log.setLevel(logging.DEBUG)

    log.addHandler(pyslogger.handler.SHandler(
        cl5_sid=None,
        address=('10.120.91.168', 6666),
        opt_data={'mail_receivers': 'bensonliu', 'mail_sender': 'bensonliu'}))

    log.info('info 中文')
    for i in range(2):
        log.critical('critical 中文[%d]' % i)
