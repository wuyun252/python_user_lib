# coding: utf-8
import inspect
import sys
import time
import logging
import threading
import traceback
import tornado.gen
import tornado.web
import xmlrpc.client
import tornado.ioloop
import tornado.options
from tornado.log import gen_log
from typing import Dict, Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import pycl5


class XmlRpcRequestStat(object):

    @staticmethod
    def _compose_obj(uri: str, func_name: str) -> str:
        return 'xmlrpc_path=%s|xmlrpc_func_name=%s' % (uri, func_name)

    def __init__(self, apd_module: str):
        assert len(apd_module) > 0, 'require non-empty apd_module'
        self._apd_module = apd_module
        self._data = defaultdict(lambda: defaultdict(lambda: 0))

    def _get_obj(self, uri: str, func_name: str) -> Dict[str, int]:
        return self._data[XmlRpcRequestStat._compose_obj(uri=uri, func_name=func_name)]

    def init_apd_data(self, uri: str, func_name: str):
        s = self._get_obj(uri=uri, func_name=func_name)
        s.setdefault('request_count', 0)
        s.setdefault('request_exception_count', 0)
        s.setdefault('request_fault_count', 0)
        s.setdefault('request_succ_count', 0)
        s.setdefault('request_latency_ms', 0)

    def record_exception(self, uri: str, func_name: str):
        s = self._get_obj(uri=uri, func_name=func_name)
        s['request_count'] += 1
        s['request_exception_count'] += 1

    def record_fault(self, uri: str, func_name: str):
        s = self._get_obj(uri=uri, func_name=func_name)
        s['request_count'] += 1
        s['request_fault_count'] += 1

    def record_access(self, uri: str, func_name: str, latency_ms: int):
        s = self._get_obj(uri=uri, func_name=func_name)
        s['request_count'] += 1
        s['request_succ_count'] += 1
        s['request_latency_ms'] += latency_ms

    def report_to_apd(self):
        gen_log.info('reporting to apd agent')
        try:
            import pyanmagent

            pyanmagent.Client.report(
                tag=self._apd_module,
                data_dict=self._data
            )

            for metric in self._data:
                self._data[metric] = dict.fromkeys(self._data[metric].keys(), 0)
        except AssertionError:
            gen_log.error('failed to do report')
        except Exception as e:
            gen_log.critical('unknown error: %s\n%s' % (e, traceback.format_exc()))


class _XmlRpcInterfaceDescriptor(object):

    @classmethod
    def create(cls, interface_module):
        sig = getattr(interface_module, 'XMLRPC_SIGNATURE')
        return cls(version=sig['version'], name=sig['name'])

    def __init__(self, version: int, name: str):
        self._version = version
        self._name = name

    @property
    def version(self) -> int:
        return self._version

    @property
    def name(self) -> str:
        return self._name

    @property
    def rpc_path(self) -> str:
        return '/xmlrpc/%s/%d' % (self.name, self.version)


class _XmlRpcDispatcher(object):

    @staticmethod
    def resolve_name(instance, name: str):
        obj = instance
        for attr in name.split('.'):
            if attr.startswith('_'):
                raise AttributeError(
                    'attempt to access private attribute "%s"' % attr
                )
            else:
                obj = getattr(obj, attr)
        return obj

    def __init__(self, instance):
        self._instance = instance

    def get_method(self, name: str):
        try:
            return self.__class__.resolve_name(instance=self._instance, name=name)
        except AttributeError:
            pass
        except Exception:
            raise Exception('method "%s" is not supported: %s' % name)


class _XmlRpcRequestHandler(tornado.web.RequestHandler):

    @staticmethod
    def get_time_ms() -> int:
        return int(time.time() * 1000)

    def initialize(self, **kwargs):
        self._dispatcher = kwargs.get('dispatcher')
        """ :type: _XmlRpcDispatcher"""
        assert self._dispatcher is not None, 'dispatcher required'

        self._executor = kwargs.get('thread_executor')
        """ :type: ThreadPoolExecutor"""

        self._request_stat = kwargs.get('request_stat')
        """ :type: XmlRpcRequestStat"""

    @staticmethod
    def dumps(response, **kwargs):
        kwargs.update({
            'allow_none': True
        })
        return xmlrpc.client.dumps(response, **kwargs)

    def prepare(self):
        self._begin_time_ms = _XmlRpcRequestHandler.get_time_ms()
        self._func_name = 'FNAME'
        self._has_exception = False
        self._has_fault = False

    def on_finish(self):
        if self._request_stat is not None:
            if self._has_exception:
                self._request_stat.record_exception(uri=self.request.uri, func_name=self._func_name)
            elif self._has_fault:
                self._request_stat.record_fault(uri=self.request.uri, func_name=self._func_name)
            else:
                delta_time_ms = _XmlRpcRequestHandler.get_time_ms() - self._begin_time_ms
                self._request_stat.record_access(
                    uri=self.request.uri, func_name=self._func_name, latency_ms=delta_time_ms)

    @tornado.gen.coroutine
    def post(self):
        try:
            (args, kwargs), func_name = xmlrpc.client.loads(data=self.request.body.decode())
            self._func_name = func_name

            func = self._dispatcher.get_method(name=func_name)
            if self._executor is None:
                response = func(*args, **kwargs)
            else:
                response = yield self._executor.submit(func, *args, **kwargs)
            response = _XmlRpcRequestHandler.dumps(response=(response,), methodresponse=1)
        except xmlrpc.client.Fault as fault:
            gen_log.error(traceback.format_exc())
            self._has_fault = True
            response = _XmlRpcRequestHandler.dumps(fault)
        except Exception:
            gen_log.error(traceback.format_exc())
            self._has_exception = True
            exc_type, exc_value, exc_tb = sys.exc_info()
            response = _XmlRpcRequestHandler.dumps(xmlrpc.client.Fault(1, "%s:%s" % (exc_type, exc_value)))

        self.content_type = 'text/xml'
        self.write(response)


class _ClientServerProxy(object):

    def __init__(self, uri: str):
        self._s = xmlrpc.client.Server(uri)

    def __getattr__(self, item):
        call_proxy = getattr(self._s, item)

        def _call(*_args, **_kwargs):
            nonlocal call_proxy
            # xmlrpc原生不支持keyword arguments，这里加一个层次，将任何调用转换成两个positional参数的调用
            # 约定服务端获取到的总是(args, kwargs)
            return call_proxy(_args, _kwargs)

        return _call


class _CL5ClientServerProxy(object):

    def __init__(self, path: str, cl5_client: pycl5.CL5Client):
        self._path = path
        self._cl5_client = cl5_client
        self._server_memo = {}

    def __getattr__(self, item):

        def _call(*_args, **_kwargs):
            nonlocal item
            result, exception_obj = None, None
            with self._cl5_client.get_route(timeout_seconds=None) as (host, port):
                s = self._server_memo.get((host, port))
                if s is None:
                    s = xmlrpc.client.Server(uri='http://%s:%d%s' % (host, port, self._path))
                    self._server_memo[(host, port)] = s

                try:
                    # xmlrpc原生不支持keyword arguments，这里加一个层次，将任何调用转换成两个positional参数的调用
                    # 约定服务端获取到的总是(args, kwargs)
                    result = getattr(s, item)(_args, _kwargs)
                except OSError as e:
                    exception_obj = e
                    raise pycl5.CL5NetworkException()
            if exception_obj is not None:
                raise exception_obj
            return result

        return _call


class ClientFactory:

    _class_memo = {}
    _class_memo_lock = threading.Lock()

    @classmethod
    def create_client(cls, interface_module, svr: str, port: int):
        """NOTE: returned object is not thread-safe."""
        desc = _XmlRpcInterfaceDescriptor.create(interface_module=interface_module)
        class_name = '%s_rpc_client_class' % desc.name
        with cls._class_memo_lock:
            if class_name not in cls._class_memo:
                cls_obj = type(class_name, (_ClientServerProxy,), {})
            else:
                cls_obj = cls._class_memo[class_name]
            return cls_obj(uri='http://%s:%d%s' % (svr, port, desc.rpc_path))

    @classmethod
    def create_cl5_client(cls, interface_module: str, cl5_client: pycl5.CL5Client):
        """NOTE: returned object is not thread-safe."""
        desc = _XmlRpcInterfaceDescriptor.create(interface_module=interface_module)
        class_name = '%s_rpc_cl5_client_class' % desc.name
        with cls._class_memo_lock:
            if class_name not in cls._class_memo:
                cls_obj = type(class_name, (_CL5ClientServerProxy,), {})
            else:
                cls_obj = cls._class_memo[class_name]
            return cls_obj(path=desc.rpc_path, cl5_client=cl5_client)


class Server:

    @staticmethod
    def get_func_name(instance):
        result = []
        for name, __ in inspect.getmembers(instance, inspect.isroutine):
            if not name.startswith('_'):
                result.append(name)
        for name, member in instance.__dict__.items():
            if not name.startswith('_'):
                mem_funcs = Server.get_func_name(member)
                result.extend(["%s.%s" % (name, func_name) for func_name in mem_funcs])
        return result

    @staticmethod
    def init_tornado_app(
            request_stat: Optional[XmlRpcRequestStat],
            settings: Dict[object, object],
            n_concurrency: int) -> tornado.web.Application:
        thread_executor = None
        if n_concurrency > 1:
            thread_executor = ThreadPoolExecutor(max_workers=n_concurrency)

        assert len(settings) > 0, 'setting required'
        handlers = []
        registered_paths = set()
        for interface_module, impl_object in settings.items():
            desc = _XmlRpcInterfaceDescriptor.create(interface_module=interface_module)
            rpc_path = desc.rpc_path
            assert rpc_path not in registered_paths, 'duplicated signature: %s' % rpc_path
            registered_paths.add(rpc_path)
            handlers.append((
                rpc_path,
                _XmlRpcRequestHandler,
                {
                    'dispatcher': _XmlRpcDispatcher(instance=impl_object),
                    'thread_executor': thread_executor,
                    'request_stat': request_stat
                }
            ))
            if request_stat is not None:
                for func in Server.get_func_name(instance=impl_object):
                    request_stat.init_apd_data(uri=rpc_path, func_name=func)

        return tornado.web.Application(handlers=handlers)

    def __init__(self,
                 request_stat: Optional[XmlRpcRequestStat],
                 settings: Dict[object, object],
                 svr: str, port: int,
                 n_concurrency: int,
                 ioloop=tornado.ioloop.IOLoop.current()):
        self._request_stat = request_stat
        self._settings = settings
        self._svr = svr
        self._port = port
        self._ioloop = ioloop
        self._concurrency = n_concurrency

    def start_server(self):
        tornado.options.parse_command_line(sys.argv[1:])

        if self._request_stat is not None:
            tornado.ioloop.PeriodicCallback(self._request_stat.report_to_apd, 60000).start()
        app = Server.init_tornado_app(
            request_stat=self._request_stat, settings=self._settings, n_concurrency=self._concurrency)
        app.listen(self._port, address=self._svr)
        self._ioloop.start()
