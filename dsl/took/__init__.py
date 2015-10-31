from .util import walk_modules
from .asyn import unblock

import inspect

class Commander(object):


    @classmethod
    def instance(cls, settings):
        ins = cls()

        cls._operator = Operator.instance(settings)
        cls._settings = settings
        cls._instance = ins

        return ins


    @classmethod
    def current(cls):
        return cls._instance


    def execute(self, request):
        tkey = request['tkey']
        param = request['param']
        task = self._operator.task(tkey)

        print('get task %s' % task)

        result = {}
        steps = getattr(task, 'steps')

        if inspect.isgeneratorfunction(steps):
            for next in steps(param, result):
                if isinstance(next, list):
                    result, break_flag = self._execute_steps_list(next, param)
                    if break_flag:
                        break
                elif isinstance(next, Step):
                    result = self._execute_step(next, param)
                elif isinstance(next, dict):
                    result = next
                    break
                else:
                    self._raise_step_type_error(next)
        else:
            obj = steps(param, result)
            if not isinstance(obj, list):
                obj = [obj]
            result = self._execute_steps_list(obj, param)

        return result


    def _execute_steps_list(self, step_list, param):
        for next in step_list:
            if isinstance(next, Step):
                result = self._execute_step(next, param)
            elif isinstance(next, dict):
                return next, True
            else:
                self._raise_step_type_error(next)

        return result, False


    def _raise_step_type_error(self, obj):
        raise ValueError(
            "expect Task.steps() to be a Step generator, \
            or return a Step list, instance directly, \
            to end the task, expect a dict object or None, \
            not %s" % type(obj)
        )


    def _execute_step(self, step, param):
        result = None
        handler = self._operator.handler(step.hkey)

        print('execute step %s' % step)
        if 'asyn' in step and step['asyn']:
            print('asyn call %s with %s' % (handler, param))
            self._asyn_call(handler, step, param)
            result = {'status':'asyn task'}
        else: 
            print('syn call %s with %s' % (handler, param))
            result = handler.action(step, param)
            print(result)
        return result


    @unblock
    def _asyn_call(self, handler, step, param):
        return handler.action(step, param)


class Operator(object):


    @classmethod
    def instance(cls, settings):
        if getattr(cls, '_frozen', None):
            return cls._instance

        ins = cls()

        ins._settings = settings

        ins._handlers = ins._iter_cls(settings.getlist('HANDLER_MODULES'), ins._is_handler)
        ins._handlers_ins = ins._create_ins(ins._handlers)
        ins._listeners = ins._iter_cls(settings.getlist('LISTERNER_MODULES'), ins._is_listener)
        ins._listeners_ins = ins._create_ins(ins._listeners)
        ins._tasks = ins._iter_cls(settings.getlist('TASK_MODULES'), ins._is_task)
        ins._tasks_ins = ins._create_ins(ins._tasks)

        cls._instance = ins
        cls._frozen = True

        return cls._instance


    @classmethod
    def current(cls):
        return cls._instance


    def _iter_cls(self, modules, detail_check_fn):
        class_list = {}
        for name in modules:
            for module in walk_modules(name):
                for obj in vars(module).values():
                    if inspect.isclass(obj) and \
                        obj.__module__ == module.__name__:
                        key = detail_check_fn(obj)
                        if key:
                            class_list[key] = obj

        return class_list


    def _is_handler(self, cls):
        key = getattr(cls, 'hkey', None)
        action = getattr(cls, 'action', None)
        if action and callable(action):
            return key
    

    def _is_listener(self, cls):
        key = getattr(cls, 'lkey', None)
        before = getattr(cls, 'before', None)
        after = getattr(cls, 'after', None)
        if before and after and callable(before) and callable(after):
            return key


    def _is_task(self, cls):
        key = getattr(cls, 'tkey', None)
        steps = getattr(cls, 'steps', None)
        if steps and inspect.isgeneratorfunction(steps):
            return key


    def _create_ins(self, cls_map):
        def _ins(cls):
            ins = cls()
            ins.settings = self._settings
            return ins

        return {key:_ins(cls) for key, cls in cls_map.items()}


    def task(self, tkey):
        return self._tasks_ins[tkey]


    def handler(self, hkey):
        return self._handlers_ins[hkey]


    def listener(self, lkey):
        return self._listeners_ins[lkey]


def concern(*lkeys):
    def wrap(f):
        def wrapped_f(*args):
            operator = Operator.current()

            for lkey in lkeys:
                listener = operator.listener(lkey)
                if getattr(listener, 'before', None):
                    listener.before(f, *args)

            f(*args)

            for lkey in lkeys:
                listener = operator.listener(lkey)
                if getattr(listener, 'after', None):
                    listener.after(f, *args)

        return wrapped_f

    return wrap


class Step(dict):


    hkey = None


    def __init__(self, hkey=None):
        if hkey is not None:
            self.hkey = hkey
        elif not getattr(self, 'hkey', None):
            raise ValueError("%s must have a hkey" % type(self).__name__)
