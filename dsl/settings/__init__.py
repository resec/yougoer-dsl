import json
import copy
from importlib import import_module

class Settings(object):


    def __init__(self, values=None):
        self.frozen = False
        self.attributes = {}
        if values is not None:
            self.setdict(values)


    def __getitem__(self, opt_name):
        value = None
        if opt_name in self.attributes:
            value = self.attributes[opt_name]
        return value


    def get(self, name, default=None):
        return self[name] if self[name] is not None else default


    def getbool(self, name, default=False):
        """
        True is: 1, '1', True
        False is: 0, '0', False, None
        """
        return bool(int(self.get(name, default)))


    def getint(self, name, default=0):
        return int(self.get(name, default))


    def getfloat(self, name, default=0.0):
        return float(self.get(name, default))


    def getlist(self, name, default=None):
        value = self.get(name, default or [])
        if isinstance(value, str):
            value = value.split(',')
        return list(value)


    def getdict(self, name, default=None):
        value = self.get(name, default or {})
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value)


    def set(self, name, value):
        self._assert_mutability()
        self.attributes[name] = value


    def setdict(self, values):
        self._assert_mutability()
        for name, value in enumerate(values):
            self.set(name, value)


    def setmodule(self, module):
        self._assert_mutability()
        if isinstance(module, str):
            module = import_module(module)
        for key in dir(module):
            if key.isupper():
                self.set(key, getattr(module, key))


    def _assert_mutability(self):
        if self.frozen:
            raise TypeError("Trying to modify an immutable Settings object")


    def copy(self):
        return copy.deepcopy(self)


    def freeze(self):
        self.frozen = True


    def frozencopy(self):
        copy = self.copy()
        copy.freeze()
        return copy
        
