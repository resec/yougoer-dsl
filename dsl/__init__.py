from .settings import Settings
from .took import Commander
from .took import asyn
from . import json

stemp = Settings()
stemp.setmodule('dsl.setting')
settings = stemp
settings.freeze()
del stemp

#asyn = asyn.init(settings)

commander = Commander.instance(settings)

def serive(request):
	result = commander.execute(request)
	#print('dsl.serive result: %s' % result)
	return result
