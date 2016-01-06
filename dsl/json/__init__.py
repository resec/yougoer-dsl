import json
import decimal


class DSLJSONEncoder(json.JSONEncoder):


    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def loads(s, encoding=None, **kw):
	return json.loads(s, encoding, **kw)


def load(fp, **kw):
	return json.load(fp, **kw)


def dumps(obj, **kw):
	return json.dumps(obj, cls=DSLJSONEncoder, **kw)


def dump(obj, fp, **kw):
	return json.dump(obj, fp, cls=DSLJSONEncoder, **kw)


