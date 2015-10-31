import dsl

import tornado.ioloop
import tornado.web

import os.path


class BaseHandler(tornado.web.RequestHandler):


    def prepare(self):
        if self.request.headers["Content-Type"] == "application/json":
            self.json_args = dsl.json.loads(self.request.body.decode('utf-8'))
        else:
            self.json_args = None


    def post(self):
        assert 'tkey' in self.json_args
        assert 'param' in self.json_args

        tkey = self.json_args['tkey']
        param = self.json_args['param']

        result = dsl.serive(self.json_args)
        #return result
        #print(type(result))
        self.write(dsl.json.dumps(result))
        self.finish()


application = tornado.web.Application(
    [
        (r"/", BaseHandler),
    ],
    debug=True
    )

if __name__ == "__main__":
    application.listen(8008)
    tornado.ioloop.IOLoop.current().start()
