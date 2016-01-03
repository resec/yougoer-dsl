import dsl

import tornado.ioloop
import tornado.web
import tornado.httpserver
from tornado.options import define, options

import os.path

define("port", default=8008, help="run on the given port", type=int)

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

def main():
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
    main()
