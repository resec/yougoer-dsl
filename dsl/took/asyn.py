from concurrent.futures import ProcessPoolExecutor
from functools import partial, wraps

EXECUTOR = ProcessPoolExecutor(max_workers=4)

def unblock(f):

    @wraps(f)
    def wrapper(*args, **kwargs):
        #self = args[0]

        print('unblock.wrapper args: %s kwargs: %s' % (args, kwargs))

        '''
        def callback(future):
            print(future)
            result = future.result()
            print(result)
            print(type(result))
            self.write(result)
            self.finish()
        '''

        return EXECUTOR.submit(
            partial(f, *args, **kwargs)
        )
        '''
        .add_done_callback(
            lambda future: 
                tornado.ioloop.IOLoop.instance().add_callback(
                    partial(callback, future)
                )
        )
        '''

    return wrapper