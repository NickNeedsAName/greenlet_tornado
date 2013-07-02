"""
Everything related to using Greenlet with Tornado.
"""

import logging
import sys
import traceback
from functools import wraps, partial

import greenlet
import tornado.httpclient
import tornado.ioloop
import tornado.web

DONT_FINISH = 'DONTFINISH'

def greenlet_fetch(request, **kwargs):
    """
    To use this function, it must be called (either directly or indirectly) from a method wrapped by the greenlet_asynchronous decorator.

    The request arg may be either a string URL or an HTTPRequest object.
    If it is a string, any additional kwargs will be passed directly to AsyncHTTPClient.fetch().

    Returns an HTTPResponse object, or raises a tornado.httpclient.HTTPError exception on error (such as a timeout, or a non-200 response).
    """
    cli = tornado.httpclient.AsyncHTTPClient(max_clients=100)
    cli.max_clients = 100

    gr = greenlet.getcurrent()
    assert gr.parent is not None, "greenlet_fetch() can only be called (possibly indirectly) from a RequestHandler method wrapped by the greenlet_asynchronous decorator."
    def callback(response):
        # Make sure we are on the master greenlet before we switch.
        tornado.ioloop.IOLoop.instance().add_callback(partial(gr.switch, response))


    cli.fetch(request, callback, **kwargs)

    # Now, yield control back to the master greenlet, and wait for data to be sent to us.
    response = gr.parent.switch()

    # Raise the exception, if any.
    if response.error:
        try:
            url = request.url
        except Exception,e:
            url = request
        logging.warning("Error: %s for url %s" % (response.error, url))
        response.rethrow()
    return response


def greenlet_asynchronous(wrapped_method):
    """
    Decorator that allows you to make async calls as if they were synchronous, by pausing the callstack and resuming it later.

    This decorator is meant to be used on the get() and post() methods of tornado.web.RequestHandler subclasses.

    It does not make sense to use the tornado.web.asynchronous decorator as well as this decorator.
    The returned wrapper method will be asynchronous, but the wrapped method will be synchronous.
    The request will be finished automatically when the wrapped method returns.
    """
    @tornado.web.asynchronous
    @wraps(wrapped_method)
    def get_or_post_wrapper(self, *args, **kwargs):
        def greenlet_base_func():
            try:
                retval = wrapped_method(self, *args, **kwargs)
                # Sometimes you may want to call self.finish() before actually
                # being done with the request handler.  In that case the RequestHandler
                # ****MUST****** return DONT_FINISH so we don't call self.finish() twice.
                if retval == DONT_FINISH:
                    return
                self.finish()
            except Exception as e:
                if self.get_argument('admin_debug_mode','0') == '1':
                    self.write("Exception: %s<br/>" % e)
                    self.write("TB2: %s" % "<br/>".join(traceback.format_exception(*sys.exc_info())))
                raise
        gr = greenlet.greenlet(greenlet_base_func)
        gr.switch()
    return get_or_post_wrapper



# EXAMPLE USAGE:
# class RequestHandler(tornado.web.RequestHandler):
#   @greenlet_asynchronous
#   def get(self):
        # DO STUFF
#       response = greenlet_fetch(request+kwargs go here)

# Magically yields to IOLoop + comes back w/ the response.