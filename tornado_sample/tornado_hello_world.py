# To check whether tornado is up and running. 
# Steps to execute:
# 1. Run "python tornado_hello_world.py" on terminal.
# 2. Open url: http://localhost:8888 in web browser.
import tornado.ioloop
import tornado.web
from tornado.options import define, options

define("port", default=8888, help="add port number", type=int)

class MainHandler(tornado.web.RequestHandler):
	
	def get(self):
		self.write("Hey, tornado is running fine.")

application = tornado.web.Application([
	(r"/", MainHandler),
])

if __name__ == "__main__":
	application.listen(options.port)
	tornado.ioloop.IOLoop.current().start()
