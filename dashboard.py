import json
import os
import tornado
from base import BaseRequestHandler, remove_none_values, serialize_job, serialize_date, pagination_window
from functools import wraps
from rq import Queue, Worker
from rq import cancel_job, requeue_job
from rq import get_failed_queue
from tornado.options import define, options


define("port", default=8989, help="port number", type=int)
define("poll_interval", default=1000, help="polling interval to redis", type=int)
define("debug", default=0, help="For debugging in dev", type=int)
define("static_prefix", default="/static/", help="static contents url", type=str)
define("url_prefix", default="/rq", help="Url prefix", type=str)


def jsonify(f):
    @wraps(f)
    def _wrapped(*args, **kwargs):
        try:
            result_dict = f(*args, **kwargs)
        except Exception as e:
            result_dict = dict(status='error')
            if options.debug:
                result_dict['reason'] = str(e)
                from traceback import format_exc
                result_dict['exc_info'] = format_exc()
        return json_me(result_dict)
    return _wrapped


def json_me(obj):
	return json.dumps(obj)


def serialize_queues(queues):
    return [dict(name=q.name, count=q.count, url=options.url_prefix + "/" + q.name) for q in queues]

class TornadoApplication(tornado.web.Application):

    def __init__(self):
        handlers = [
            (r"/?", HealthHandler),
            (r"/job/(.*)/cancel/?", CancelJobHandler),
            (r"/job/(.*)/requeue/?", RequeueJobHandler),
            (r"/requeue-all/?", RequeueAllHandler),
            (r"/queue/(.*)/empty/?", EmptyQueueHandler),
            (r"/queue/(.*)/compact/?", CompactQueueHandler),
            (r"/queues.json", ListQueuesHandler),
            (r"/jobs/(.*)/(.*).json", ListJobsHandler),
            (r"/workers.json", ListWorkersHandler),
            (r"/?([^/]*)/?(.*)/?", OverViewHandler),    
        ]
        if options.url_prefix:
            handlers = [((options.url_prefix + handler[0]) if handler[0] != "/?" else (handler[0]), handler[1]) for handler in handlers]

        settings = dict(
            autoescape=None,
            debug=options.debug,
            gzip=True,
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            static_url_prefix=options.static_prefix,
        )   
        tornado.web.Application.__init__(self, handlers, **settings)


class HealthHandler(BaseRequestHandler):
	
	def get(self):
		self.finish("RQ Dashboard health check")

class OverViewHandler(BaseRequestHandler):
	
    def get(self, queue_name=None, page=None):
        if not queue_name:
            queue_name = None
        if not page:
        	page = 1
        else:
        	page = int(page)
        if queue_name is None:
        	# Show the failed queue by default if it contains any jobs
            failed = Queue('failed')
            if not failed.is_empty():
                queue = failed
            else:
                queue = Queue()	
        else:
            queue = Queue(queue_name)
        self.render('rq_dashboard/dashboard.html',
                    workers=Worker.all(),
                    queue=queue,
                    page=page,
                    queues=Queue.all(),
                    rq_url_prefix=options.url_prefix,
                    poll_interval=options.poll_interval,)


class CancelJobHandler(BaseRequestHandler):

    @staticmethod
    @jsonify
    def cancel_rq_job(job_id):
        assert job_id
        cancel_job(job_id)
        return dict(status='OK')

    def post(self, job_id):
        json_response = CancelJobHandler.cancel_rq_job(job_id)
        self.finish(json_response)
        return


class RequeueJobHandler(BaseRequestHandler):

    @staticmethod
    @jsonify
    def requeue_rq_job(job_id):
        assert job_id
        requeue_job(job_id)
        return dict(status='OK')

    def post(self, job_id):
        json_response = RequeueJobHandler.requeue_rq_job(job_id)
        self.finish(json_response)
        return


class RequeueAllHandler(BaseRequestHandler):

    @staticmethod
    @jsonify
    def requeue_all_rq_jobs():
        fq = get_failed_queue()
        job_ids = fq.job_ids
        count = len(job_ids)
        for job_id in job_ids:
            requeue_job(job_id)

        return dict(status='OK', count=count)

    def get(self):
        self.post()

    def post(self):
        json_response = RequeueAllHandler.requeue_all_rq_jobs()
        self.finish(json_response)


class EmptyQueueHandler(BaseRequestHandler):

    @staticmethod
    @jsonify
    def empty_rq_queue(queue_name):
        q = Queue(queue_name)
        q.empty()
        return dict(status='OK')

    def post(self, queue_name):
        json_response = EmptyQueueHandler.empty_rq_queue(queue_name)
        self.finish(json_response)


class CompactQueueHandler(BaseRequestHandler):

    @staticmethod
    @jsonify
    def compact_rq_queue(queue_name):
        q = Queue(queue_name)
        q.compact()
        return dict(status='OK')

    def post(self, queue_name):
        json_response = CompactQueueHandler.compact_rq_queue(queue_name)
        self.finish(json_response)


class ListQueuesHandler(BaseRequestHandler):

    @staticmethod
    @jsonify
    def list_rq_queues():
        queues = serialize_queues(sorted(Queue.all()))
        return dict(queues=queues)

    def get(self):
        json_response = ListQueuesHandler.list_rq_queues()
        self.finish(json_response)


class ListJobsHandler(BaseRequestHandler):

    @staticmethod
    @jsonify
    def list_rq_jobs(queue_name, page):
        current_page = int(page)
        queue = Queue(queue_name)
        per_page = 5
        total_items = queue.count
        pages_numbers_in_window = pagination_window(
            total_items, current_page, per_page)
        pages_in_window = [dict(number=p, url=options.url_prefix +
                                "/" + queue_name + "/" + str(p)) for p in pages_numbers_in_window]
        last_page = int(ceil(total_items / float(per_page)))

        prev_page = None
        if current_page > 1:
            prev_page = dict(
                url=options.url_prefix + "/" + queue_name + "/" + str((current_page - 1)))

        next_page = None
        if current_page < last_page:
            next_page = dict(
                url=options.url_prefix + "/" + queue_name + "/" + str((current_page + 1)))

        pagination = remove_none_values(
            dict(pages_in_window=pages_in_window,
                 next_page=next_page,
                 prev_page=prev_page))

        offset = (current_page - 1) * per_page
        jobs = [serialize_job(job) for job in queue.get_jobs(offset, per_page)]
        return dict(name=queue.name, jobs=jobs, pagination=pagination)

    def get(self, queue_name, page):
        json_response = ListJobsHandler.list_rq_jobs(queue_name, page)
        self.finish(json_response)


class ListWorkersHandler(BaseRequestHandler):

    @staticmethod
    @jsonify
    def list_rq_workers():
        def serialize_queue_names(worker):
            return [q.name for q in worker.queues]

        workers = [dict(name=worker.name, queues=serialize_queue_names(worker),
                        state=worker.get_state()) for worker in Worker.all()]
        return dict(workers=workers)

    def get(self):
        json_response = ListWorkersHandler.list_rq_workers()
        self.finish(json_response)


def main():
    tornado.options.parse_command_line()
    TornadoApplication().listen(options.port, xheaders=True)
    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
