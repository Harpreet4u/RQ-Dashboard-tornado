# Tornado base file containing common methods.
import redis
import times
import tornado.web
from math import ceil
from rq import push_connection, pop_connection
from tornado.options import define, options

define("redis_host", default="127.0.0.1",
       help="Redis host ip address", type=str)
define("redis_port", default=6379, help="Redis port number", type=int)


def remove_none_values(input_dict):
    return dict([(key, val) for key, val in input_dict.items() if val is not None])


def serialize_date(dt):
    if dt is None:
        return None
    return times.format(dt, 'UTC')


def serialize_job(job):
    return dict(
        id=job.id,
        created_at=serialize_date(job.created_at),
        enqueued_at=serialize_date(job.enqueued_at),
        ended_at=serialize_date(job.ended_at),
        origin=job.origin,
        result=job._result,
        exc_info=job.exc_info,
        description=job.description)


def pagination_window(total_items, cur_page, per_page=5, window_size=10):
    all_pages = range(1, int(ceil(total_items / float(per_page))) + 1)
    results = all_pages

    if (window_size >= 1):
        pages_window_start = int(max(
            0, min(len(all_pages) - window_size, (cur_page - 1) - ceil(window_size / 2.0))))
        pages_window_end = int(pages_window_start + window_size)
        result = all_pages[pages_window_start:pages_window_end]

        return result


class BaseRequestHandler(tornado.web.RequestHandler):

    def __init__(self, application, request, **kwargs):
        super(BaseRequestHandler, self).__init__(application, request)

    def prepare(self):
        push_connection(
            redis.Redis(host=options.redis_host, port=options.redis_port))

    def on_finish(self):
        pop_connection()
