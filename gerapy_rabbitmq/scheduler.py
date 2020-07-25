from . import connection as rabbitmq_connection
from scrapy.utils.misc import load_object
from .defaults import SCHEDULER_PERSIST, SCHEDULER_QUEUE_KEY, SCHEDULER_QUEUE_CLASS, SCHEDULER_IDLE_BEFORE_CLOSE, \
    SCHEDULER_FLUSH_ON_START, SCHEDULER_PRE_ENQUEUE_ALL_START_REQUESTS, SCHEDULER_QUEUE_MAX_PRIORITY, \
    SCHEDULER_QUEUE_PRIORITY_OFFSET, SCHEDULER_QUEUE_DURABLE, SCHEDULER_QUEUE_FORCE_FLUSH, \
    DUPEFILTER_CLASS
from .queue import PriorityQueue


class Scheduler(object):
    """
    Scheduler using RabbitMQ
    """
    
    def __init__(self, server, persist, queue_key, queue_cls, queue_durable, idle_before_close,
                 flush_on_start,
                 pre_enqueue_all_start_requests, queue_max_priority, queue_priority_offset, queue_force_flush,
                 dupefilter_cls,
                 *args,
                 **kwargs):
        """
        init scheduler
        :param server: pika connection channel
        :param persist: persist queue and df
        :param queue_key: queue name
        :param queue_cls: queue class
        :param queue_durable: queue durable
        :param idle_before_close: idle before close
        :param flush_on_start: clear queue and df when spider open
        :param args:
        :param kwargs:
        """
        self.server = server
        self.persist = persist
        self.queue_key = queue_key
        self.queue_cls = queue_cls
        self.queue_durable = queue_durable
        self.flush_on_start = flush_on_start
        self.pre_enqueue_all_start_requests = pre_enqueue_all_start_requests
        self.queue_max_priority = queue_max_priority
        self.queue_priority_offset = queue_priority_offset
        self.queue_force_flush = queue_force_flush
        self.idle_before_close = idle_before_close
        self.dupefilter_cls = dupefilter_cls
        self.stats = None
    
    def __len__(self):
        """
        get length of queue
        :return:
        """
        return len(self.queue)
    
    @classmethod
    def from_settings(cls, settings):
        """
        use settings to init cls
        :param settings:
        :return:
        """
        kwargs = dict()
        kwargs['persist'] = settings.get('SCHEDULER_PERSIST', SCHEDULER_PERSIST)
        kwargs['queue_key'] = settings.get('SCHEDULER_QUEUE_KEY', SCHEDULER_QUEUE_KEY)
        kwargs['queue_cls'] = load_object(settings.get('SCHEDULER_QUEUE_CLASS', SCHEDULER_QUEUE_CLASS))
        kwargs['queue_durable'] = settings.getbool('SCHEDULER_QUEUE_DURABLE', SCHEDULER_QUEUE_DURABLE)
        kwargs['queue_max_priority'] = settings.getint('SCHEDULER_QUEUE_MAX_PRIORITY',
                                                       SCHEDULER_QUEUE_MAX_PRIORITY)
        kwargs['queue_priority_offset'] = settings.getint('SCHEDULER_QUEUE_PRIORITY_OFFSET',
                                                          SCHEDULER_QUEUE_PRIORITY_OFFSET)
        kwargs['queue_force_flush'] = settings.getbool('SCHEDULER_QUEUE_FORCE_FLUSH', SCHEDULER_QUEUE_FORCE_FLUSH)
        kwargs['idle_before_close'] = settings.get('SCHEDULER_IDLE_BEFORE_CLOSE', SCHEDULER_IDLE_BEFORE_CLOSE)
        kwargs['flush_on_start'] = settings.getbool('SCHEDULER_FLUSH_ON_START', SCHEDULER_FLUSH_ON_START)
        kwargs['pre_enqueue_all_start_requests'] = settings.getbool('SCHEDULER_PRE_ENQUEUE_ALL_START_REQUESTS',
                                                                    SCHEDULER_PRE_ENQUEUE_ALL_START_REQUESTS)
        kwargs['dupefilter_cls'] = load_object(settings.get('DUPEFILTER_CLASS', DUPEFILTER_CLASS))
        kwargs['server'] = rabbitmq_connection.from_settings(settings)
        cls.settings = settings
        return cls(**kwargs)
    
    @classmethod
    def from_crawler(cls, crawler):
        """
        use settings to init crawler
        :param crawler:
        :return:
        """
        instance = cls.from_settings(crawler.settings)
        instance.stats = crawler.stats
        return instance
    
    def open(self, spider):
        """
        called when spider opened
        :param spider:
        :return:
        """
        self.spider = spider
        
        queue_kwargs = {
            'server': self.server,
            'spider': spider,
            'key': self.queue_key % {'spider': spider.name}
        }
        
        # add args according to type
        if self.queue_cls == PriorityQueue:
            queue_kwargs['max_priority'] = self.queue_max_priority
            queue_kwargs['priority_offset'] = self.queue_priority_offset
            queue_kwargs['durable'] = self.queue_durable
            queue_kwargs['force_flush'] = self.queue_force_flush
        
        self.queue = self.queue_cls(**queue_kwargs)
        
        if not self.queue.inited:
            spider.log('Queue inited failed')
            if self.queue_force_flush:
                spider.log('Force to refresh queue')
                server = rabbitmq_connection.from_settings(self.settings)
                server.queue_delete(self.queue.key)
                queue_kwargs['server'] = server
                self.queue = self.queue_cls(**queue_kwargs)
            else:
                spider.log('Close spider due to failed initialization of queue')
                spider.crawler.engine.close_spider(spider, reason='shutdown')
                return
        
        self.df = self.dupefilter_cls.from_spider(spider)
        
        if self.idle_before_close < 0:
            self.idle_before_close = 0
        
        if self.flush_on_start:
            spider.log('Flush on start')
            self.flush()
        
        if len(self.queue):
            spider.log("Resuming crawl (%d requests scheduled)" % len(self.queue))
        else:
            if not self.pre_enqueue_all_start_requests:
                spider.log("You set SCHEDULER_PRE_ENQUEUE_ALL_START_REQUESTS to False, skip enqueue all start requests")
                return
            spider.log("No requests in queue, pre enqueue all start requests")
            engine = spider.crawler.engine
            for request in engine.slot.start_requests:
                spider.log("Enqueue start request %s to queue" % request)
                engine.schedule(request, spider)
    
    def flush(self):
        """
        clear df and queue
        :return:
        """
        self.df.clear()
        self.queue.clear()
    
    def close(self, reason):
        """
        called when spider closed
        :param reason:
        :return:
        """
        if not self.persist:
            self.flush()
    
    def enqueue_request(self, request):
        """
        add request to queue
        :param request:
        :return:
        """
        if not request.dont_filter and self.df.request_seen(request):
            return
        if self.stats:
            self.stats.inc_value('scheduler/enqueued/rabbitmq', spider=self.spider)
        self.queue.push(request)
    
    def next_request(self):
        """
        get next request
        :return:
        """
        # block_pop_timeout = self.idle_before_close
        request = self.queue.pop()
        if request and self.stats:
            self.stats.inc_value('scheduler/dequeued/rabbitmq', spider=self.spider)
        return request
    
    def has_pending_requests(self):
        """
        has
        :return:
        """
        return len(self) > 0
