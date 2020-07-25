import pika
from pika.exceptions import ChannelClosedByBroker
from scrapy.utils.reqser import request_to_dict, request_from_dict

from gerapy_rabbitmq.defaults import SCHEDULER_QUEUE_MAX_PRIORITY, SCHEDULER_QUEUE_PRIORITY_OFFSET, \
    SCHEDULER_QUEUE_DURABLE, SCHEDULER_QUEUE_FORCE_FLUSH

try:
    import cPickle as pickle
except ImportError:
    import pickle

import logging

logger = logging.getLogger(__name__)


class Base(object):
    
    def __init__(self, server, spider, key):
        """
        init rabbitmq queue
        :param server: pika channel
        :param spider: spider object
        :param key: queue name
        """
        self.server = server
        self.spider = spider
        self.key = key
        self.spider.log('Using key %s as RabbitMQ queue name' % self.key)
    
    def _encode_request(self, request):
        """
        encode request
        :param request:
        :return:
        """
        return pickle.dumps(request_to_dict(request, self.spider), protocol=-1)
    
    def _decode_request(self, encoded_request):
        """
        decode request
        :param encoded_request:
        :return:
        """
        return request_from_dict(pickle.loads(encoded_request), self.spider)
    
    def __len__(self):
        """
        length of queue
        :return:
        """
        raise NotImplementedError
    
    def push(self, request):
        """
        add request
        :param request:
        :return:
        """
        raise NotImplementedError
    
    def pop(self):
        """
        pop request
        :return:
        """
        raise NotImplementedError
    
    def clear(self):
        """
        clear queue
        :return:
        """
        self.server.queue_purge(self.key)


class PriorityQueue(Base):
    """
    Queue with priority
    """
    
    def __init__(self, server, spider, key,
                 max_priority=SCHEDULER_QUEUE_MAX_PRIORITY,
                 durable=SCHEDULER_QUEUE_DURABLE,
                 force_flush=SCHEDULER_QUEUE_FORCE_FLUSH,
                 priority_offset=SCHEDULER_QUEUE_PRIORITY_OFFSET):
        """
        init rabbitmq queue
        :param server: pika channel
        :param spider: spider object
        :param key: queue name
        """
        self.inited = False
        logger.debug('Queue args %s', {
            'server': server,
            'spider': spider,
            'key': key,
            'max_priority': max_priority,
            'durable': durable,
            'force_flush': force_flush,
            'priority_offset': priority_offset
        })
        self.durable = durable
        super(PriorityQueue, self).__init__(server, spider, key)
        try:
            self.queue_operator = self.server.queue_declare(queue=self.key, arguments={
                'x-max-priority': max_priority
            }, durable=durable)
            logger.debug('Queue operator %s', self.queue_operator)
            self.inited = True
        except ChannelClosedByBroker as e:
            logger.error("You have changed queue configuration, you "
                         "must delete queue manually or set `SCHEDULER_QUEUE_FORCE_FLUSH` "
                         "to True, error detail %s" % str(e.args), exc_info=True)
            self.inited = False
        self.priority_offset = priority_offset
    
    def __len__(self):
        """
        get length of queue
        :return:
        """
        if not hasattr(self, 'queue_operator'):
            return 0
        return self.queue_operator.method.message_count
    
    def push(self, request):
        """
        push request to queue
        :param request:
        :return:
        """
        priority = request.priority + self.priority_offset
        # set min priority in queue to 0
        if priority < 0:
            priority = 0
        delivery_mode = 2 if self.durable else None
        self.server.basic_publish(
            exchange='',
            properties=pika.BasicProperties(
                priority=priority,
                delivery_mode=delivery_mode
            ),
            routing_key=self.key,
            body=self._encode_request(request)
        )
    
    def pop(self):
        """
        pop request from queue
        :return:
        """
        method_frame, header, body = self.server.basic_get(queue=self.key, auto_ack=True)
        
        if body:
            return self._decode_request(body)
