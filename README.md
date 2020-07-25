# Gerapy RabbitMQ

This is a package for supporting distribution in Scrapy using RabbitMQ, also this
package is a module in [Gerapy](https://github.com/Gerapy/Gerapy).

## Installation

You can install with this command:

```shell script
pip3 install gerapy-rabbitmq
```

## Usage

Required configuration:

```python
# Use RabbitMQ for queue
SCHEDULER = "gerapy_rabbitmq.scheduler.Scheduler"
SCHEDULER_QUEUE_KEY = '%(spider)s_requests'

# RabbitMQ Connection Parameters, see https://pika.readthedocs.io/en/stable/modules/parameters.html
RABBITMQ_CONNECTION_PARAMETERS = {
    'host': 'localhost'
}

# Use Redis for dupefilter
DUPEFILTER_CLASS = "gerapy_redis.dupefilter.RFPDupeFilter"
SCHEDULER_DUPEFILTER_KEY = '%(spider)s:dupefilter'
```

Optional configuration:

```python
# RabbitMQ Queue Configuration
SCHEDULER_QUEUE_DURABLE = True
SCHEDULER_QUEUE_MAX_PRIORITY = 100
SCHEDULER_QUEUE_PRIORITY_OFFSET = 30
SCHEDULER_QUEUE_FORCE_FLUSH = True
SCHEDULER_PERSIST = False
SCHEDULER_IDLE_BEFORE_CLOSE = 0
SCHEDULER_FLUSH_ON_START = False
SCHEDULER_PRE_ENQUEUE_ALL_START_REQUESTS = True
```

## More

For more detail, you can refer to [example](./example).

## RabbitMQ Preview

![](https://qiniu.cuiqingcai.com/zjc82.png)