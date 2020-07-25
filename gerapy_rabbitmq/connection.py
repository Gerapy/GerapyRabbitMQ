# -*- coding: utf-8 -*-
from gerapy_rabbitmq.defaults import RABBITMQ_CONNECTION_PARAMETERS

try:
    import pika
except ImportError:
    raise ImportError("Please install pika before running gerapy-rabbitmq.")

import logging

logger = logging.getLogger('pika')
logger.setLevel(logging.WARNING)


def from_settings(settings):
    """
    generate connection of rabbitmq
    :param settings:
    :return:
    """
    connection_parameters = settings.get('RABBITMQ_CONNECTION_PARAMETERS', RABBITMQ_CONNECTION_PARAMETERS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(**connection_parameters))
    channel = connection.channel()
    return channel
