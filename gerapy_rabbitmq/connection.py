# -*- coding: utf-8 -*-
from gerapy_rabbitmq.defaults import RABBITMQ_CONNECTION_PARAMETERS, RABBITMQ_URL_PARAMETERS

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
    # firstly try to connect with url parameters
    url_parameters = settings.get('RABBITMQ_URL_PARAMETERS', RABBITMQ_URL_PARAMETERS)
    if url_parameters:
        connection = pika.BlockingConnection(pika.URLParameters(url_parameters))
        channel = connection.channel()
        return channel
    # secondly try to connect with connection parameters
    connection_parameters = settings.get('RABBITMQ_CONNECTION_PARAMETERS', RABBITMQ_CONNECTION_PARAMETERS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(**connection_parameters))
    channel = connection.channel()
    return channel
