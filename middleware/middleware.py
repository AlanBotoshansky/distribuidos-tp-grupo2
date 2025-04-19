import pika
import logging

HOST = 'rabbitmq'
EXCHANGE_TYPE = 'fanout'
PREFETCH_COUNT = 1

class Middleware:
    def __init__(self, callback_function=None, callback_args=(), input_queues=[], output_exchange=None):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
        self._channel = self._connection.channel()
        self._callback_function = callback_function
        self._callback_args = callback_args
        self._input_queues = input_queues
        self._output_exchange = output_exchange
        
        self.__declare_input_queues()
        self.__declare_output_exchange()
        
    def __declare_input_queues(self):
        self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)    
        for queue, exchange in self._input_queues:
            self._channel.queue_declare(queue=queue)
            if exchange:
                self._channel.exchange_declare(exchange=exchange, exchange_type=EXCHANGE_TYPE)
                self._channel.queue_bind(exchange=exchange, queue=queue)
            
            self._channel.basic_consume(queue=queue, on_message_callback=self.__wrapper_callback_function())
            
    def __wrapper_callback_function(self):
        def callback(ch, method, properties, body):
            self._callback_function(body, *self._callback_args)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        return callback
        
    def __declare_output_exchange(self):
        if self._output_exchange is None:
            return
        self._channel.exchange_declare(exchange=self._output_exchange, exchange_type=EXCHANGE_TYPE)
        
    def send_message(self, msg, exchange=None):
        if self._output_exchange is None and exchange is None:
            return
        if exchange is None:
            self._channel.basic_publish(exchange=self._output_exchange, routing_key='', body=msg)
        else:
            self._channel.exchange_declare(exchange=exchange, exchange_type=EXCHANGE_TYPE)
            self._channel.basic_publish(exchange=exchange, routing_key='', body=msg)
        
    def reenqueue_message(self, msg):
        for queue, _ in self._input_queues:
            self._channel.basic_publish(exchange='', routing_key=queue, body=msg)
        
    def handle_messages(self):
        try:
            self._channel.start_consuming()
        except OSError:
            return
            
    def stop_handling_messages(self):
        self._channel.stop_consuming()
        logging.info("action: middleware_stop_consuming | result: success")

    def close_connection(self):
        self._channel.close()
        self._connection.close()
        logging.info("action: middleware_close_connection | result: success")