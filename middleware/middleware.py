import pika

class Middleware:
    def __init__(self, output_exchange=None, callback_function=None, ):
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self._channel = self._connection.channel()
        self._output_exchange = output_exchange
        self._callback_function = callback_function
        
        self.__declare_output_exchange()
        
    def __declare_output_exchange(self):
        if self._output_exchange is None:
            return
        self._channel.exchange_declare(exchange=self._output_exchange, exchange_type='fanout')
        
    def send_message(self, msg):
        self._channel.basic_publish(exchange=self._output_exchange, routing_key='', body=msg)
        
    def handle_messages(self):
        self._channel.start_consuming()

    def close_connection(self):
        self._channel.close()
        self._connection.close()