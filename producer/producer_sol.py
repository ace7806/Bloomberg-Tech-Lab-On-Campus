import pika
import os
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
            self.routing_key = routing_key
            self.exchange_name = exchange_name
            self.channel = None
            self.setupRMQConnection()
            
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = connection.channel()
        # Create the exchange if not already present
        exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')

        

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        if not self.channel:
            print('channel not set up')
            return
        
        self.channel.basic_publish(exchange = self.exchange_name,
                      routing_key = self.routing_key,
                      body = message)
        
