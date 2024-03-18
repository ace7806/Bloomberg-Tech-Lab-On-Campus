from consumer_interface import mqConsumerInterface
import pika
import os

class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        # Save parameters to class variables
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.channel = None
        self.connection = None
        # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        channel = connection.channel()
        self.channel = channel
        # Create Queue if not already present
        channel.queue_declare(queue=self.queue_name)
        # Create the exchange if not already present
        channel.exchange_declare(
            exchange=self.exchange_name, exchange_type='topic'
        )
        # Bind Binding Key to Queue on the exchange
        channel.queue_bind(
            queue= self.queue_name,
            routing_key= self.binding_key,
            exchange=self.exchange_name,
        )
        self.connection = connection
        # Set-up Callback function for receiving messages
        channel.basic_consume(
    self.queue_name, self.on_message_callback, auto_ack=False
) 
        
        

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        # Acknowledge and print message
        channel.basic_ack(method_frame.delivery_tag,False)
        print(body)
        # Close channel and connection
        channel.close()
        self.connection.close()

        
        pass

    def startConsuming(self) -> None:
        self.channel.start_consuming()
        pass
