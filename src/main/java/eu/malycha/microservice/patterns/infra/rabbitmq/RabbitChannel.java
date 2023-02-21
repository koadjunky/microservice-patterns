package eu.malycha.microservice.patterns.infra.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class RabbitChannel {

    private final Channel channel;

    // One thread per channel
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public RabbitChannel(Channel channel) {
        this.channel = channel;
    }

    public Future<Void> publish(String exchangeName, String routingKey, byte[] message, Map<String, Object> headers) {
        return executorService.submit(() -> publishInternal(exchangeName, routingKey, message, headers));
    }

    public void attach(String queueName, RabbitConsumer consumer) throws IOException {
        channel.basicConsume(queueName, new RabbitChannelConsumer(channel, consumer));
    }

    private Void publishInternal(String exchangeName, String routingKey, byte[] message, Map<String, Object> headers) throws IOException {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
            .headers(headers)
            .timestamp(getTimestamp())
            .build();
        // TODO: Protect against channel closing in case of errors (i.e. non-existing exchange)
        channel.basicPublish(exchangeName, routingKey, properties, message);
        return null;
    }

    public void shutdown() throws IOException, TimeoutException {
        executorService.shutdown();
        channel.close();
    }

    private static Date getTimestamp() {
        return new Date(ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli());
    }

    public class RabbitChannelConsumer extends DefaultConsumer {

        private RabbitConsumer consumer;

        public RabbitChannelConsumer(Channel channel, RabbitConsumer consumer) {
            super(channel);
            this.consumer = consumer;
            consumer.setChannelConsumer(this);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            throws IOException
        {
            // TODO: submit to executor service
            long deliveryTag = envelope.getDeliveryTag();
            RabbitConsumer.ConsumptionResult result = consumer.consume(body);
            switch (result) {
                case COMPLETE -> channel.basicAck(deliveryTag, false);
                case FAILURE_NON_RECOVERABLE -> channel.basicNack(deliveryTag, false, false);
                case FAILURE_NEEDS_RETRY -> channel.basicNack(deliveryTag, false, true);
            }
        }

        // TODO: Handle cancel, etc
    }
}
