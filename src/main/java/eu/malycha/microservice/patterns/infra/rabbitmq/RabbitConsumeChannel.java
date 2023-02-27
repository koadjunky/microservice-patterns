package eu.malycha.microservice.patterns.infra.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class RabbitConsumeChannel {

    private final Channel channel;

    // One thread per channel
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    public RabbitConsumeChannel(Channel channel) {
        this.channel = channel;
    }

    public void shutdown() throws IOException, TimeoutException {
        executorService.shutdown();
        channel.close();
    }

    public void attach(String queueName, RabbitConsumer consumer) throws IOException {
        channel.basicConsume(queueName, new RabbitChannelConsumer(channel, consumer));
    }

    public class RabbitChannelConsumer extends DefaultConsumer {

        private static final Logger LOGGER = LoggerFactory.getLogger(RabbitChannelConsumer.class);

        private final RabbitConsumer consumer;

        public RabbitChannelConsumer(Channel channel, RabbitConsumer consumer) {
            super(channel);
            this.consumer = consumer;
            consumer.setChannelConsumer(this);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            long deliveryTag = envelope.getDeliveryTag();
            executorService.submit(() -> consumeAndAck(deliveryTag, body));
        }

        private void consumeAndAck(long deliveryTag, byte[] body) {
            try {
                RabbitConsumer.ConsumptionResult result = consumeWrapper(body);
                switch (result) {
                    case COMPLETE -> channel.basicAck(deliveryTag, false);
                    case FAILURE_NON_RECOVERABLE -> channel.basicNack(deliveryTag, false, false);
                    case FAILURE_NEEDS_RETRY -> channel.basicNack(deliveryTag, false, true);
                }
            } catch (IOException ex) {
                LOGGER.error("IOException when acking message. Will be re-delivered again.", ex);
            }
        }

        public RabbitConsumer.ConsumptionResult consumeWrapper(byte[] body) {
            try {
                return consumer.consume(body);
            } catch (Exception ex) {
                LOGGER.error("Exception when consuming message", ex);
                return RabbitConsumer.ConsumptionResult.FAILURE_NON_RECOVERABLE;
            }
        }

        // TODO: Handle cancel, etc
    }
}
