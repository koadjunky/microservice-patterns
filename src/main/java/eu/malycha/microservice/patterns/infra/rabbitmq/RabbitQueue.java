package eu.malycha.microservice.patterns.infra.rabbitmq;

import java.io.IOException;

public class RabbitQueue {

    private String queueName;
    private boolean durable;
    private boolean exclusive;
    private boolean autoDelete;

    private RabbitQueue(Builder builder) {
        queueName = builder.queueName;
        durable = builder.durable;
        exclusive = builder.exclusive;
        autoDelete = builder.autoDelete;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(RabbitQueue copy) {
        Builder builder = new Builder();
        builder.queueName = copy.queueName;
        builder.durable = copy.durable;
        builder.exclusive = copy.exclusive;
        builder.autoDelete = copy.autoDelete;
        return builder;
    }

    public void declare(RabbitConsumeChannel channel) throws IOException {
        channel.declareQueue(queueName, durable, exclusive, autoDelete);
    }

    public static final class Builder {
        private String queueName;
        private boolean durable;
        private boolean exclusive;
        private boolean autoDelete;

        private Builder() {
        }

        public Builder setQueueName(String val) {
            queueName = val;
            return this;
        }

        public Builder setDurable(boolean val) {
            durable = val;
            return this;
        }

        public Builder setExclusive(boolean val) {
            exclusive = val;
            return this;
        }

        public Builder setAutoDelete(boolean val) {
            autoDelete = val;
            return this;
        }

        public RabbitQueue build() {
            return new RabbitQueue(this);
        }
    }
}
