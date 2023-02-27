package eu.malycha.microservice.patterns.infra.rabbitmq;


public class RabbitConsumer {

    public enum ConsumptionResult {
        COMPLETE,
        FAILURE_NEEDS_RETRY,
        FAILURE_NON_RECOVERABLE
    }

    private RabbitConsumeChannel.RabbitChannelConsumer channelConsumer;

    ConsumptionResult consume(byte[] message) {
        return ConsumptionResult.FAILURE_NEEDS_RETRY;
    }

    public void setChannelConsumer(RabbitConsumeChannel.RabbitChannelConsumer channelConsumer) {
        this.channelConsumer = channelConsumer;
    }
}
