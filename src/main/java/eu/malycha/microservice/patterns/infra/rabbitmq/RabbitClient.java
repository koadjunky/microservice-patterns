package eu.malycha.microservice.patterns.infra.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitClient.class);

    private final Connection publish;

    private final Connection consume;

    public RabbitClient(String username, String password, String vhost, String host, int port) throws IOException, TimeoutException {
        this.publish = createConnection(username, password, vhost, host, port);
        this.consume = createConnection(username, password, vhost, host, port);
    }

    private static Connection createConnection(String username, String password, String vhost, String host, int port) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(vhost);
        factory.setHost(host);
        factory.setPort(port);
        factory.setAutomaticRecoveryEnabled(true);

        return factory.newConnection();
    }
}
