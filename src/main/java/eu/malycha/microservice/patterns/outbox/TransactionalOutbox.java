package eu.malycha.microservice.patterns.outbox;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.FencedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionalOutbox {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalOutbox.class);

    private final HazelcastInstance hazelcast;
    private final String lockName;
    private final String outboxName;

    private TransactionalOutbox(HazelcastInstance hazelcast, String lockName, String outboxName) {
        this.hazelcast = hazelcast;
        this.lockName = lockName;
        this.outboxName = outboxName;
    }

    public void process(Runnable messageProcessor) {
        try {
            trySendMessages(messageProcessor);
        } catch (Exception ex) {
            LOGGER.error("Exception:", ex);
        }
    }

    private void trySendMessages(Runnable messageProcessor) {
        FencedLock fencedLock = hazelcast.getCPSubsystem().getLock(lockName);
        if (fencedLock.tryLock()) {
            try {
                LOGGER.trace("Sending messages from {}", outboxName);
                messageProcessor.run();
            } finally {
                fencedLock.unlock();
            }
        } else {
            LOGGER.trace("Lock failed on {}", lockName);
        }
    }

    public static TransactionalOutbox create(HazelcastInstance hazelcast, String lockName, String outboxName) {
        return new TransactionalOutbox(hazelcast, lockName, outboxName);
    }
}
