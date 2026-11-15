package eu.malycha.microservice.patterns.outbox;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import io.wyden.published.oems.OemsResponse;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ConcurrentModificationException;
import java.util.concurrent.CompletableFuture;

public class OemsResponseOutbox {

    public static final String OEMS_RESPONSE_OUTBOX_LOCK = "oems-response-outbox-lock";
    public static final String OEMS_RESPONSE_OUTBOX = "oems-response-outbox";

    private final HazelcastInstance hazelcast;
    private final TransactionalOutbox transactionalOutbox;
    private final OemsResponseEmitter emitter;

    public OemsResponseOutbox(HazelcastInstance hazelcast, OemsResponseEmitter emitter) {
        this.hazelcast = hazelcast;
        this.emitter = emitter;
        this.transactionalOutbox = TransactionalOutbox.create(hazelcast, OEMS_RESPONSE_OUTBOX_LOCK, OEMS_RESPONSE_OUTBOX);
    }

    @Scheduled(fixedDelay = 100)
    public void process() {
        transactionalOutbox.process(() -> {
            IList<OemsResponse> messages = hazelcast.getList(OEMS_RESPONSE_OUTBOX);
            while (!messages.isEmpty()) {
                OemsResponse oemsResponse = messages.get(0);
                CompletableFuture<Void> future = emitter.emit(oemsResponse);
                future.join();
                if (!messages.remove(oemsResponse)) {
                    throw new ConcurrentModificationException("Concurrent processing of " + OEMS_RESPONSE_OUTBOX + " detected.");
                }
            }
        });
    }
}
