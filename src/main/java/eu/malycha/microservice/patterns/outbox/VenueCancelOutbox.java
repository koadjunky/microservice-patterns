package eu.malycha.microservice.patterns.outbox;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import io.wyden.published.venue.VenueCancel;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ConcurrentModificationException;
import java.util.concurrent.CompletableFuture;

public class VenueCancelOutbox {

    public static final String VENUE_CANCEL_OUTBOX_LOCK = "venue-cancel-outbox-lock";
    public static final String VENUE_CANCEL_OUTBOX = "venue-cancel-outbox";

    private final HazelcastInstance hazelcast;
    private final TransactionalOutbox transactionalOutbox;
    private final ConnectorCommandEmitter emitter;

    public VenueCancelOutbox(HazelcastInstance hazelcast, ConnectorCommandEmitter emitter) {
        this.hazelcast = hazelcast;
        this.emitter = emitter;
        this.transactionalOutbox = TransactionalOutbox.create(hazelcast, VENUE_CANCEL_OUTBOX_LOCK, VENUE_CANCEL_OUTBOX);
    }

    @Scheduled(fixedDelay = 100)
    public void process() {
        transactionalOutbox.process(() -> {
            IList<VenueCancel> messages = hazelcast.getList(VENUE_CANCEL_OUTBOX);
            while (!messages.isEmpty()) {
                VenueCancel venueCancel = messages.get(0);
                CompletableFuture<Void> future = emitter.emit(venueCancel);
                future.join();
                if (!messages.remove(venueCancel)) {
                    throw new ConcurrentModificationException("Concurrent processing of " + VENUE_CANCEL_OUTBOX + " detected.");
                }
            }
        });
    }
}
