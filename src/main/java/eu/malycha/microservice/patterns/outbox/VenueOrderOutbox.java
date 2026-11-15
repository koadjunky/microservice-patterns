package eu.malycha.microservice.patterns.outbox;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import io.wyden.published.venue.VenueOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.CompletableFuture;

public class VenueOrderOutbox {

    private static final Logger LOGGER = LoggerFactory.getLogger(VenueOrderOutbox.class);

    public static final String VENUE_ORDER_OUTBOX_LOCK = "venue-order-outbox-lock";
    public static final String VENUE_ORDER_OUTBOX = "venue-order-outbox";

    private final HazelcastInstance hazelcast;
    private final TransactionalOutbox transactionalOutbox;
    private final ConnectorCommandEmitter emitter;

    public VenueOrderOutbox(HazelcastInstance hazelcast, ConnectorCommandEmitter emitter) {
        this.hazelcast = hazelcast;
        this.emitter = emitter;
        this.transactionalOutbox = TransactionalOutbox.create(hazelcast, VENUE_ORDER_OUTBOX_LOCK, VENUE_ORDER_OUTBOX);
    }

    @Scheduled(fixedDelay = 100)
    public void process() {
        transactionalOutbox.process(() -> {
            IList<VenueOrder> messages = hazelcast.getList(VENUE_ORDER_OUTBOX);
            while (!messages.isEmpty()) {
                processSingleOrder(messages.remove(0));
            }
        });
    }

    private void processSingleOrder(VenueOrder venueOrder) {
        try {
            // We are playing safe here - better to not send message at all than send it multiple times
            CompletableFuture<Void> future = emitter.emit(venueOrder);
            future.join();
        } catch (Exception ex) {
            // Rabbit is probably down/malfunctioning - should not be used
            // We could store order in cache, wait a bit for status change if the message was sent
            // and if no change, reject the order after timeout. For now let's just log it.
            LOGGER.error("Unable to send VenueOrder:%n%s".formatted(venueOrder));
        }
    }
}
