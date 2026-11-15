package eu.malycha.microservice.patterns.outbox;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalList;
import com.hazelcast.transaction.TransactionalMap;
import io.wyden.oems.ordercollider.model.VenueOrderState;
import io.wyden.published.oems.OemsResponse;
import io.wyden.published.venue.VenueCancel;
import io.wyden.published.venue.VenueOrder;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.wyden.oems.ordercollider.collider.service.connector.command.VenueCancelOutbox.VENUE_CANCEL_OUTBOX;
import static io.wyden.oems.ordercollider.collider.service.connector.command.VenueOrderOutbox.VENUE_ORDER_OUTBOX;
import static io.wyden.oems.ordercollider.collider.service.oems.response.OemsResponseOutbox.OEMS_RESPONSE_OUTBOX;
import static io.wyden.oems.ordercollider.collider.service.tracking.OrderStateLookup.CONNECTOR_ORDER_STATE_MAP;

public class TransactionalOutboxService {

    private final HazelcastInstance hazelcast;
    private final long ttlSeconds;

    public TransactionalOutboxService(HazelcastInstance hazelcast, @Value("${hz.completedOrderTtlSeconds:60}") long completedOrderTtlSeconds) {
        this.hazelcast = hazelcast;
        this.ttlSeconds = completedOrderTtlSeconds;
    }

    public void update(CacheUpdateRecord data) {
        TransactionOptions options = new TransactionOptions()
            .setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
        TransactionContext context = hazelcast.newTransactionContext(options);
        context.beginTransaction();
        try {
            VenueOrderState venueOrderState = data.venueOrderState;
            if (venueOrderState != null) {
                TransactionalMap<String, VenueOrderState> stateMap = context.getMap(CONNECTOR_ORDER_STATE_MAP);
                stateMap.put(venueOrderState.getOrder().getVenueOrderId(), venueOrderState, ttlSeconds, TimeUnit.SECONDS);
            }

            TransactionalList<VenueOrder> orderList = context.getList(VENUE_ORDER_OUTBOX);
            data.venueOrders.forEach(orderList::add);

            TransactionalList<VenueCancel> cancelList = context.getList(VENUE_CANCEL_OUTBOX);
            data.venueCancels.forEach(cancelList::add);

            TransactionalList<OemsResponse> responseList = context.getList(OEMS_RESPONSE_OUTBOX);
            data.oemsResponses.forEach(responseList::add);

            context.commitTransaction();
        } catch (Exception ex) {
            context.rollbackTransaction();
            throw ex;
        }
    }

    public record CacheUpdateRecord(
        VenueOrderState venueOrderState,
        List<OemsResponse> oemsResponses,
        List<VenueOrder> venueOrders,
        List<VenueCancel> venueCancels
    ) {}

}
