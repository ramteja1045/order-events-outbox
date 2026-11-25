package com.ramteja.orders.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ramteja.orders.domain.OrderPlacedEvent;
import com.ramteja.orders.domain.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reserves inventory for new orders.
 * <ul>
 *   <li>Idempotent: the event id is recorded in {@code processed_events} in the same transaction
 *       as the state change, so a redelivered event is skipped.</li>
 *   <li>Non-blocking retries: failures move to retry topics with exponential backoff, then to a
 *       dead-letter topic, without stalling the main partition.</li>
 * </ul>
 */
@Component
public class InventoryReservationConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservationConsumer.class);
    static final String CONSUMER_NAME = "inventory-reservation";

    private final ObjectMapper mapper;
    private final OrderRepository orders;
    private final ProcessedEventRepository processed;
    private final InventoryClient inventory;

    public InventoryReservationConsumer(ObjectMapper mapper, OrderRepository orders,
                                        ProcessedEventRepository processed, InventoryClient inventory) {
        this.mapper = mapper;
        this.orders = orders;
        this.processed = processed;
        this.inventory = inventory;
    }

    @RetryableTopic(attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = {com.fasterxml.jackson.core.JsonProcessingException.class})
    @KafkaListener(topics = "orders.placed", groupId = CONSUMER_NAME)
    @Transactional
    public void onOrderPlaced(String payload) throws Exception {
        OrderPlacedEvent event = mapper.readValue(payload, OrderPlacedEvent.class);

        if (processed.existsById(event.eventId())) {
            log.info("Skipping duplicate event {}", event.eventId());
            return;
        }

        var order = orders.findById(event.orderId())
                .orElseThrow(() -> new IllegalStateException("Order " + event.orderId() + " not visible yet"));

        if (inventory.reserve(event.sku(), event.quantity())) {
            order.markReserved();
        } else {
            order.markRejected();
        }
        processed.save(new ProcessedEvent(event.eventId(), CONSUMER_NAME));
    }

    @DltHandler
    public void onDeadLetter(String payload, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        // In production this would alert and land in a table for manual replay.
        log.error("Event moved to dead-letter topic {}: {}", topic, payload);
    }
}
