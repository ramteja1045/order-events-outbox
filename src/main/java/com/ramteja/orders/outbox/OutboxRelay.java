package com.ramteja.orders.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.TimeUnit;

/**
 * Polls the outbox and publishes events to Kafka. Delivery is at-least-once: if the app crashes
 * after sending but before commit, the event is sent again, so consumers must be idempotent.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka,
                       @Value("${outbox.batch-size:100}") int batchSize) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:500}")
    @Transactional
    public void publishPending() {
        for (OutboxEvent event : outbox.claimBatch(batchSize)) {
            try {
                // Key by aggregate id so all events for one order land on the same partition, in order.
                kafka.send(event.getTopic(), event.getAggregateId(), event.getPayload()).get(5, TimeUnit.SECONDS);
                event.markPublished();
            } catch (Exception e) {
                event.recordFailedAttempt();
                log.warn("Outbox publish failed for event {} (attempt {}): {}", event.getId(), event.getAttempts(), e.getMessage());
                // Stop the batch so ordering per aggregate is preserved; we retry on the next poll.
                break;
            }
        }
    }
}
