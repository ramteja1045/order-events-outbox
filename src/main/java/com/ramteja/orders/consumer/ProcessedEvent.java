package com.ramteja.orders.consumer;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

/** Dedup table: one row per event a consumer has fully handled. */
@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    private UUID eventId;

    @Column(nullable = false)
    private String consumer;

    @Column(nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() { }

    public ProcessedEvent(UUID eventId, String consumer) {
        this.eventId = eventId;
        this.consumer = consumer;
        this.processedAt = Instant.now();
    }

    public UUID getEventId() { return eventId; }
}
