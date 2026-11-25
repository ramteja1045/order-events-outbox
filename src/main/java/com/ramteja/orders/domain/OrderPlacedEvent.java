package com.ramteja.orders.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Event contract published to the {@code orders.placed} topic. */
public record OrderPlacedEvent(UUID eventId, UUID orderId, String customerId, String sku,
                               int quantity, BigDecimal total, Instant occurredAt) { }
