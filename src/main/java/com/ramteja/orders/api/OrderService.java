package com.ramteja.orders.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ramteja.orders.domain.Order;
import com.ramteja.orders.domain.OrderPlacedEvent;
import com.ramteja.orders.domain.OrderRepository;
import com.ramteja.orders.outbox.OutboxEvent;
import com.ramteja.orders.outbox.OutboxRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {

    public static final String ORDERS_PLACED_TOPIC = "orders.placed";

    private final OrderRepository orders;
    private final OutboxRepository outbox;
    private final ObjectMapper mapper;

    public OrderService(OrderRepository orders, OutboxRepository outbox, ObjectMapper mapper) {
        this.orders = orders;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    /**
     * Saves the order and its event in ONE database transaction. There is no dual write:
     * either both rows commit or neither does, and the relay publishes the event afterwards.
     */
    @Transactional
    public Order placeOrder(String customerId, String sku, int quantity, BigDecimal total) {
        Order order = orders.save(new Order(customerId, sku, quantity, total));
        UUID eventId = UUID.randomUUID();
        var event = new OrderPlacedEvent(eventId, order.getId(), customerId, sku, quantity, total, Instant.now());
        outbox.save(new OutboxEvent(eventId, order.getId().toString(), ORDERS_PLACED_TOPIC, toJson(event)));
        return order;
    }

    private String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize event", e);
        }
    }
}
