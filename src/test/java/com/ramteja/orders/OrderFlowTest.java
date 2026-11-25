package com.ramteja.orders;

import com.ramteja.orders.api.OrderService;
import com.ramteja.orders.domain.Order;
import com.ramteja.orders.domain.OrderRepository;
import com.ramteja.orders.outbox.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class OrderFlowTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired OrderService service;
    @Autowired OrderRepository orders;
    @Autowired OutboxRepository outbox;

    @Test
    void order_event_flows_through_outbox_to_consumer() {
        Order order = service.placeOrder("cust-1", "SKU-BOOK", 2, new BigDecimal("39.98"));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo(Order.Status.RESERVED));
        assertThat(outbox.countByPublishedAtIsNull()).isZero();
    }

    @Test
    void out_of_stock_order_is_rejected() {
        Order order = service.placeOrder("cust-2", "SKU-LAPTOP", 50, new BigDecimal("50000.00"));

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(orders.findById(order.getId()).orElseThrow().getStatus()).isEqualTo(Order.Status.REJECTED));
    }
}
