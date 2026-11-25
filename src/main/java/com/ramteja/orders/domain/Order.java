package com.ramteja.orders.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    public enum Status { PLACED, RESERVED, REJECTED }

    @Id
    private UUID id;

    @Column(nullable = false)
    private String customerId;

    @Column(nullable = false)
    private String sku;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal total;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Status status;

    @Column(nullable = false)
    private Instant createdAt;

    protected Order() { }

    public Order(String customerId, String sku, int quantity, BigDecimal total) {
        this.id = UUID.randomUUID();
        this.customerId = customerId;
        this.sku = sku;
        this.quantity = quantity;
        this.total = total;
        this.status = Status.PLACED;
        this.createdAt = Instant.now();
    }

    public void markReserved() { this.status = Status.RESERVED; }
    public void markRejected() { this.status = Status.REJECTED; }

    public UUID getId() { return id; }
    public String getCustomerId() { return customerId; }
    public String getSku() { return sku; }
    public int getQuantity() { return quantity; }
    public BigDecimal getTotal() { return total; }
    public Status getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
}
