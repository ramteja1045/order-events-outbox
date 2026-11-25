package com.ramteja.orders.consumer;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stand-in for a downstream inventory service. Keeps stock in memory so the demo runs without
 * extra infrastructure; swap for an HTTP client in a real deployment.
 */
@Component
public class InventoryClient {

    private final Map<String, Integer> stock = new ConcurrentHashMap<>(Map.of(
            "SKU-BOOK", 100,
            "SKU-LAPTOP", 5));

    public boolean reserve(String sku, int quantity) {
        if (sku.startsWith("SKU-FAIL")) {
            throw new IllegalStateException("Inventory service unavailable");
        }
        int[] reserved = {0};
        stock.computeIfPresent(sku, (k, available) -> {
            if (available >= quantity) {
                reserved[0] = 1;
                return available - quantity;
            }
            return available;
        });
        return reserved[0] == 1;
    }
}
