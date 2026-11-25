package com.ramteja.orders.api;

import com.ramteja.orders.domain.Order;
import com.ramteja.orders.domain.OrderRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    public record PlaceOrderRequest(@NotBlank String customerId,
                                    @NotBlank String sku,
                                    @Min(1) @Max(1000) int quantity,
                                    @NotNull @Positive BigDecimal total) { }

    private final OrderService service;
    private final OrderRepository orders;

    public OrderController(OrderService service, OrderRepository orders) {
        this.service = service;
        this.orders = orders;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Order place(@Valid @RequestBody PlaceOrderRequest req) {
        return service.placeOrder(req.customerId(), req.sku(), req.quantity(), req.total());
    }

    @GetMapping("/{id}")
    public Order get(@PathVariable UUID id) {
        return orders.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order " + id + " not found"));
    }
}
