package com.minishop.order.api;

import com.minishop.order.domain.Order;
import com.minishop.order.domain.OrderRepository;
import com.minishop.order.saga.OrderSagaOrchestrator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderSagaOrchestrator orchestrator;
    private final OrderRepository orders;

    public OrderController(OrderSagaOrchestrator orchestrator, OrderRepository orders) {
        this.orchestrator = orchestrator;
        this.orders = orders;
    }

    /**
     * Returns 202 Accepted, not 201/200: the order is only PENDING. The saga finishes
     * asynchronously and the client polls GET /orders/{id} (or gets notified) for the outcome.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody CreateOrderRequest request) {
        request.validate();
        UUID orderId = orchestrator.startSaga(request);
        return ResponseEntity.accepted()
                .location(URI.create("/orders/" + orderId))
                .body(Map.of("orderId", orderId, "status", "PENDING"));
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> get(@PathVariable UUID id) {
        return ResponseEntity.of(orders.findById(id));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
