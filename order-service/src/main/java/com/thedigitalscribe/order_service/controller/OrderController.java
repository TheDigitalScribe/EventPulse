package com.thedigitalscribe.order_service.controller;

import com.thedigitalscribe.model.OrderEvent;
import com.thedigitalscribe.order_service.dto.CreateOrderRequest;
import com.thedigitalscribe.order_service.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        OrderEvent event = new OrderEvent();
        event.setProduct(request.getProduct());
        event.setQuantity(request.getQuantity());
        event.setPrice(request.getPrice());
        event.setTimestamp(request.getTimestamp() != null ? request.getTimestamp() : Instant.now());

        orderService.sendEvent(event);
        return ResponseEntity.ok("Event sent to Kafka!");
    }
}
