package com.thedigitalscribe.order_service.controller;

import com.thedigitalscribe.model.PurchaseEvent;
import com.thedigitalscribe.order_service.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody PurchaseEvent event) {
        orderService.sendEvent(event);
        return ResponseEntity.ok("Event sent to Kafka!");
    }
}
