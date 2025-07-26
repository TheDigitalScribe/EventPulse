package com.thedigitalscribe.producer_service.controller;

import com.thedigitalscribe.producer_service.model.PurchaseEvent;
import com.thedigitalscribe.producer_service.service.ProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/events")
public class ProducerController {

    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<String> publishEvent(@RequestBody PurchaseEvent event) {
        producerService.sendEvent(event);
        return ResponseEntity.ok("Event sent to Kafka!");
    }
}
