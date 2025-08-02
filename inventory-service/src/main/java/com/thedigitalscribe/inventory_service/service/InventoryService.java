package com.thedigitalscribe.inventory_service.service;

import com.thedigitalscribe.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class InventoryService {

    @KafkaListener(topics = "${app.kafka.topic}")
    public void handleOrderEvent(OrderEvent orderEvent) {
        log.info("Received event: {}", orderEvent);
    }
}
