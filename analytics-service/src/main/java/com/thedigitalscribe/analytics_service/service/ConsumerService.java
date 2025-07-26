package com.thedigitalscribe.analytics_service.service;

import com.thedigitalscribe.model.PurchaseEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerService {

    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void listen(PurchaseEvent purchaseEvent) {
        log.info("Received event: {}", purchaseEvent);
    }
}
