package com.thedigitalscribe.producer_service.service;

import com.thedigitalscribe.producer_service.model.PurchaseEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public ProducerService(KafkaTemplate<String, Object> kafkaTemplate, @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendEvent(PurchaseEvent event) {
        kafkaTemplate.send(topic, event.getOrderId(), event);
    }
}
