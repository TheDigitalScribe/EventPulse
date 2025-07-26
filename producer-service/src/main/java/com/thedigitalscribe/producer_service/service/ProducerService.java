package com.thedigitalscribe.producer_service.service;

import com.thedigitalscribe.model.PurchaseEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public ProducerService(KafkaTemplate<String, Object> kafkaTemplate, @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendEvent(PurchaseEvent event) {
        String rawKey = event.getOrderId();
        String key = (rawKey != null && !rawKey.isBlank()) ? rawKey : UUID.randomUUID().toString();

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, key, event);
        producerRecord.headers().add(new RecordHeader("eventType", "PurchaseEvent".getBytes()));
        producerRecord.headers().add(new RecordHeader("source", "producer-service".getBytes()));

        kafkaTemplate.send(producerRecord)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully sent event={} to topic={}, partition={}",
                                key,
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition());

                        log.info("event_producer: success, key={}, topic={}, partition={}, product={}, amount={}",
                                key, result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                event.getProduct(), event.getPrice() * event.getQuantity());
                    } else {
                        log.error("Failed to send event={} due to {}", key, ex.getMessage(), ex);
                    }
                });
    }
}
