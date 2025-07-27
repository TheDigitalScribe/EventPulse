package com.thedigitalscribe.producer_service.service;

import com.thedigitalscribe.model.PurchaseEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

@Service
@Slf4j
public class ProducerService {

    private final KafkaTemplate<String, PurchaseEvent> kafkaTemplate;
    private final String topic;

    public ProducerService(KafkaTemplate<String, PurchaseEvent> kafkaTemplate, @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendEvent(PurchaseEvent event) {
        String key = event.getOrderId() != null ? event.getOrderId().trim() : UUID.randomUUID().toString();
        event.setOrderId(key);

        ProducerRecord<String, PurchaseEvent> producerRecord = new ProducerRecord<>(topic, key, event);
        producerRecord.headers()
                .add(new RecordHeader("eventType", "PurchaseEvent".getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("eventSource", "purchase-events".getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("correlationId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("eventTimestamp", Instant.now().toString().getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(producerRecord)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully sent event {} to {}-[partition {}] @offset {}",
                                key,
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset()
                        );
                    } else {
                        log.error("Failed to send event {}: {}", key, ex.getMessage(), ex);
                    }
                });
    }
}
