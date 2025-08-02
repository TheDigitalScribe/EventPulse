package com.thedigitalscribe.order_service.service;

import com.thedigitalscribe.model.OrderEvent;
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
public class OrderService {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final String topic;

    private static final String EVENT_TYPE = "OrderEvent";
    private static final String EVENT_SOURCE = "order-events";

    public OrderService(KafkaTemplate<String, OrderEvent> kafkaTemplate, @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendEvent(OrderEvent event) {
        String key;

        if (event.getOrderId() != null && !event.getOrderId().isBlank()) {
            key = event.getOrderId().trim();
        }
        else {
            key = UUID.randomUUID().toString();
        }

        event.setOrderId(key);

        if (event.getTimestamp() == null) {
            event.setTimestamp(Instant.now());
        }

        String correlationId = UUID.randomUUID().toString();

        ProducerRecord<String, OrderEvent> producerRecord = new ProducerRecord<>(topic, key, event);
        producerRecord.headers()
                .add(new RecordHeader("eventType", EVENT_TYPE.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("eventSource", EVENT_SOURCE.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("eventTimestamp", event.getTimestamp().toString().getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(producerRecord)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully sent event {} to {}-[partition {}] @offset {} (correlationId={})",
                                key,
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                correlationId
                        );
                    } else {
                        log.error("Failed to send event {} (correlationId={}): {}", key, correlationId, ex.getMessage(), ex);                    }
                });
    }
}
