package com.thedigitalscribe.validation_service.service;

import com.thedigitalscribe.model.PurchaseEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
@Slf4j
public class ValidationService {

    private final KafkaTemplate<String, PurchaseEvent> kafkaTemplate;
    private final String validTopic;
    private final String deadLetterTopic;

    public ValidationService(KafkaTemplate<String, PurchaseEvent> kafkaTemplate, @Value("${app.kafka.topic.valid}") String validTopic, @Value("${app.kafka.topic.dlt}") String deadLetterTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.validTopic = validTopic;
        this.deadLetterTopic = deadLetterTopic;
    }

    @KafkaListener(
            topics = "${app.kafka.topic.input}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void validate(PurchaseEvent event) {
        String key = event.getOrderId() != null ? event.getOrderId() : "unknown";

        if (isValid(event)) {
            log.info("✅ Valid event: {}", event);
            kafkaTemplate.send(validTopic, key, event);
        } else {
            log.warn("Invalid event, sending to DLT: {}", event);
            ProducerRecord<String, PurchaseEvent> dltRecord = new ProducerRecord<>(deadLetterTopic, key, event);
            dltRecord.headers().add(new RecordHeader("validation-error", "invalid-payload".getBytes(StandardCharsets.UTF_8)));
            kafkaTemplate.send(dltRecord);
        }
    }

    private boolean isValid(PurchaseEvent event) {
        return event != null &&
                isNotBlank(event.getOrderId()) &&
                isNotBlank(event.getProduct()) &&
                isNotBlank(event.getTimestamp()) &&
                event.getQuantity() > 0 &&
                event.getPrice() > 0;
    }

    private boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }
}
