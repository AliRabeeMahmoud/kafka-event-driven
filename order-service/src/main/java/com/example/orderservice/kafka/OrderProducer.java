package com.example.orderservice.kafka;

import com.example.basedomains.dto.OrderEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class OrderProducer {

    private NewTopic topic;

    private static final org.slf4j.Logger LOGGER=  LoggerFactory.getLogger(OrderProducer.class);
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(NewTopic topic, KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(OrderEvent event){

        LOGGER.info(String.format("order event => %s", event.toString()));

        Message<OrderEvent> message= MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, topic.name())
                .build();
        kafkaTemplate.send(message);

    }
}
