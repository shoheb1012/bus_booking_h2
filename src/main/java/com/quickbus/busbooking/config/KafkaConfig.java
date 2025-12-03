package com.quickbus.busbooking.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;


@Configuration
public class KafkaConfig {

    public static final String EMAIL_TOPIC = "quickbus-email-events";
    public static final String EMAIL_DLQ_TOPIC = "quickbus-email-events-dlq"; // Dead Letter Queue

    @Bean
    public NewTopic emailTopic() {
        return TopicBuilder.name(EMAIL_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic emailDlqTopic() {
        return TopicBuilder.name(EMAIL_DLQ_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
}