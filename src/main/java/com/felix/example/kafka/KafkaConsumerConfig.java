package com.felix.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huangzq
 * @date 2019-06-25
 * @description
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${error.kafka.consumer.servers}")
    private String errorServers;

    @Value("${error.kafka.consumer.group-id}")
    private String errorGroup;

    @Value("${info.kafka.consumer.servers}")
    private String infoServers;

    @Value("${info.kafka.consumer.group-id}")
    private String infoGroup;

    @Value("${spring.kafka.consumer.concurrency}")
    private Integer concurrency;
    @Value("${spring.kafka.consumer.fetch-max-wait}")
    private String fetchMaxWait;
    @Value("${spring.kafka.consumer.session-timeout}")
    private String sessionTimeout;
    @Value("${spring.kafka.consumer.request-timeout}")
    private String requestTimeout;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private Boolean enableAutoCommit;
    @Value("${spring.kafka.consumer.heartbeat-interval}")
    private String heatBeatInterval;
    @Value("${spring.kafka.consumer.auto-commit-interval}")
    private String autoCommitInterval;
    @Value("${spring.kafka.consumer.max-poll-records}")
    private Integer maxPollRecordsConfig;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> errorListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(errorFactory());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setPollTimeout(5000);
        return factory;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> infoListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(infoFactory());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setPollTimeout(5000);
        return factory;
    }

    private Map<String, Object> getCommonProperties() {
        Map<String, Object> properties = new HashMap<>(16);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heatBeatInterval);
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecordsConfig);
        return properties;
    }

    private ConsumerFactory<String, String> errorFactory() {
        Map<String, Object> properties = getCommonProperties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, errorServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, errorGroup);
        return new DefaultKafkaConsumerFactory<>(properties);
    }

    private ConsumerFactory<String, String> infoFactory() {
        Map<String, Object> properties = getCommonProperties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, infoServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, infoGroup);
        return new DefaultKafkaConsumerFactory<>(properties);
    }

}
