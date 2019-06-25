package com.felix.example.kafka.consumers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author huangzq
 * @date 2019-06-25
 * @description
 */
@Component
public class ErrorConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorConsumer.class);

    @KafkaListener(topics = "${errorTopic}", containerFactory = "errorListenerContainerFactory")
    public void listen(String records) {
        if (records == null) {
            LOGGER.error("records is null");
            return;
        }

        LOGGER.info("Message:{}", records);
    }

}
