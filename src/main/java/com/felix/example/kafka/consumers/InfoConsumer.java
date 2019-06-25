package com.felix.example.kafka.consumers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author huangzq01
 * @date 2019-06-25
 * @description
 */
@Component
public class InfoConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(InfoConsumer.class);

    @KafkaListener(topics = "${infoTopic}", containerFactory = "infoListenerContainerFactory")
    public void listen(String records) {
        if (records == null) {
            LOGGER.error("records is null");
            return;
        }

        LOGGER.info("Message:{}", records);
    }


}
