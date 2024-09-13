package com.learnkafka.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsConsumerConfig {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
                                                                                       ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());  // exception handling technique in consumers

        return factory;
    }

    private DefaultErrorHandler errorHandler() {

//        var fixedBackOff = new FixedBackOff(1000L, 2); // 3 attempts in total. Time interval doesn't change over time
//        var errorHandler = new DefaultErrorHandler(fixedBackOff);

        var expBackOff = new ExponentialBackOffWithMaxRetries(2); // time interval increases from 1 second to 2 over time
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);

        var errorHandler = new DefaultErrorHandler(publishingRecoverer(), expBackOff);

        var exceptionIgnoreList = List.of( // specific unwanted exceptions not to be considered
                IllegalArgumentException.class
        );

        exceptionIgnoreList.forEach(errorHandler::addNotRetryableExceptions);

        errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> { // monitor each failed record via logs
            log.info("Failed Record in Retry Listener, Exception: {}, deliveryAttempt: {}", ex, deliveryAttempt);
        }));

        return errorHandler;
    }

    private DeadLetterPublishingRecoverer publishingRecoverer() { // publishing the message to the retry and dead letter topics

        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    log.error("Exception in publishingRecoverer : {} ", e.getMessage(), e);
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                }
        );
    }
}