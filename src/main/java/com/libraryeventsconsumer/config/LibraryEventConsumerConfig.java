package com.libraryeventsconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfig {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    /*public DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (r,e) -> {

            if(e instanceof RecoverableDataAccessException){
                return new TopicPartition(retryTopic, r.partition());
            }
            else{
                return new TopicPartition(deadLetterTopic, r.partition());
            }
        }
        );
        return recoverer;

    }*/

    public DefaultErrorHandler errorHandler(){

        var exceptionsToIgnoreList = List.of(IllegalArgumentException.class);
        var exceptionToRetryList = List.of(RecoverableDataAccessException.class);

        var fixedBackOff = new FixedBackOff(1000L, 2);

        var expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);



        var errorHandler =  new DefaultErrorHandler(
                //publishingRecoverer(),
                //fixedBackOff
                expBackOff
        );

        // Ignore retries for specified exceptions
        // exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        // Attempt retry to specific exceptions
        exceptionToRetryList.forEach(errorHandler::addRetryableExceptions);

        //Add retry Listener --> listen to each and every retry attempt
        errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed Record in Retry Listener, Exception : {}, deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt);
        }));

        return errorHandler;
    }


    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory<Object, Object>) kafkaConsumerFactory);
        factory.setConcurrency(3);
        // Implement Custom Retry
        factory.setCommonErrorHandler(errorHandler());
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }


}
