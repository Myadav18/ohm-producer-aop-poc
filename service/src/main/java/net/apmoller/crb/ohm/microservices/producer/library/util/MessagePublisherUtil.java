package net.apmoller.crb.ohm.microservices.producer.library.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaHeaderValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.PayloadValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.services.ConfigValidator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
public class MessagePublisherUtil<T> {

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    @Autowired
    private ConfigValidator<T> configValidator;

    @Autowired
    private ApplicationContext context;

    /**
     * Method sends message to kafka and returns the Success or Failure case.
     *
     * @param producerRecord - producer record to be sent on topic
     * @param kafkaHeader - Kafka headers map from input
     */
    public void publishOnTopic(ProducerRecord<String, T> producerRecord, Map<String, Object> kafkaHeader) {
        try {
            addHeaders(producerRecord.headers(), kafkaHeader);
            ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(producerRecord);
            future.addCallback(new ListenableFutureCallback<>() {
                @Override
                public void onSuccess(SendResult<String, T> result) {
                    log.info("Sent message to kafka topic:[{}] with offset=[{}]", producerRecord.topic(),
                            result.getRecordMetadata().offset());
                }

                @SneakyThrows
                @Override
                public void onFailure(Throwable ex) {
                    log.error("Unable to send message to kafka topic:[{}] due to : {}", producerRecord.topic(), ex);
                    throw ex;
                }
            });
        } catch (Exception ex) {
            log.error("Exception occurred while pushing message ", ex);
            throw ex;
        }
    }

    /**
     * Method to publish on Retry or Dead letter topic
     *
     * @param e  - Runtime exception from main method
     * @param message - payload
     * @param kafkaHeader - Kafka headers map
     * 
     * @throws TopicNameValidationException - for missing topic name
     * @throws PayloadValidationException - for null payload
     * @throws KafkaServerNotFoundException - for missing kafka bootstrap server
     * @throws KafkaHeaderValidationException - for missing kafka headers
     */
    public void produceToRetryOrDlt(RuntimeException e, T message, Map<String, Object> kafkaHeader)
            throws KafkaServerNotFoundException, TopicNameValidationException, PayloadValidationException,
            KafkaHeaderValidationException {
        if (configValidator.isInputValidationException(e)) {
            log.info("Throwing validation exception: {}", e.getClass().getName());
            throw e;
        }
        try {
            var retryTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.RETRY_TOPIC);
            var dltTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.DLT);
            if (configValidator.sendToRetryTopic(retryTopic, e)) {
                publishToRetryTopic(message, kafkaHeader, retryTopic, dltTopic, e);
            } else {
                publishToDltTopic(message, kafkaHeader, dltTopic, e);
            }
        } catch (Exception ex) {
            log.error("Exception Occurred while Posting to Retry or DLT Topic", ex);
            throw ex;
        }
    }

    /**
     * Method return retry topic/DLT name.
     *
     * @param e - runtime exception from the main method
     * @param topics - map containing topic names from input
     * @param message - payload
     * @param kafkaHeader - Kafka headers map from input
     * @throws TopicNameValidationException - for missing topic name
     * @throws PayloadValidationException - for null payload
     * @throws KafkaServerNotFoundException - for missing kafka bootstrap server
     * @throws KafkaHeaderValidationException - for missing kafka headers
     */
    public void produceMessageToRetryOrDlt(RuntimeException e, Map<String, String> topics, T message,
            Map<String, Object> kafkaHeader) throws KafkaServerNotFoundException, TopicNameValidationException,
            PayloadValidationException, KafkaHeaderValidationException {
        if (configValidator.isInputValidationException(e)) {
            log.info("Throwing validation exception: {}", e.getClass().getName());
            throw e;
        }
        try {
            if (configValidator.sendToRetryTopic(topics, e)) {
                publishMessageToRetryTopic(message, kafkaHeader, e, topics);
            } else if (configValidator.dltTopicPresent(topics)) {
                var dltTopic = topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY);
                publishMessageToDltTopic(message, kafkaHeader, dltTopic);
            } else
                throw e;
        } catch (Exception ex) {
            log.error("Exception Occurred in produceMessageToRetryOrDlt :", ex);
            throw ex;
        }
    }

    /**
     * Method adds headers to the producerRecord.
     * 
     * @param headers - Producer record header
     * @param kafkaHeader - Kafka headers map from input
     */
    private void addHeaders(Headers headers, Map<String, Object> kafkaHeader) {
        if (CollectionUtils.isEmpty(kafkaHeader))
            throw new KafkaHeaderValidationException(ConfigConstants.INVALID_KAFKA_HEADER_MAP_ERROR_MSG);
        kafkaHeader.forEach((k, v) -> {
            if (Objects.isNull(v))
                throw new KafkaHeaderValidationException(String.format(ConfigConstants.INVALID_KAFKA_HEADER_VALUE_ERROR_MSG, k));
            headers.add(k, v.toString().getBytes(StandardCharsets.UTF_8));
        });
    }

    /**
     * Method will send message to Retry Topic
     *
     * @param message - payload
     * @param kafkaHeader - Kafka headers map
     * @param retryTopic - retry topic name
     */
    public void publishToRetryTopic(T message, Map<String, Object> kafkaHeader, String retryTopic, String dltTopic,
            RuntimeException e) {
        try {
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
            publishOnTopic(producerRecord, kafkaHeader);
            log.info("Successfully published message to kafka retry topic: {}", retryTopic);
        } catch (Exception ex) {
            log.error("Exception while posting message to retry topic: {}", retryTopic, ex);
            publishToDltTopic(message, kafkaHeader, dltTopic, e);
        }
    }

    /**
     * Method will send the message to retry topic
     * 
     * @param message - payload
     * @param kafkaHeader - kafka headers map
     * @param topics - Topics map
     * @param e - Runtime exception from main method
     */
    public void publishMessageToRetryTopic(T message, Map<String, Object> kafkaHeader, RuntimeException e,
            Map<String, String> topics) {
        var retryTopic = topics.get(ConfigConstants.RETRY_TOPIC_KEY);
        try {
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
            publishOnTopic(producerRecord, kafkaHeader);
            log.info("Successfully published message to kafka retry topic: {}", retryTopic);
        } catch (Exception ex) {
            log.error("Exception while posting to retry topic: {}", retryTopic, ex);
            if (configValidator.dltTopicPresent(topics)) {
                var dltTopic = topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY);
                publishMessageToDltTopic(message, kafkaHeader, dltTopic);
            } else {
                log.info("Dead letter topic not present in input map");
                throw e;
            }
        }
    }

    /**
     * Method will send the message to Dlt topic
     * 
     * @param message - payload
     * @param kafkaHeader - kafka headers map
     * @param dltTopic - Dead letter topic name
     * @param e - Runtime exception from main method
     */
    public void publishToDltTopic(T message, Map<String, Object> kafkaHeader, String dltTopic, RuntimeException e) {
        try {
            if (configValidator.dltTopicIsPresent(dltTopic)) {
                ProducerRecord<String, T> producerRecord = new ProducerRecord<>(dltTopic, message);
                publishOnTopic(producerRecord, kafkaHeader);
                log.info("Publish message to kafka Dead letter topic: {}", dltTopic);
            } else {
                log.info("DLT not added in config");
                throw e;
            }
        } catch (Exception ex) {
            log.error("Exception while posting to DLT: {}", dltTopic, ex);
            throw ex;
        }
    }

    /**
     * Method to post message on DLT
     * 
     * @param message - payload
     * @param kafkaHeader - kafka headers map
     * @param dltTopic - Dead letter topic name
     */
    public void publishMessageToDltTopic(T message, Map<String, Object> kafkaHeader, String dltTopic) {
        try {
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(dltTopic, message);
            publishOnTopic(producerRecord, kafkaHeader);
            log.info("Successfully published message to kafka Dead letter topic: {}", dltTopic);
        } catch (Exception ex) {
            log.error("Exception while posting to DLT: {} ", dltTopic, ex);
            throw ex;
        }
    }

}
