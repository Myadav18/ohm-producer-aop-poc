package net.apmoller.crb.ohm.microservices.producer.library.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.PayloadValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.services.ConfigValidator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionTimedOutException;
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

    /**
     * Method sends message to kafka and returns the Success or Failure case.
     * 
     * @param producerRecord
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
     * Method return retry topic/DLT name.
     * 
     * @param e
     *            runtime exception from the main method
     * @param topics
     *            map containing topic names from input
     * 
     * @throws KafkaServerNotFoundException
     * @throws TopicNameValidationException
     */
    public void produceMessageToRetryOrDlt(RuntimeException e, Map<String, String> topics, T message,
            Map<String, Object> kafkaHeader)
            throws KafkaServerNotFoundException, TopicNameValidationException, PayloadValidationException {
        if ((e instanceof KafkaServerNotFoundException) || (e instanceof TopicNameValidationException)
                || (e instanceof PayloadValidationException)) {
            log.info("Throwing validation exception: {}", e.getClass().getName());
            throw e;
        }
        try {
            if (configValidator.retryTopicPresent(topics) && ((e instanceof TransactionTimedOutException)
                    || (e instanceof TimeoutException)
                    || (Objects.nonNull(e.getCause()) && (e.getCause() instanceof TopicAuthorizationException)))) {
                publishMessageToRetryTopic(message, kafkaHeader, e, topics);
            } else if (configValidator.dltTopicPresent(topics)) {
                var dltTopic = topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY);
                publishMessageToDltTopic(message, kafkaHeader, dltTopic);
            } else
                throw e;
        } catch (Exception ex) {
            log.error("Exception Occured :", ex);
            throw ex;
        }
    }

    /**
     * Method adds headers to the producerRecord.
     * 
     * @param headers
     * @param kafkaHeader
     */
    private void addHeaders(Headers headers, Map<String, Object> kafkaHeader) {
        if (!CollectionUtils.isEmpty(kafkaHeader)) {
            kafkaHeader.forEach((k, v) -> headers.add(k, v.toString().getBytes(StandardCharsets.UTF_8)));
        }
    }

    /**
     * Method will send message to Retry Topic
     *
     * @param message
     * @param kafkaHeader
     * @param retryTopic
     */
    public void publishToRetryTopic(T message, Map<String, Object> kafkaHeader, String retryTopic, String dltTopic,
            RuntimeException e) {
        try {
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
            publishOnTopic(producerRecord, kafkaHeader);
            log.info("Publish message to kafka retry topic");
        } catch (Exception ex) {
            publishToDltTopic(message, kafkaHeader, dltTopic, e);
        }
    }

    /**
     * @param message
     * @param kafkaHeader
     * @param e
     * @param topics
     */
    public void publishMessageToRetryTopic(T message, Map<String, Object> kafkaHeader, RuntimeException e,
            Map<String, String> topics) {
        try {
            var retryTopic = topics.get(ConfigConstants.RETRY_TOPIC_KEY);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
            publishOnTopic(producerRecord, kafkaHeader);
            log.info("Publish message to kafka retry topic");
        } catch (Exception ex) {
            if (configValidator.dltTopicPresent(topics)) {
                var dltTopic = topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY);
                publishMessageToDltTopic(message, kafkaHeader, dltTopic);
            } else
                throw e;
        }
    }

    /**
     * Method will send the message to Dlt topic
     *
     * @param message
     * @param kafkaHeader
     * @param dltTopic
     */
    public void publishToDltTopic(T message, Map<String, Object> kafkaHeader, String dltTopic, RuntimeException e) {
        try {
            if (configValidator.dltTopicIsPresent(dltTopic)) {
                ProducerRecord<String, T> producerRecord = new ProducerRecord<>(dltTopic, message);
                publishOnTopic(producerRecord, kafkaHeader);
                log.info("Publish message to kafka Dead letter topic");
            } else
                throw e;
        } catch (Exception ex) {
            log.error("Exception: ", ex);
            throw ex;
        }
    }

    /**
     * @param message
     * @param kafkaHeader
     * @param dltTopic
     */
    public void publishMessageToDltTopic(T message, Map<String, Object> kafkaHeader, String dltTopic) {
        try {
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(dltTopic, message);
            publishOnTopic(producerRecord, kafkaHeader);
            log.info("Publish message to kafka Dead letter topic");
        } catch (Exception ex) {
            log.error("Exception: ", ex);
            throw ex;
        }
    }

}
