package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.PayloadValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionTimedOutException;

import java.io.IOException;
import java.util.Map;

@Slf4j
@Service
public class KafkaProducerServiceImpl<T> implements KafkaProducerService<T> {

    @Autowired
    private ConfigValidator<T> configValidator;

    @Autowired
    private MessagePublisherUtil<T> messagePublisherUtil;

    /**
     * Method is used to Send Message to kafka topic after validations.
     * @param topics Map containing target, retry and dead letter topic names
     * @param message payload
     * @param kafkaHeader Map containing headers to be posted on topic
     * @throws TopicNameValidationException
     * @throws InternalServerException
     * @throws KafkaServerNotFoundException
     */
    @Override
    @Retryable(value = { TransactionTimedOutException.class,
            TimeoutException.class }, maxAttemptsExpression = "${spring.retry.maximum.attempts}", backoff = @Backoff(delayExpression = "${spring.retry.backoff.delay}", multiplierExpression = "${spring.retry.backoff.multiplier}", maxDelayExpression = "${spring.retry.backoff.maxdelay}"))
    public void produceMessages(Map<String, String> topics, T message, Map<String, Object> kafkaHeader)
            throws TopicNameValidationException, KafkaServerNotFoundException, InternalServerException,
            PayloadValidationException, IOException {
        long startedAt = System.currentTimeMillis();
        try {
            configValidator.validateInputsForMultipleProducerFlow(topics, message);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(
                    topics.get(ConfigConstants.NOTIFICATION_TOPIC_KEY), message);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
        } catch (Exception ex) {
            log.error("Exception occurred while posting message to kafka topic ", ex);
            throw ex;
        }
        log.info("Time taken to execute produceMessages: {} milliseconds", (System.currentTimeMillis() - startedAt));
    }

    /**
     * Method Sends the Message to Retry Or DLT Topic.
     * @param e
     * @param message
     * @param kafkaHeader
     * @throws TopicNameValidationException
     * @throws InternalServerException
     * @throws KafkaServerNotFoundException
     */
    @Recover
    public void publishMessageOnRetryOrDltTopic(RuntimeException e, Map<String, String> topics, T message,
            Map<String, Object> kafkaHeader) throws InternalServerException, TopicNameValidationException,
            KafkaServerNotFoundException, PayloadValidationException, IOException {
        long startedAt = System.currentTimeMillis();
        try {
            messagePublisherUtil.produceMessageToRetryOrDlt(e, topics, message, kafkaHeader);
        } catch (Exception ex) {
            log.error("Exception while pushing message to error topic ", ex);
            throw ex;
        }
        log.info("Time taken to execute publishMessageOnRetryOrDltTopic: {} milliseconds", (System.currentTimeMillis() - startedAt));
    }

}
