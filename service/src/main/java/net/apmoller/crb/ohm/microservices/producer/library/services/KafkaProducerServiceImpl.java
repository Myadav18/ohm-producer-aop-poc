package net.apmoller.crb.ohm.microservices.producer.library.services;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.aop.annotations.LogException;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.*;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionTimedOutException;

import java.util.Map;

@Slf4j
@Service
public class KafkaProducerServiceImpl<T> implements KafkaProducerService<T> {

    private final ConfigValidator<T> configValidator;

    private final MessagePublisherUtil<T> messagePublisherUtil;

    private final ClaimsCheckService<T> claimsCheckService;

    private String correlationId;

    @Autowired
    private MeterRegistry registry;

    @Autowired
    public KafkaProducerServiceImpl(ConfigValidator<T> configValidator, MessagePublisherUtil<T> messagePublisherUtil,
            ClaimsCheckService<T> claimsCheckService) {
        this.configValidator = configValidator;
        this.messagePublisherUtil = messagePublisherUtil;
        this.claimsCheckService = claimsCheckService;
    }

    /**
     * Method is used to Send Message to kafka topic after validations.
     * 
     * @param topics - Map containing target, retry and dead letter topic names
     * @param message - payload
     * @param kafkaHeader - Map containing headers to be posted on topic
     * @throws TopicNameValidationException - for missing topic name
     * @throws PayloadValidationException - for null payload
     * @throws KafkaServerNotFoundException - for missing kafka bootstrap server
     * @throws KafkaHeaderValidationException - for missing kafka headers
     */
    @Override
    @LogException
    @Retryable(value = { TransactionTimedOutException.class,
            TimeoutException.class }, maxAttemptsExpression = "${spring.retry.maximum.attempts}", backoff = @Backoff(delayExpression = "${spring.retry.backoff.delay}", multiplierExpression = "${spring.retry.backoff.multiplier}", maxDelayExpression = "${spring.retry.backoff.maxdelay}"))
    public void produceMessages(Map<String, String> topics, T message, Map<String, Object> kafkaHeader)
            throws TopicNameValidationException, KafkaServerNotFoundException, PayloadValidationException,
            KafkaHeaderValidationException, DLTException, ClaimsCheckFailedException {
        long startedAt = System.currentTimeMillis();
        String producerTopic = null;
        try {
            configValidator.validateInputsForMultipleProducerFlow(topics, message);
            correlationId = configValidator.getCorrelationId(kafkaHeader);
            producerTopic = topics.get(ConfigConstants.NOTIFICATION_TOPIC_KEY);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            log.info("Successfully published Payload with Correlation-Id {} to Kafka topic: {} in {} milliseconds", correlationId, producerTopic,
                    (System.currentTimeMillis() - startedAt));
        } catch (Exception ex) {
            registry.counter(ConfigConstants.MULTIPLE_PRODUCER_TARGET_TOPIC_ERROR_TOTAL).increment();
            if (ex.getCause() instanceof RecordTooLargeException) {
                claimsCheckService.handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topics, message);
            } else {
                log.error("Exception occurred while posting Payload with Correlation-Id {} to target kafka topic: {} ", correlationId, producerTopic, ex);
                throw ex;
            }
        }
    }

    /**
     * Method Sends the Message to DLT Topic.
     * 
     * @throws DLTException - Exception thrown when message successfully published on Dead letter topic
     */
    @LogException
    @Recover
    public void publishMessageOnDltTopic(RuntimeException e, Map<String, String> topics, T message,
            Map<String, Object> kafkaHeader) throws TopicNameValidationException, KafkaServerNotFoundException,
            PayloadValidationException, DLTException, ClaimsCheckFailedException {
        long startedAt = System.currentTimeMillis();
        if (e instanceof ClaimsCheckFailedException || e instanceof DLTException)
            throw e;
        try {
            messagePublisherUtil.produceMessageToDlt(e, topics, message, kafkaHeader);
        } catch (Exception ex) {
            log.error("Exception while pushing Payload with Correlation-Id {} to DLT ", correlationId, ex);
            throw ex;
        }
        log.info("Time taken to successfully execute publishMessageOnDltTopic for Payload with Correlation-Id {}: {} milliseconds",
            correlationId, (System.currentTimeMillis() - startedAt));
        throw new DLTException(String.format("Successfully published Payload with Correlation-Id %s to DLT", correlationId));
    }

}
