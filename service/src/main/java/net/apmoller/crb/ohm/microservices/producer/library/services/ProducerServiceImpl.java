package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.aop.annotations.LogException;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.*;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionTimedOutException;

import java.util.Map;

@Slf4j
@Service
public class ProducerServiceImpl<T> implements ProducerService<T> {

    @Autowired
    private final ApplicationContext context;

    @Autowired
    private final ConfigValidator<T> configValidator;


    @Autowired
    private final MessagePublisherUtil<T> messagePublisherUtil;

    @Autowired
    private final ClaimsCheckService<T> claimsCheckService;

    public ProducerServiceImpl(ApplicationContext context, ConfigValidator<T> configValidator, MessagePublisherUtil<T> messagePublisherUtil, ClaimsCheckService<T> claimsCheckService) {
        this.context = context;
        this.configValidator = configValidator;
        this.messagePublisherUtil = messagePublisherUtil;
        this.claimsCheckService = claimsCheckService;
    }

    /**
     * Method is used to Send Message to kafka topic after validations.
     * 
     * @param message - payload
     * @param kafkaHeader - headers map
     * @throws TopicNameValidationException - for missing topic name
     * @throws PayloadValidationException - for null payload
     * @throws KafkaServerNotFoundException - for missing kafka bootstrap server
     * @throws KafkaHeaderValidationException - for missing kafka headers
     */
    @Override
    @LogException
    @Retryable(value = { TransactionTimedOutException.class, TimeoutException.class }, maxAttemptsExpression = "${spring.retry.maximum.attempts}",
            backoff = @Backoff(delayExpression = "${spring.retry.backoff.delay}", multiplierExpression = "${spring.retry.backoff.multiplier}", maxDelayExpression = "${spring.retry.backoff.maxdelay}"))
    public void produceMessages(T message, Map<String, Object> kafkaHeader) throws TopicNameValidationException,
            KafkaServerNotFoundException, PayloadValidationException, KafkaHeaderValidationException {
        long startedAt = System.currentTimeMillis();
        String producerTopic = null;
        try {
            producerTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.NOTIFICATION_TOPIC);
            configValidator.validateInputs(producerTopic, message);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
        } catch (RecordTooLargeException ex) {
            String claimschecktopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.CLAIMS_CHECK);
            if (configValidator.claimsCheckTopicIsPresent(claimschecktopic)) {
                claimsCheckService.handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, claimschecktopic, message);
            } else {
                log.error("Exception occurred because claims check topic not found", ex);
                throw new ClaimsCheckFailedException("claims check topic not found in request",ex);
            }
        } catch (Exception ex) {
            log.error("Unable to push message to kafka topic: {}", producerTopic, ex);
            throw ex;
        }
         log.info("Successfully published to Kafka topic: {} in {} milliseconds", producerTopic, (System.currentTimeMillis() - startedAt));
    }

    /**
     * Method Sends the Message to Retry Or DLT Topic.
     *
     * @param e - Runtime exception from main method
     * @param message - payload
     * @param kafkaHeader - Kafka headers map
     * @throws TopicNameValidationException - for missing topic name
     * @throws PayloadValidationException - for null payload
     * @throws KafkaServerNotFoundException - for missing kafka bootstrap server
     * @throws KafkaHeaderValidationException - for missing kafka headers
     */
    @LogException
    @Recover
    public void publishMessageOnRetryOrDltTopic(RuntimeException e, T message, Map<String, Object> kafkaHeader)
            throws TopicNameValidationException, KafkaServerNotFoundException, PayloadValidationException, KafkaHeaderValidationException {
        long startedAt = System.currentTimeMillis();
        if (e instanceof ClaimsCheckFailedException){
            throw e;
        }
        try {
            messagePublisherUtil.produceToRetryOrDlt(e, message, kafkaHeader);
        } catch (Exception ex) {
            log.error("Exception occurred while posting to error topic ", ex);
            throw ex;
        }
        log.info("Time taken to successfully execute publishMessageOnRetryOrDltTopic: {} milliseconds", (System.currentTimeMillis() - startedAt));
    }

}
