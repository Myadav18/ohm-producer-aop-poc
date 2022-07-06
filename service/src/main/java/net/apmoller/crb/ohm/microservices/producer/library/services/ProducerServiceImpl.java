package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.aop.annotations.LogException;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.*;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
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

    public ProducerServiceImpl(ApplicationContext context, ConfigValidator<T> configValidator,
            MessagePublisherUtil<T> messagePublisherUtil) {
        this.context = context;
        this.configValidator = configValidator;
        this.messagePublisherUtil = messagePublisherUtil;
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
    @Retryable(value = { TransactionTimedOutException.class,
            TimeoutException.class }, maxAttemptsExpression = "${spring.retry.maximum.attempts}", backoff = @Backoff(delayExpression = "${spring.retry.backoff.delay}", multiplierExpression = "${spring.retry.backoff.multiplier}", maxDelayExpression = "${spring.retry.backoff.maxdelay}"))
    public void produceMessages(T message, Map<String, Object> kafkaHeader) throws TopicNameValidationException,
            KafkaServerNotFoundException, PayloadValidationException, KafkaHeaderValidationException, DLTException {
        long startedAt = System.currentTimeMillis();
        String producerTopic = null;
        try {
            producerTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.NOTIFICATION_TOPIC);
            configValidator.validateInputs(producerTopic, message);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
        } catch (Exception ex) {
            log.error("Unable to push message to kafka topic: {}", producerTopic, ex);
            throw ex;
        }
        log.info("Successfully published to Kafka topic: {} in {} milliseconds", producerTopic, (System.currentTimeMillis() - startedAt));
    }

    /**
     * Method Sends the Message to DLT Topic.
     */
    @LogException
    @Recover
    public void publishMessageOnDltTopic(RuntimeException e, T message, Map<String, Object> kafkaHeader)
            throws TopicNameValidationException, KafkaServerNotFoundException, PayloadValidationException,
            KafkaHeaderValidationException, DLTException {
        long startedAt = System.currentTimeMillis();
        try {
            messagePublisherUtil.produceMessageToDlt(e, message, kafkaHeader);
        } catch (Exception ex) {
            log.error("Exception while pushing message to DLT ", ex);
            throw ex;
        }
        log.info("Time taken to successfully execute publishMessageOnRetryOrDltTopic: {} milliseconds", (System.currentTimeMillis() - startedAt));
        throw new DLTException("Successfully published message to DLT");
    }

}
