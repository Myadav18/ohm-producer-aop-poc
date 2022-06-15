package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.aop.annotations.LogException;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.PayloadValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.TopicNameValidationException;
import net.apmoller.crb.ohm.microservices.producer.library.util.MessagePublisherUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionTimedOutException;

import java.io.IOException;
import java.util.Map;

@Slf4j
@Service
public class ProducerServiceImpl<T> implements ProducerService<T> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ConfigValidator<T> configValidator;

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    @Autowired
    private MessagePublisherUtil<T> messagePublisherUtil;

    /**
     * Method is used to Send Message to kafka topic after validations.
     * 
     * @param message
     * @param kafkaHeader
     * 
     * @throws TopicNameValidationException
     * @throws InternalServerException
     * @throws KafkaServerNotFoundException
     */
    @Override
    @LogException
    @Retryable(value = { TransactionTimedOutException.class,
            TimeoutException.class }, maxAttemptsExpression = "${spring.retry.maximum.attempts}", backoff = @Backoff(delayExpression = "${spring.retry.backoff.delay}", multiplierExpression = "${spring.retry.backoff.multiplier}", maxDelayExpression = "${spring.retry.backoff.maxdelay}"))
    public void produceMessages(T message, Map<String, Object> kafkaHeader) throws TopicNameValidationException,
            InternalServerException, KafkaServerNotFoundException, IOException, PayloadValidationException {
        long startedAt = System.currentTimeMillis();
        try {
            var producerTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.NOTIFICATION_TOPIC);
            configValidator.validateInputs(producerTopic, message);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            log.info("Published to Kafka topic");
        } catch (Exception ex) {
            log.error("unable to push message to kafka : ", ex);
            throw ex;
        }
        long finishedAt = System.currentTimeMillis();
        log.info("Finished method X at time: " + finishedAt + " after: " + (finishedAt - startedAt) + " milliseconds");
    }

    /**
     * Method Sends the Message to Retry Or DLT Topic.
     *
     * @param e
     * @param message
     * @param kafkaHeader
     *
     * @throws TopicNameValidationException
     * @throws InternalServerException
     * @throws TransactionTimedOutException
     */
    @LogException
    @Recover
    public void publishMessageOnRetryOrDltTopic(RuntimeException e, T message, Map<String, Object> kafkaHeader)
            throws TopicNameValidationException, KafkaServerNotFoundException, InternalServerException, IOException,
            PayloadValidationException {
        long startedAt = System.currentTimeMillis();
        try {
            produceToRetryOrDlt(e, message, kafkaHeader);
        } catch (Exception ex) {
            log.error("Exception: ", ex);
            throw ex;
        }
        long finishedAt = System.currentTimeMillis();
        log.info("Finished method X at time: " + finishedAt + " after: " + (finishedAt - startedAt) + " milliseconds");
    }

    /**
     * Method will return the topic Topic Name.
     * 
     * @param e
     * 
     * @return
     * 
     * @throws KafkaServerNotFoundException
     * @throws TopicNameValidationException
     */
    public void produceToRetryOrDlt(RuntimeException e, T message, Map<String, Object> kafkaHeader)
            throws KafkaServerNotFoundException, TopicNameValidationException, PayloadValidationException {
        if ((e instanceof KafkaServerNotFoundException) || (e instanceof TopicNameValidationException)
                || (e instanceof PayloadValidationException)) {
            log.info("Throwing validation exception: {}", e.getClass().getName());
            throw e;
        }
        var retryTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.RETRY_TOPIC);
        var dltTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.DLT);
        try {
            if (configValidator.retryTopicIsPresent(retryTopic)
                    && ((e instanceof TransactionTimedOutException) || (e instanceof TimeoutException))) {
                messagePublisherUtil.publishToRetryTopic(message, kafkaHeader, retryTopic, dltTopic, e);
            } else {
                messagePublisherUtil.publishToDltTopic(message, kafkaHeader, dltTopic, e);
            }
        } catch (Exception ex) {
            throw ex;
        }
    }

}
