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
import org.springframework.context.ApplicationContext;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionTimedOutException;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class ProducerServiceImpl<T> implements ProducerService<T> {

    private final ApplicationContext context;

    private final ConfigValidator<T> configValidator;

    private final MessagePublisherUtil<T> messagePublisherUtil;

    private final ClaimsCheckService<T> claimsCheckService;

    @Autowired
    private MeterRegistry registry;

    private String correlationId;

    private static boolean isRecordTooLargeEncountered = false;

    @Autowired
    public ProducerServiceImpl(ApplicationContext context, ConfigValidator<T> configValidator,
            MessagePublisherUtil<T> messagePublisherUtil, ClaimsCheckService<T> claimsCheckService) {
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
    @Retryable(value = { TransactionTimedOutException.class,
            TimeoutException.class }, maxAttemptsExpression = "${spring.retry.maximum.attempts}", backoff = @Backoff(delayExpression = "${spring.retry.backoff.delay}", multiplierExpression = "${spring.retry.backoff.multiplier}", maxDelayExpression = "${spring.retry.backoff.maxdelay}"))
    public void produceMessages(T message, Map<String, Object> kafkaHeader)
            throws TopicNameValidationException, KafkaServerNotFoundException, PayloadValidationException,
            KafkaHeaderValidationException, DLTException, ClaimsCheckFailedException {
        long startedAt = System.currentTimeMillis();
        isRecordTooLargeEncountered = false;
        String producerTopic = null;
        try {
            producerTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.NOTIFICATION_TOPIC);
            configValidator.validateInputs(producerTopic, message);
            correlationId = configValidator.getCorrelationId(kafkaHeader);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            messagePublisherUtil.publishOnTopic(producerRecord, kafkaHeader);
            log.info("Successfully published Payload with Correlation-Id {} to Kafka topic: {} in {} milliseconds", correlationId, producerTopic,
                    (System.currentTimeMillis() - startedAt));
        } catch (Exception ex) {
            log.error("Unable to push Payload with Correlation-Id {} to kafka topic: {}", correlationId, producerTopic, ex);
            registry.counter(ConfigConstants.SINGLE_PRODUCER_TARGET_TOPIC_ERROR_TOTAL).increment();
            if (ex.getCause() instanceof RecordTooLargeException) {
                isRecordTooLargeEncountered = true;
                var claimsCheckTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.CLAIMS_CHECK);
                var claimsCheckDlt = context.getEnvironment().resolvePlaceholders(ConfigConstants.CLAIMS_CHECK_DLT);
                Map<String, String> topics = new HashMap<>();
                topics.put(ConfigConstants.CLAIMS_CHECK_TOPIC_KEY, claimsCheckTopic);
                topics.put(ConfigConstants.CLAIMS_CHECK_DLT_KEY, claimsCheckDlt);
                claimsCheckService.handleClaimsCheckAfterGettingMemoryIssue(kafkaHeader, topics, message);
            } else {
                log.error("Unable to push Payload with Correlation-Id {} to kafka topic: {}", correlationId, producerTopic, ex);
                throw ex;
            }
        }
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
        if (e instanceof ClaimsCheckFailedException || e instanceof DLTException || isRecordTooLargeEncountered)
            throw e;
        try {
            messagePublisherUtil.produceMessageToDlt(e, message, kafkaHeader);
        } catch (Exception ex) {
            log.error("Exception while pushing Payload with Correlation-Id {} to DLT ", correlationId, ex);
            throw ex;
        }
        log.info("Time taken to successfully execute publishMessageOnRetryOrDltTopic for Payload with Correlation-Id {}: {} milliseconds",
            correlationId, (System.currentTimeMillis() - startedAt));
        throw new DLTException(String.format("Successfully published Payload with Correlation-Id %s to DLT", correlationId));
    }

}
