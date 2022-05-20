package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionTimedOutException;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
public class ProducerServiceImpl<T> implements ProducerService<T> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ConfigValidator configValidator;

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    /**
     * Method is used to Send Message to kafka topic after validations.
     * @param message
     * @param kafkaHeader
     * @throws InvalidTopicException
     * @throws InternalServerException
     * @throws KafkaServerNotFoundException
     */
    @Override
    @Retryable(value = { TransactionTimedOutException.class,
            TimeoutException.class }, maxAttemptsExpression = "${spring.retry.maximum.attempts}", backoff = @Backoff(delayExpression = "${spring.retry.backoff.delay}", multiplierExpression = "${spring.retry.backoff.multiplier}", maxDelayExpression = "${spring.retry.backoff.maxdelay}"))
    public void produceMessages(T message, Map<String, Object> kafkaHeader)
            throws InvalidTopicException, InternalServerException, KafkaServerNotFoundException {
        long startedAt = System.currentTimeMillis();
        log.info("Started method X at time: " + startedAt);
        try {
            log.info("Inside produceMessages ");
            var producerTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.NOTIFICATION_TOPIC);
            configValidator.validateInputs(producerTopic);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            addHeaders(producerRecord.headers(), kafkaHeader);
            publishOnTopic(producerRecord);
            log.info("Published to Kafka topic");
        } catch (InternalServerException ex) {
            log.error("unable to push message to kafka ", ex);
            throw ex;
        } catch (KafkaServerNotFoundException ex) {
            log.error("Unable to Connect the Kafka Server ", ex);
            throw ex;
        } catch (InvalidTopicException ex) {
            log.error("Exception Occured while searching for Topic", ex);
            throw ex;
        } catch (Exception ex) {
            log.error("Exception: ", ex);
            throw ex;
        }
        long finishedAt = System.currentTimeMillis();
        log.info("Finished method X at time: " + finishedAt + " after: " + (finishedAt - startedAt) + " milliseconds");
    }

    /**
     * Method adds headers to the producerRecord.
     * @param headers
     * @param kafkaHeader
     */
    private void addHeaders(Headers headers, Map<String, Object> kafkaHeader) {
        if (Objects.nonNull(kafkaHeader)) {
            kafkaHeader.forEach((k, v) -> {
                headers.add(k, v.toString().getBytes(StandardCharsets.UTF_8));
            });
        }
    }

    /**
     * Method sends message to kafka and returns the Success or Failure case.
     * @param producerRecord
     * @throws InternalServerException
     */
    public void publishOnTopic(ProducerRecord<String, T> producerRecord) throws InternalServerException {
        ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(producerRecord);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, T> result) {
                log.info("Sent message=[{}] with offset=[{}]", producerRecord.value(),
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("Unable to send message=[{}] due to : {}", producerRecord.value(), ex);
                throw new InternalServerException("unable to push message to kafka", ex);
            }
        });
    }

    /**
     * Method Sends the Message to Retry Or DLT Topic.
     * 
     * @param e
     * @param message
     * @param kafkaHeader
     * 
     * @throws InvalidTopicException
     * @throws InternalServerException
     * @throws TransactionTimedOutException
     */
    @Recover
    public void publishMessageOnRetryOrDltTopic(RuntimeException e, T message, Map<String, Object> kafkaHeader)
            throws InvalidTopicException, InternalServerException {
        long startedAt = System.currentTimeMillis();
        log.info("Started method X at time: " + startedAt);
        try {
            log.info("Inside publishMessageOnRetryOrDltTopic ");
            if ((e instanceof TransactionTimedOutException) || (e instanceof TimeoutException)) {
                var retryTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.RETRY_TOPIC);
                configValidator.validateInputs(retryTopic);
                ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
                addHeaders(producerRecord.headers(), kafkaHeader);
                publishOnTopic(producerRecord);
                log.info("Publish message to kafka Retry topic");
            }

        } catch (InvalidTopicException ex) {
            log.error("Exception Occured while searching for Retry Topic", ex);
            throw ex;
        } catch (InternalServerException ex) {
            log.error("unable to push message to kafka ", ex);
            throw ex;
        } catch (Exception ex) {
            log.error("Exception: ", ex);
            throw ex;
        }
        long finishedAt = System.currentTimeMillis();
        log.info("Finished method X at time: " + finishedAt + " after: " + (finishedAt - startedAt) + " milliseconds");
    }

}