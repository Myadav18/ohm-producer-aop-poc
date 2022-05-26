package net.apmoller.crb.ohm.microservices.producer.library.util;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
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

@Slf4j
@Component
public class MessagePublisherUtil<T> {

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    /**
     * Method sends message to kafka and returns the Success or Failure case.
     * @param producerRecord
     * @throws InternalServerException
     */
    public void publishOnTopic(ProducerRecord<String, T> producerRecord, Map<String, Object> kafkaHeader)
            throws InternalServerException {
        try {
            addHeaders(producerRecord.headers(), kafkaHeader);
            ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(producerRecord);
            future.addCallback(new ListenableFutureCallback<>() {
                @Override public void onSuccess(SendResult<String, T> result) {
                    log.info("Sent message=[{}] with offset=[{}]", producerRecord.value(), result.getRecordMetadata().offset());
                }

                @Override public void onFailure(Throwable ex) {
                    log.error("Unable to send message=[{}] due to : {}", producerRecord.value(), ex);
                    throw new InternalServerException("unable to push message to kafka", ex); }
            });
        } catch (Exception ex) {
            log.error("Exception occurred while pushing message ", ex);
            throw ex;
        }
    }

    /**
     * Method return retry topic/DLT name.
     * @param e runtime exception from the main method
     * @param topics map containing topic names from input
     * @throws KafkaServerNotFoundException
     * @throws InvalidTopicException
     */
    public String getErrorTopic(RuntimeException e, Map<String, String> topics)
            throws KafkaServerNotFoundException, InvalidTopicException {
        if ((e instanceof KafkaServerNotFoundException) || (e instanceof InvalidTopicException)) {
            log.info("Throwing validation exception: {}", e.getClass().getName());
            throw e;
        }
        return ((e instanceof TransactionTimedOutException) || (e instanceof TimeoutException))
                ? topics.get(ConfigConstants.RETRY_TOPIC_KEY) : topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY);
    }

    /**
     * Method adds headers to the producerRecord.
     * @param headers
     * @param kafkaHeader
     */
    private void addHeaders(Headers headers, Map<String, Object> kafkaHeader) {
        if (!CollectionUtils.isEmpty(kafkaHeader)) {
            kafkaHeader.forEach((k, v) -> headers.add(k, v.toString().getBytes(StandardCharsets.UTF_8)));
        }
    }

}
