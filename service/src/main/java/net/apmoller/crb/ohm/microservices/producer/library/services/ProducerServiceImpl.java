package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
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
    public void produceMessages(T message, Map<String, Object> kafkaHeader)
            throws InvalidTopicException, InternalServerException, KafkaServerNotFoundException {
        try {
            var producerTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.NOTIFICATION_TOPIC);
            configValidator.validateInputs(producerTopic);
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(producerTopic, message);
            addHeaders(producerRecord.headers(), kafkaHeader);
            publishOnTopic(producerRecord);
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

}
