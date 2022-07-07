package net.apmoller.crb.ohm.microservices.producer.library.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.constants.ConfigConstants;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.*;
import net.apmoller.crb.ohm.microservices.producer.library.services.ConfigValidator;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
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
    private KafkaTemplate<String, T> kafkaTemplateAvro;

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplateJson;

    @Autowired
    private ConfigValidator<T> configValidator;

    @Autowired
    private ApplicationContext context;

    /**
     * Method sends message to kafka and returns the Success or Failure case.
     *
     * @param producerRecord - producer record to be sent on topic
     * @param kafkaHeader - Kafka headers map from input
     */
    public void publishOnTopic(ProducerRecord<String, T> producerRecord, Map<String, Object> kafkaHeader) {
        try {
            addHeaders(producerRecord.headers(), kafkaHeader);
            Schema schema = ReflectData.get().getSchema(producerRecord.value().getClass());
            ListenableFuture<SendResult<String, T>> future = getKafkaTemplate(schema).send(producerRecord);
            future.addCallback(new ListenableFutureCallback<>() {
                @Override
                public void onSuccess(SendResult<String, T> result) {
                    log.info("Sent message to kafka topic:[{}] on partition:[{}] with offset=[{}]",
                            producerRecord.topic(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
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
     * Method adds headers to the producerRecord.
     *
     * @param headers - Producer record header
     * @param kafkaHeader - Kafka headers map from input
     */
    private void addHeaders(Headers headers, Map<String, Object> kafkaHeader) {
        if (CollectionUtils.isEmpty(kafkaHeader))
            throw new KafkaHeaderValidationException(ConfigConstants.INVALID_KAFKA_HEADER_MAP_ERROR_MSG);
        kafkaHeader.forEach((k, v) -> {
            if (Objects.isNull(v))
                throw new KafkaHeaderValidationException(String.format(ConfigConstants.INVALID_KAFKA_HEADER_VALUE_ERROR_MSG, k));
            headers.add(k, v.toString().getBytes(StandardCharsets.UTF_8));
        });
    }

    /**
     * Method returns KafkaTemplate object based on payload schema.
     */
    public KafkaTemplate<String, T> getKafkaTemplate(Schema schema) {
        return schema.getName().equalsIgnoreCase("String") ? kafkaTemplateJson : kafkaTemplateAvro;
    }

    /**
     * Method to publish the Message on DLT Topic for single producer flow.
     */
    public void produceMessageToDlt(RuntimeException e, T message, Map<String, Object> kafkaHeader)
            throws KafkaServerNotFoundException, TopicNameValidationException, PayloadValidationException,
            KafkaHeaderValidationException {
        String dltTopic = null;
        if (configValidator.isInputValidationException(e)) {
            log.info("Throwing validation exception: {}", e.getClass().getName());
            throw e;
        }
        try {
            dltTopic = context.getEnvironment().resolvePlaceholders(ConfigConstants.DLT);
            if (configValidator.dltTopicIsPresent(dltTopic)) {
                ProducerRecord<String, T> producerRecord = new ProducerRecord<>(dltTopic, message);
                publishOnTopic(producerRecord, kafkaHeader);
                log.info("Published message to dead letter topic: {}", dltTopic);
            } else {
                log.info("DLT not added in config");
                throw e;
            }
        } catch (Exception ex) {
            log.error("Exception Occurred while posting to DLT: {}", dltTopic);
            throw ex;
        }
    }

    /**
     * Method to publish the Message on DLT Topic for multiple producer flow.
     */
    public void produceMessageToDlt(RuntimeException e, Map<String, String> topics, T message,
            Map<String, Object> kafkaHeader) throws KafkaServerNotFoundException, TopicNameValidationException,
            PayloadValidationException, KafkaHeaderValidationException {
        String dltTopic = null;
        if (configValidator.isInputValidationException(e)) {
            log.info("Throwing validation exception: {}", e.getClass().getName());
            throw e;
        }
        try {
            if (configValidator.dltTopicPresent(topics)) {
                dltTopic = topics.get(ConfigConstants.DEAD_LETTER_TOPIC_KEY);
                ProducerRecord<String, T> producerRecord = new ProducerRecord<>(dltTopic, message);
                publishOnTopic(producerRecord, kafkaHeader);
                log.info("Published message to dead letter topic: {}", dltTopic);
            } else {
                log.info("DLT not added in input topic map");
                throw e;
            }
        } catch (Exception ex) {
            log.error("Exception while posting to DLT: {} ", dltTopic, ex);
            throw ex;
        }
    }

}
