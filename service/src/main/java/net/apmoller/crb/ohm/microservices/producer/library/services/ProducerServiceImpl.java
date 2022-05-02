package net.apmoller.crb.ohm.microservices.producer.library.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
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
@RequiredArgsConstructor
public class ProducerServiceImpl<T> implements ProducerService<T> {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private Validator validator;

    @Autowired
    private Environment env;

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    private static final String BOOTSTRAP_SERVER = "${kafka.bootstrapserver}";
    private static final String NOTIFICATION_TOPIC = "${kafka.notification.topic}";

    @Override
    public void sendMessage(T message, Map<String, Object> kafkaHeader)
            throws InvalidTopicException, InternalServerException {
        try {
            log.info("get topic name");
            var producerTopic = context.getEnvironment().resolvePlaceholders(NOTIFICATION_TOPIC);
            var bootstrapServer = context.getEnvironment().resolvePlaceholders(BOOTSTRAP_SERVER);
            log.info("topic name in producerTopic: {}", producerTopic);
            validator.checkValidation(producerTopic, bootstrapServer);
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
            log.error("Topic name or placeholder is not correct", ex);
            throw ex;
        } catch (Exception ex) {
            log.error("Exception: ", ex);
            throw ex;
        }
    }

    private void addHeaders(Headers headers, Map<String, Object> kafkaHeader) {
        if (Objects.nonNull(kafkaHeader)) {
            kafkaHeader.forEach((k, v) -> {
                headers.add(k, v.toString().getBytes(StandardCharsets.UTF_8));
            });
        }
    }

    public void publishOnTopic(ProducerRecord<String, T> producerRecord) throws InternalServerException {
        ListenableFuture<SendResult<String, T>> future = kafkaTemplate.send(producerRecord);
        log.info("Going to Publish the Message");
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
        log.info("completed the Process");
    }

}
