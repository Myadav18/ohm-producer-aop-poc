package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.exceptions.*;

import java.util.Map;

public interface ProducerService<T> {

    void produceMessages(T message, Map<String, Object> kafkaHeader) throws TopicNameValidationException,
            KafkaServerNotFoundException, PayloadValidationException, KafkaHeaderValidationException, DLTException;
}
