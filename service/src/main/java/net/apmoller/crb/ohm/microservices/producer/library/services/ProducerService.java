package net.apmoller.crb.ohm.microservices.producer.library.services;

import net.apmoller.crb.ohm.microservices.producer.library.exceptions.InternalServerException;
import net.apmoller.crb.ohm.microservices.producer.library.exceptions.KafkaServerNotFoundException;
import org.apache.kafka.common.errors.InvalidTopicException;

import java.util.Map;

public interface ProducerService<T> {

    void produceMessages(T message, Map<String, Object> kafkaHeader)
            throws InvalidTopicException, InternalServerException, KafkaServerNotFoundException;
}
