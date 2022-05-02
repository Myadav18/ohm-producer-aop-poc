package net.apmoller.crb.ohm.microservices.producer.library.services;

import org.apache.kafka.common.errors.InvalidTopicException;

import java.io.IOException;
import java.util.Map;

public interface ProducerService<T> {

    void sendMessage(T message, Map<String, Object> kafkaHeader) throws InvalidTopicException, InterruptedException;
}
