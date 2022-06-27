package net.apmoller.crb.ohm.microservices.producer.library.exceptions;

public class KafkaHeaderValidationException extends RuntimeException {

    public KafkaHeaderValidationException(String message) {
        super(message);
    }
}
