package net.apmoller.crb.ohm.microservices.producer.library.exceptions;

public class TopicNameValidationException extends RuntimeException {
    public TopicNameValidationException(String message) {
        super(message);
    }
}
