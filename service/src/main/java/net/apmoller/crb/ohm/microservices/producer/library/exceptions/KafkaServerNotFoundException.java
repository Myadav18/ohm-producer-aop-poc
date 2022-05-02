package net.apmoller.crb.ohm.microservices.producer.library.exceptions;

public class KafkaServerNotFoundException extends RuntimeException {

    public KafkaServerNotFoundException(String message) {
        super(message);
    }
}
