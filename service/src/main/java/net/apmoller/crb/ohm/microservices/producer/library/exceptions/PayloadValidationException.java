package net.apmoller.crb.ohm.microservices.producer.library.exceptions;

public class PayloadValidationException extends RuntimeException {

    public PayloadValidationException(String message) {
        super(message);
    }
}
