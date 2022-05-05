package net.apmoller.crb.ohm.microservices.producer.library.exceptions;

public class InternalServerException extends java.lang.RuntimeException {
    public InternalServerException(java.lang.String e) {
    }

    public InternalServerException(String message, Throwable e) {
        super(message, e);
    }
}
