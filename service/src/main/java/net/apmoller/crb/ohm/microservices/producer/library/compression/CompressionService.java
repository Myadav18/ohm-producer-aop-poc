package net.apmoller.crb.ohm.microservices.producer.library.compression;

public interface CompressionService<T> {

    T compressMessage(T message);
}
