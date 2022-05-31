package net.apmoller.crb.ohm.microservices.producer.library.compression;

import java.io.IOException;

public interface CompressionService<T> {

    T compressMessage(T message) throws IOException;
}
