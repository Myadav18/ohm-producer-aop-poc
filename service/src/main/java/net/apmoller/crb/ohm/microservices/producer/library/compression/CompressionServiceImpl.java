package net.apmoller.crb.ohm.microservices.producer.library.compression;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.util.CompressionUtil;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
@Service
public class CompressionServiceImpl<T> implements CompressionService<T> {
    /**
     * Method will compress and encode the payload.
     * 
     * @param message
     * 
     * @return
     */
    @Override
    public T compressMessage(T message) throws IOException {
        T compressedPayload = null;
        try {
            if (Objects.nonNull(message)) {
                log.info("Original payload size: {} bytes", message.toString().getBytes(StandardCharsets.UTF_8).length);
                if (message instanceof String) {
                    compressedPayload = (T) CompressionUtil.compressAndReturnB64(message.toString());
                } else {
                    CustomAvroSerializer avroSerializer = new CustomAvroSerializer();
                    compressedPayload = (T) avroSerializer.serialize(null, null, message);
                }
                log.info("Compressed payload size: {} bytes",
                        compressedPayload.toString().getBytes(StandardCharsets.UTF_8).length);
            }
        } catch (Exception e) {
            log.error("unable to Compress and Encode the Payload : ", e);
            throw e;
        }
        return compressedPayload;
    }
}