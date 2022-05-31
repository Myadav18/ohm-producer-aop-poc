package net.apmoller.crb.ohm.microservices.producer.library.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.DeflaterOutputStream;

/**
 * Class is to Compress the Payload.
 */
@Slf4j
@Component
public class CompressionUtil {

    public static String compressAndReturnB64(String text) throws IOException {
        return new String(Base64.getEncoder().encode(compress(text)));
    }

    public static byte[] compress(String text) throws IOException {
        return compress(text.getBytes());
    }

    public static byte[] compress(byte[] bArray) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (DeflaterOutputStream dos = new DeflaterOutputStream(os)) {
            dos.write(bArray);
        }
        return os.toByteArray();
    }

}
