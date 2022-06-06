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

    /**
     * Method will Encode the Compress Message
     * @param text
     * @return
     * @throws IOException
     */
    public static String compressAndReturnB64(String text) throws IOException {
        try {
            return new String(Base64.getEncoder().encode(compress(text)));
        } catch (Exception e) {
            log.error("Exception Occured during Compression");
            throw e;
        }
    }

    /**
     * Method will call compress Method to compress Data.
     * @param text
     * @return
     * @throws IOException
     */
    public static byte[] compress(String text) throws IOException {
        try {
            if (!text.isEmpty())
                return compress(text.getBytes());
        } catch (Exception e) {
            log.error("Payload can't be null or Empty");
            throw e;
        }
        return null;
    }

    /**
     * Method will Compress and Return the Byte Array.
     * @param bArray
     * @return
     * @throws IOException
     */
    public static byte[] compress(byte[] bArray) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (DeflaterOutputStream dos = new DeflaterOutputStream(os)) {
            dos.write(bArray);
        } catch (Exception e) {
            log.error("Exception Occured while Compressing and Encoding String Payload");
            throw e;
        }
        return os.toByteArray();
    }

}
