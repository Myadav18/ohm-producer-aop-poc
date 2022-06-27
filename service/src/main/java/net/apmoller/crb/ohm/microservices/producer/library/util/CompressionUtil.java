package net.apmoller.crb.ohm.microservices.producer.library.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Class is to Compress the Payload.
 */
@Slf4j
@Component
public class CompressionUtil {

    /**
     * Private Constructor
     */
    private CompressionUtil() {
    }

    /**
     * Method will Encode the Compress Message
     *
     * @param text
     *
     * @return
     *
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
     *
     * @param text
     *
     * @return
     *
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
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Method will Compress and Return the Byte Array.
     *
     * @param bArray
     *
     * @return
     *
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

    public static byte[] gzipCompress(byte[] uncompressedData) {
        byte[] result = new byte[] {};
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(uncompressedData.length);
                GZIPOutputStream gzipOS = new GZIPOutputStream(bos)) {
            gzipOS.write(uncompressedData);
            // You need to close it before using bos
            gzipOS.close();
            result = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static byte[] gzipUncompress(byte[] compressedData) {
        byte[] result = new byte[] {};
        try (ByteArrayInputStream bis = new ByteArrayInputStream(compressedData);
             ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPInputStream gzipIS = new GZIPInputStream(bis)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipIS.read(buffer)) != -1) {
                bos.write(buffer, 0, len);
            }
            result = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

}
