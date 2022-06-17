package net.apmoller.crb.ohm.microservices.producer.library.utils;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.util.CompressionUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@SpringBootTest(classes = { CompressionUtil.class })
@ActiveProfiles({ "test" })
public class CompressionUtilTest {

    @Autowired
    private CompressionUtil compressionUtil;

    @Test
    void testCompressionAndEncoding() {
        String payload = null;
        assertThrows(Exception.class, () -> compressionUtil.compressAndReturnB64(payload));
    }

    @Test
    void testEmptyPayload() throws IOException {
        String payload = "";
        var compressData = compressionUtil.compressAndReturnB64(payload);
    }

    @Test
    void testNullPayload() {
        String payload = null;
        assertThrows(Exception.class, () -> compressionUtil.compress(payload));
    }

    @Test
    void testEmptyPayloadCompression() throws IOException {
        String payload = "";
        var compressedData = compressionUtil.compress(payload);
    }

    @Test
    void testCompressionAndEncodingSuccess() throws IOException {
        String payload = "test";
        compressionUtil.compressAndReturnB64(payload);
    }

    @Test
    void testbyteArrayNull() throws IOException {
        byte[] payload = null;
        assertThrows(Exception.class, () -> compressionUtil.compress(payload));
    }

}
