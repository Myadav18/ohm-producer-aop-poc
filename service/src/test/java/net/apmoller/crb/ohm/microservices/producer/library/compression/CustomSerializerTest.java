package net.apmoller.crb.ohm.microservices.producer.library.compression;

import net.apmoller.crb.ohm.microservices.producer.library.util.CompressionUtil;
import net.apmoller.ohm.adapter.avro.model.EventNotificationsAdapterModel;
import net.minidev.json.JSONObject;
import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { CustomSerializer.class })
@ActiveProfiles({ "test" })
public class CustomSerializerTest {

    @Autowired
    private CustomSerializer customSerializer;

    @Before
    public void setup() {
        customSerializer = new CustomSerializer();
    }

    @Test
    public void teststringCompression() {
        String message = TestPayload.jsonPayload();
        byte[] compressedPayload = customSerializer.serialize(null, null, message);
        Assertions.assertTrue(
                (message.length()) > (compressedPayload.toString().getBytes(StandardCharsets.UTF_8).length));
    }

    @Test
    public void testAvroMessage() {
        List<String> downstream = new ArrayList<>();
        String topic = "test";
        downstream.add("documentservice");
        String testPayload = "{\n"
                + "    \"response\": \"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?> <db_response type=\\\"db_extract_package\\\" version=\\\"2\\\" revision=\\\"0\\\"> <response error=\\\"0\\\" returncode=\\\"0\\\" origin=\\\"DMS:SCRBDBKDK007206\\\"> <returnstring source=\\\"Docengine\\\">Added ply [1] by index</returnstring> </response> <container> <archive save=\\\"true\\\" doctype=\\\"\\\" docid=\\\"RNKT00003\\\" expirydate=\\\"2022-10-04\\\"><domain>WCAIND</domain><code>0A732E774E34615B25315973EA2C</code><index_s>2ab97fda-55dd-4ae3-a379-88f45e0a3b37</index_s><index_m>43e4be58-9c54-493f-ad40-6847bad741a5</index_m></archive></container></db_response>\"\n"
                + "}";
        EventNotificationsAdapterModel avro = EventNotificationsAdapterModel.newBuilder().setResponse(testPayload)
                .setCorrelationId("TESTCORRELATIONID1234567890").setMessageType("xml")
                .setMessageId("TESTMESSAGEID1234567890").setSourceSystem("docbroker").setResponseConsumers(downstream)
                .build();
        byte[] compressedPayload = customSerializer.serialize(topic, null, avro);
        Assertions.assertTrue(
                testPayload.length() > compressedPayload.toString().getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void testNullMessage() {
        String message = null;
        byte[] compressedPayload = customSerializer.serialize(null, null, message);
        Assertions.assertEquals(null, compressedPayload);
    }

    @Test
    public void testGzipMessage() throws IOException {
        String message = null;
        byte[] compressedPayload = CompressionUtil.gzipCompress("test".getBytes(StandardCharsets.UTF_8));
        Assertions.assertNotNull(compressedPayload);
    }
    @Test
    public void testUnGzipMessage() throws IOException {
        String message = null;
        byte[] compressedPayload = CompressionUtil.gzipUncompress("test".getBytes(StandardCharsets.UTF_8));
        Assertions.assertNotNull(compressedPayload);
    }

    @Test
    public void testAvroMessageException() {
        List<String> downstream = new ArrayList<>();
        downstream.add("documentservice");
        String testPayload = "{\n"
                + "    \"response\": \"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?> <db_response type=\\\"db_extract_package\\\" version=\\\"2\\\" revision=\\\"0\\\"> <response error=\\\"0\\\" returncode=\\\"0\\\" origin=\\\"DMS:SCRBDBKDK007206\\\"> <returnstring source=\\\"Docengine\\\">Added ply [1] by index</returnstring> </response> <container> <archive save=\\\"true\\\" doctype=\\\"\\\" docid=\\\"RNKT00003\\\" expirydate=\\\"2022-10-04\\\"><domain>WCAIND</domain><code>0A732E774E34615B25315973EA2C</code><index_s>2ab97fda-55dd-4ae3-a379-88f45e0a3b37</index_s><index_m>43e4be58-9c54-493f-ad40-6847bad741a5</index_m></archive></container></db_response>\"\n"
                + "}";
        JSONObject message = new JSONObject();
        assertThrows(Exception.class, () -> customSerializer.serialize(null, null, message));
    }
}
