package net.apmoller.crb.ohm.microservices.producer.library.compression;

import lombok.extern.slf4j.Slf4j;
import net.apmoller.ohm.adapter.avro.model.EventNotificationsAdapterModel;
import net.minidev.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThrows;

@Slf4j
@SpringBootTest(classes = { CompressionServiceImpl.class })
@ActiveProfiles({ "test" })
public class CompressionServiceImplTest<T> {

    @Autowired
    private CompressionServiceImpl compressionServiceImpl;

    @Test
    void teststringCompression() throws IOException {
        String message = TestPayload.jsonPayload();
        Object compressedPayload = compressionServiceImpl.compressMessage(message);
        Assertions.assertTrue((message.length()) > (compressedPayload.toString().length()));

    }

    @Test
    void testAvroMessage() throws IOException {
        List<String> downstream = new ArrayList<>();
        downstream.add("documentservice");
        String testPayload = "{\n"
                + "    \"response\": \"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?> <db_response type=\\\"db_extract_package\\\" version=\\\"2\\\" revision=\\\"0\\\"> <response error=\\\"0\\\" returncode=\\\"0\\\" origin=\\\"DMS:SCRBDBKDK007206\\\"> <returnstring source=\\\"Docengine\\\">Added ply [1] by index</returnstring> </response> <container> <archive save=\\\"true\\\" doctype=\\\"\\\" docid=\\\"RNKT00003\\\" expirydate=\\\"2022-10-04\\\"><domain>WCAIND</domain><code>0A732E774E34615B25315973EA2C</code><index_s>2ab97fda-55dd-4ae3-a379-88f45e0a3b37</index_s><index_m>43e4be58-9c54-493f-ad40-6847bad741a5</index_m></archive></container></db_response>\"\n"
                + "}";
        EventNotificationsAdapterModel avro = EventNotificationsAdapterModel.newBuilder().setResponse(testPayload)
                .setCorrelationId("TESTCORRELATIONID1234567890").setMessageType("xml")
                .setMessageId("TESTMESSAGEID1234567890").setSourceSystem("docbroker").setResponseConsumers(downstream)
                .build();
        Object compressedPayload = compressionServiceImpl.compressMessage(avro);
        Assertions.assertTrue(testPayload.length() > compressedPayload.toString().length());
    }

    @Test
    void testNullMessage() throws IOException {
        String message = null;
        Object compressedPayload = compressionServiceImpl.compressMessage(message);
        Assertions.assertTrue((compressedPayload == null));
    }

    @Test
    void testAvroMessageException() throws IOException {
        List<String> downstream = new ArrayList<>();
        downstream.add("documentservice");
        String testPayload = "{\n"
                + "    \"response\": \"<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?> <db_response type=\\\"db_extract_package\\\" version=\\\"2\\\" revision=\\\"0\\\"> <response error=\\\"0\\\" returncode=\\\"0\\\" origin=\\\"DMS:SCRBDBKDK007206\\\"> <returnstring source=\\\"Docengine\\\">Added ply [1] by index</returnstring> </response> <container> <archive save=\\\"true\\\" doctype=\\\"\\\" docid=\\\"RNKT00003\\\" expirydate=\\\"2022-10-04\\\"><domain>WCAIND</domain><code>0A732E774E34615B25315973EA2C</code><index_s>2ab97fda-55dd-4ae3-a379-88f45e0a3b37</index_s><index_m>43e4be58-9c54-493f-ad40-6847bad741a5</index_m></archive></container></db_response>\"\n"
                + "}";
        JSONObject response = new JSONObject();
        assertThrows(Exception.class, () -> compressionServiceImpl.compressMessage(response));
    }

}