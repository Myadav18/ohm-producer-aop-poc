package net.apmoller.crb.ohm.microservices.producer.library.compression;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.util.CompressionUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.header.Headers;
import org.apache.poi.ss.formula.functions.T;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

@Slf4j
@Component
public class CustomSerializer extends KafkaAvroSerializer {

    /**
     * Method Compress and Encode the Payload
     * 
     * @param topic
     * @param headers
     * @param data
     * 
     * @return
     */
    @SneakyThrows
    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        byte[] compressedPayload = null;
        if (Objects.nonNull(data)) {
            long startedAt = System.currentTimeMillis();
            log.info("Original payload size: {} bytes", data.toString().getBytes(StandardCharsets.UTF_8).length);
            if (data instanceof String) {
                log.info("Json/String Compression");
                compressedPayload = CompressionUtil.compress(data.toString());

            } else {
                Schema schema = ReflectData.get().getSchema(data.getClass());
                if (Objects.nonNull(schema))
                    log.info("Avro Payload schema: {}", schema.getName());
                DatumWriter<GenericRecord> writer = new ReflectDatumWriter<>(schema);
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer)
                                .setCodec(CodecFactory.bzip2Codec()).create(schema, outputStream)) {

                    dataFileWriter.append((GenericRecord) data);
                    compressedPayload = outputStream.toByteArray();
                } catch (Exception e) {
                    log.info("Exception Occured while Compressing and Encoding");
                    throw e;
                }
            }
            log.info("Compressed payload size: {} bytes",
                    compressedPayload.toString().getBytes(StandardCharsets.UTF_8).length);
            long finishedAt = System.currentTimeMillis();
            log.info("Finished method X at time: " + finishedAt + " after: " + (finishedAt - startedAt)
                    + " milliseconds");
        }
        return compressedPayload;
    }
}
