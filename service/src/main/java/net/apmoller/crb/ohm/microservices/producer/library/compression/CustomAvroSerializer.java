package net.apmoller.crb.ohm.microservices.producer.library.compression;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.util.Base64;

@Slf4j
@Component
public class CustomAvroSerializer extends KafkaAvroSerializer {

    /**
     * Method Compress and Encode the Avro Payload
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
        Schema schema = ReflectData.get().getSchema(data.getClass());
        log.info("Payload schema: {}", schema.getName());
        DatumWriter<GenericRecord> writer = new ReflectDatumWriter<>(schema);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer)
                        .setCodec(CodecFactory.deflateCodec(9)).create(schema, outputStream)) {

            dataFileWriter.append((GenericRecord) data);
            return Base64.getEncoder().encode(outputStream.toByteArray());
        } catch (Exception e) {
            log.info("Exception Occured while Compressing and Encoding");
            throw e;
        }
    }

}
