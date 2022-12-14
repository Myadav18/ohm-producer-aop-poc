package net.apmoller.crb.ohm.microservices.producer.library;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The application class for the kafkaproducer spring boot service.
 */
@SpringBootApplication
public class ProducerLibraryApplication {

    /**
     * Standalone spring boot starter.
     *
     * @param args
     *            arguments for the spring boot app run.
     */
    public static void main(String... args) {
        SpringApplication.run(ProducerLibraryApplication.class, args);
    }
}
