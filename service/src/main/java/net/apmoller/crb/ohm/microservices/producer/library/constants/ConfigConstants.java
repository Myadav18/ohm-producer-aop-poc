package net.apmoller.crb.ohm.microservices.producer.library.constants;

public class ConfigConstants {

    public static final String NOTIFICATION_TOPIC = "${kafka.notification.topic}";
    public static final String BOOTSTRAP_SERVER = "${kafka.bootstrapserver}";
    public static final String RETRY_TOPIC = "${kafka.retry.topic}";
}
