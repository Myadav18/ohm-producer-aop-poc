package net.apmoller.crb.ohm.microservices.producer.library.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MicroMeterConfig {
    private final Counter claimsCheckTargetTopicErrorCount;
    private final Counter claimsCheckDltTopicErrorCount;

    private final Counter singleProducerTargetTopicErrorCount;

    private final Counter multipleProducerTargetTopicErrorCount;

    @Autowired
    public MicroMeterConfig(MeterRegistry meterRegistry) {
        this.claimsCheckTargetTopicErrorCount = meterRegistry
                .counter("kafka_producer_claims_check_target_topic_error_total");
        this.claimsCheckDltTopicErrorCount = meterRegistry.counter("kafka_producer_claims_check_dlt_topic_error_total");
        this.singleProducerTargetTopicErrorCount = meterRegistry
                .counter("kafka_single_producer_target_topic_record_error_total");
        this.multipleProducerTargetTopicErrorCount = meterRegistry
                .counter("kafka_multiple_producer_target_topic_record_error_total");
    }

    /**
     * @param event
     */
    public void incrementCounter(String event) {
        try {
            if ("claimsCheckTargetTopicErrorCount".equals(event)) {
                claimsCheckTargetTopicErrorCount.increment();
            } else if ("claimsCheckDltTopicErrorCount".equals(event)) {
                claimsCheckDltTopicErrorCount.increment();
            } else if ("singleProducerTargetTopicErrorCount".equals(event)) {
                singleProducerTargetTopicErrorCount.increment();
            } else if ("multipleProducerTargetTopicErrorCount".equals(event)) {
                multipleProducerTargetTopicErrorCount.increment();
            }
        } catch (Exception e) {
            log.info("exception occured in updating counter : " + e);
        }
    }
}
