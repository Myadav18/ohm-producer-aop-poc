package net.apmoller.crb.ohm.microservices.kafkaproducer.producer;

import net.apmoller.crb.ohm.microservices.kafkaproducer.models.User;
import net.apmoller.crb.ohm.microservices.kafkaproducer.utils.ResponseStubs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = { KafkaMessageGeneratorImpl.class })
class KafkaMessageGeneratorTest {

    @Autowired
    private KafkaMessageGeneratorImpl kafkaMessageGenerator;

    @Test
    public void test_getMessage() {
        User user = ResponseStubs.createUser();
        String res = kafkaMessageGenerator.getMessage(user);
        String message = "{\"name\":\"" + user.getName() + "\",\"dept\":\"" + user.getDept() + "\"}";
        Assertions.assertEquals(res, message);
    }
}
