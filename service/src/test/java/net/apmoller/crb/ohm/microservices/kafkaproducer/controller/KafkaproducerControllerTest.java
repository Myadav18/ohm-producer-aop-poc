package net.apmoller.crb.ohm.microservices.kafkaproducer.controller;

import net.apmoller.crb.ohm.microservices.kafkaproducer.models.User;
import net.apmoller.crb.ohm.microservices.kafkaproducer.services.SubmissionService;
import net.apmoller.crb.ohm.microservices.kafkaproducer.utils.ResponseStubs;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf;
import net.apmoller.crb.ohm.microservices.kafkaproducer.controllers.WithMockToken;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient(timeout = "1000000")
@WithMockToken()
class KafkaproducerControllerTest {

    @Autowired
    private WebTestClient webTestClient;
    @MockBean
    private SubmissionService submissionService;

    @Test
    void test_ResponseOK() {

        User user = ResponseStubs.createUser();

        when(submissionService.submit(Mockito.any(),Mockito.any())).thenReturn(user);

        webTestClient.mutateWith(csrf()).post().uri(builder -> builder.path("/user")
                        .queryParam("name", "komal")
                        .queryParam("dept","maersk").build())
                .exchange().expectStatus().is2xxSuccessful();
    }
}
