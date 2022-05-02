// package net.apmoller.crb.ohm.microservices.producer.library.controller;
//
// import net.apmoller.crb.ohm.microservices.producer.library.models.User;
// import net.apmoller.crb.ohm.microservices.producer.library.services.SubmissionService;
// import net.apmoller.crb.ohm.microservices.producer.library.utils.ResponseStubs;
// import org.junit.jupiter.api.Test;
// import org.mockito.Mockito;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.test.context.SpringBootTest;
// import org.springframework.boot.test.mock.mockito.MockBean;
//
// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.mockito.Mockito.when;
//
// @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//// @AutoConfigureWebTestClient(timeout = "1000000")
// @WithMockToken()
// class KafkaproducerControllerTest {
//
// /*
// * @Autowired private WebTestClient webTestClient;
// */
// @MockBean
// private SubmissionService submissionService;
//
// @Autowired
// KafkaproducerController kafkaproducerController;
//
// @Test
// void test_ResponseOK() {
//
// User user = ResponseStubs.createUser();
//
// when(submissionService.submit(Mockito.any(), Mockito.any())).thenReturn(user);
// User user1 = kafkaproducerController.getUser(user.getName(), user.getDept());
//
// Mockito.verify(submissionService, Mockito.times(1)).submit(Mockito.any(), Mockito.any());
// assertEquals("Komal", user1.getName());
// assertEquals("Maersk", user1.getDept());
// }
// }
//
/// *
// * webTestClient.mutateWith(csrf()).post() .uri(builder -> builder.path("/user").queryParam("name",
// * "komal").queryParam("dept", "maersk").build()) .exchange().expectStatus().is2xxSuccessful(); }
// */
