package net.apmoller.crb.ohm.microservices.kafkaproducer.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.kafkaproducer.models.User;
import net.apmoller.crb.ohm.microservices.kafkaproducer.services.SubmissionService;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;


@RestController
@RequiredArgsConstructor
@Slf4j
class KafkaproducerController {

private final SubmissionService submissionService;

@PostMapping(path = "/user")
public Mono<User> getUser(@RequestParam(name = "name") String user_name,
                          @RequestParam(value = "dept") String user_dept){

    log.info("request received for KafkaproducerController");

 return Mono.just(submissionService.submit(user_name,user_dept));
  }
}
