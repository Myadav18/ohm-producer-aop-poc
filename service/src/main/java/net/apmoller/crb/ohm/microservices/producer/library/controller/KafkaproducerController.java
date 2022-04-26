package net.apmoller.crb.ohm.microservices.producer.library.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.apmoller.crb.ohm.microservices.producer.library.models.User;
import net.apmoller.crb.ohm.microservices.producer.library.services.SubmissionService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@Slf4j
class KafkaproducerController {

    private final SubmissionService submissionService;

    @PostMapping(path = "/user")
    public User getUser(@RequestParam(name = "name") String user_name, @RequestParam(value = "dept") String user_dept) {

        log.info("request received for KafkaproducerController");

        return submissionService.submit(user_name, user_dept);
    }
}
