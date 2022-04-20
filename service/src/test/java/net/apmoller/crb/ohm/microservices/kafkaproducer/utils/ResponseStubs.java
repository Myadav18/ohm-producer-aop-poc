package net.apmoller.crb.ohm.microservices.kafkaproducer.utils;

import net.apmoller.crb.ohm.microservices.kafkaproducer.models.User;

public class ResponseStubs {

    public static final String NAME = "Komal";
    public static final String DEPT = "Maersk";

    public static User createUser() {
        User user = User.builder().build();
        user.setName(NAME);
        user.setDept(DEPT);
        return user;
    }
}
