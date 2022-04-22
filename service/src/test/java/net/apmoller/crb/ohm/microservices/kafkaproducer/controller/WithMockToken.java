package net.apmoller.crb.ohm.microservices.kafkaproducer.controller;

import org.springframework.boot.autoconfigure.security.oauth2.resource.OAuth2ResourceServerProperties;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
//import org.springframework.security.oauth2.jwt.Jwt;
//import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.test.context.support.WithSecurityContext;
import org.springframework.security.test.context.support.WithSecurityContextFactory;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

@Retention(RetentionPolicy.RUNTIME)
@WithSecurityContext(factory = net.apmoller.crb.ohm.microservices.kafkaproducer.controller.WithMockTokenSecurityContextFactory.class)
public @interface WithMockToken {
    String sub() default "uuid";

    String email() default "test@test.com";

    String name() default "test";
}

class WithMockTokenSecurityContextFactory implements WithSecurityContextFactory<WithMockToken> {
    String token = "Bearer eyJ0eXAiOiJKV1QiLCJraWQiOiJaRHNwdjFHSDlXcEFGQWErdTFZY1dPM2NZeVU9IiwiYWxnIjoiUlMyNTYifQ.eyJhdF9oYXNoIjoia0R4R0x3X2NPUDRHX3JsXzlpQ01PQSIsInN1YiI6IkZpWDZjT01jazhwVEZtQXRyRWRWNHNLSmFoVktmTFBYeDJTVmY2S2JKZmMiLCJjb3VudHJ5IjoiREsiLCJmaXJzdG5hbWUiOiJKeW90aSIsImF1ZGl0VHJhY2tpbmdJZCI6IjBiM2U5OTVkLWZmYjgtNDkzOS05NjA3LTljZDQzOGM0MzZkMS0xMTE5MjkxMCIsInJvbGVzIjpbIk1hZXJza0ludGVybmFsIiwiQ29udHJhY3RSYXRlIiwiRG9jdW1lbnRhdGlvbiIsIkJhc2ljQ3VzdG9tZXIiLCJJbXBvcnRDU0EiLCJXQk9MQXBwcm92ZXIiLCJCb29raW5nIiwiSW52b2ljZXMiLCJBbGxvY2F0aW9uTWFuYWdlciJdLCJpc3MiOiJodHRwczovL2lhbS1zdGFnZS5tYWVyc2suY29tL2FjbS9vYXV0aDIvbWF1IiwidG9rZW5OYW1lIjoiaWRfdG9rZW4iLCJvZmZpY2UiOiJBYXJodXMgLSBESyIsImFjciI6IjAiLCJhenAiOiJiY2EwMDEiLCJhdXRoX3RpbWUiOjE2MTQyNTQ1MDMsInBlcnNvbmlkIjoiMTAwMDA3ODg2MDEiLCJpcGFkZHIiOiIxOTUuODUuMjA0Ljk4IiwiZXhwIjoxNjE0MjYxNzA2LCJjdXN0b21lcl9jb2RlIjoiMTAwMDAwMDc5NTEiLCJpYXQiOjE2MTQyNTQ1MDYsImVtYWlsIjoiSnlvdGkuS3VtYXJpMUBtYWVyc2suY29tIiwibm9uY2UiOiJNalBRMWJIeDgzVmFURW1udndBOCIsImxhc3RuYW1lIjoiS3VtYXJpIiwiYXVkIjoiYmNhMDAxIiwiY19oYXNoIjoiUm1iczdVQy1pM3R2WWZoakp2dEY0QSIsImNhcnJpZXIiOiJNQUVVIiwibmFtZSI6Ikp5b3RpIEt1bWFyaSIsInJlYWxtIjoiL21hdSIsInRva2VuVHlwZSI6IkpXVFRva2VuIiwidXNlcm5hbWUiOiJKeW90aS5LdW1hcmkxQG1hZXJzay5jb20ifQ.jSzc36Fndzkl2rbcy7qNLcZ17GjwSEH-6O23B6wHBMzOm8oRD3ep0jnJGSjmSNMEGQ8w8nEge0vaiz8oM8IkZqQJgCeeN86zYqPQo0NTsNmzxGSvlsiv_6FqEXgWJDnijW4N3ozI8YHOEo8YDYM05ptlVf5X86Ou4Eg0lC8LDHqgN5Q2u4_Yxmo07CDa5juGzjDV3KuD1txKw494WxK2lSSmYVXmDG7QTiAIV221FglPRlRQqwm2uKskFED7kLrdMS5TlqWVx_rV1fsV2zQfEa4N1RGDtnHX9K4gwDik6TNl8H26ZHs2QYqB-SRwT4b_7gpjCUIN9ZEoBKvPA0RwFA";

    @Override
    public SecurityContext createSecurityContext(
            net.apmoller.crb.ohm.microservices.kafkaproducer.controller.WithMockToken tokenAnnotation) {
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        HashMap<String, Object> headers = new HashMap<>();
        headers.put("kid", "SOME_ID");
        headers.put("typ", "JWT");
        headers.put("alg", "RS256");
        HashMap<String, Object> claims = new HashMap<>();
        claims.put("sub", tokenAnnotation.sub());
        claims.put("aud", new ArrayList<>() {
            {
                add("SOME_ID_HERE");
            }
        });
        claims.put("roles", new ArrayList<>() {
            {
                add("MaerskInternal");
            }
        });
        claims.put("scope", new ArrayList<>() {
            {
                add("ReadOnly");
            }
        });
        claims.put("updated_at", "2019-06-24T12:16:17.384Z");
        claims.put("nickname", tokenAnnotation.email().substring(0, tokenAnnotation.email().indexOf("@")));
        claims.put("name", tokenAnnotation.name());
        claims.put("username", tokenAnnotation.name());
        claims.put("exp", new Date());
        claims.put("iat", new Date());
        claims.put("email", tokenAnnotation.email());
        /*
         * OAuth2ResourceServerProperties.Jwt jwt = new OAuth2ResourceServerProperties.Jwt(token, Instant.now(),
         * Instant.now().plus(1, ChronoUnit.HOURS), headers, claims); JwtAuthenticationToken jwtAuthenticationToken =
         * new JwtAuthenticationToken(jwt, AuthorityUtils.NO_AUTHORITIES); // Authorities are needed to pass
         * authentication in the Integration tests context.setAuthentication(jwtAuthenticationToken);
         */

        return context;
    }

}
