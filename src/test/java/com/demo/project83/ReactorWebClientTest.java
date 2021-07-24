package com.demo.project83;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(SpringExtension.class)
@SpringBootTest
public class ReactorWebClientTest {

    static String HOST = "https://jsonplaceholder.typicode.com";

    @Test
    public void getApi() {
        Mono<String> mono = WebClient.create(HOST)
                .get()
                .uri("/todos/{0}", 1)
                .accept(MediaType.APPLICATION_JSON)
                .exchangeToMono(response -> response.bodyToMono(Map.class))
                .map(response -> response.get("title").toString());

        StepVerifier.create(mono)
                .assertNext(e -> {
                    Assertions.assertNotNull(e);
                }).verifyComplete();

    }
}
