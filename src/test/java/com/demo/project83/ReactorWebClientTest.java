package com.demo.project83;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@Slf4j
public class ReactorWebClientTest {

    static String HOST = "https://jsonplaceholder.typicode.com";

    @Test
    public void getApi() {
        Mono<String> mono = WebClient.create(HOST)
                .get()
                .uri("/posts/{0}", 1)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .exchangeToMono(response -> response.bodyToMono(Map.class))
                .map(response -> response.get("title").toString());

        StepVerifier.create(mono)
                .assertNext(e -> {
                    log.info("title: {}", e);
                    Assertions.assertNotNull(e);
                }).verifyComplete();

    }

    @Test
    public void getApi2() {
        Mono<PostEntity> mono = WebClient.create(HOST)
                .get()
                .uri("/posts/{0}", 1)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(PostEntity.class);

        StepVerifier.create(mono)
                .assertNext(e -> {
                    log.info("title: {}", e);
                    Assertions.assertNotNull(e);
                }).verifyComplete();

    }

    @Test
    public void getAllApi() {
        Flux<PostEntity> mono = WebClient.create(HOST)
                .get()
                .uri("/posts")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToFlux(PostEntity.class);

        StepVerifier.create(mono)
                .assertNext(e -> {
                    log.info("title: {}", e);
                    Assertions.assertNotNull(e);
                }).expectComplete();

    }

    @Test
    public void postApi() {
        PostEntity postEntity = PostEntity.builder()
                .userId(1)
                .title("my post")
                .body("hello world")
                .build();
        Mono<PostEntity> mono = WebClient.create(HOST)
                .post()
                .uri("/posts/")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(Mono.just(postEntity), PostEntity.class)
                .retrieve()
                .bodyToMono(PostEntity.class);

        StepVerifier.create(mono)
                .assertNext(e -> {
                    log.info("post: {}", e);
                    Assertions.assertNotNull(e.getId());
                }).verifyComplete();

    }

    @Test
    public void deleteApi() {
        Mono<Void> mono = WebClient.create(HOST)
                .delete()
                .uri("/posts/{0}", 1)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(Void.class);

        StepVerifier.create(mono)
                .verifyComplete();

    }
}

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class PostEntity {
    int userId;
    int id;
    String title;
    String body;
}
