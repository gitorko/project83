package com.demo.project83;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.demo.project83.common.PostEntity;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.resolver.DefaultAddressResolverGroup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.test.StepVerifier;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@Slf4j
public class ReactorWebClientTest {

    static String HOST = "https://jsonplaceholder.typicode.com";

    @Test
    public void getPostTitle() {
        Mono<String> mono = getWebClient()
                .get()
                .uri(HOST + "/posts/{0}", 1)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .exchangeToMono(response -> response.bodyToMono(Map.class))
                .map(response -> response.get("title").toString());

        StepVerifier.create(mono)
                .assertNext(e -> {
                    log.info("title: {}", e);
                    assertNotNull(e);
                }).verifyComplete();
    }

    @Test
    public void getPostEntity() {
        Mono<PostEntity> mono = getWebClient()
                .get()
                .uri(HOST + "/posts/{0}", 1)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(PostEntity.class);

        StepVerifier.create(mono)
                .assertNext(e -> {
                    log.info("title: {}", e);
                    assertNotNull(e);
                }).verifyComplete();
    }

    @Test
    public void getAllPosts() {
        Flux<PostEntity> flux = getWebClient()
                .get()
                .uri(HOST + "/posts")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToFlux(PostEntity.class);

        StepVerifier.create(flux)
                .assertNext(e -> {
                    System.out.println(e);
                    log.info("title: {}", e);
                    assertNotNull(e);
                }).expectComplete();
    }

    @Test
    public void savePost() {
        PostEntity postEntity = PostEntity.builder()
                .userId(1)
                .title("my post")
                .body("hello world")
                .build();
        Mono<PostEntity> mono = getWebClient()
                .post()
                .uri(HOST + "/posts/")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(Mono.just(postEntity), PostEntity.class)
                .retrieve()
                .bodyToMono(PostEntity.class);

        StepVerifier.create(mono)
                .assertNext(e -> {
                    log.info("post: {}", e);
                    assertNotNull(e.getId());
                }).verifyComplete();
    }

    @Test
    public void deletePost() {
        Mono<Void> mono = getWebClient()
                .delete()
                .uri(HOST + "/posts/{0}", 1)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(Void.class);

        StepVerifier.create(mono)
                .verifyComplete();
    }

    @Test
    public void getAllPostsPagination() {
        Mono<Page<PostEntity>> mono = getWebClient()
                .get()
                .uri(HOST + "/posts")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToFlux(PostEntity.class)
                .collectList()
                .map(t -> new PageImpl<>(t, Pageable.ofSize(t.size()), t.size()));

        StepVerifier.create(mono)
                .assertNext(e -> {
                    assertEquals(e.getTotalElements(), 100);
                }).verifyComplete();

    }

    @Test
    public void getAllPostsPageByPage() {
        PageRequest pageRequest = PageRequest.of(1, 10);
        Function<Pageable, Mono<Page<PostEntity>>> pageSupplier = p -> getSinglePage(p)
                .onErrorReturn(Page.empty());

        StepVerifier.create(getAllPages(pageRequest, pageSupplier))
                .assertNext(e -> {
                    assertEquals(e.size(), 100);
                })
                .verifyComplete();
    }

    private Mono<List<PostEntity>> getAllPages(PageRequest pageRequest, Function<Pageable, Mono<Page<PostEntity>>> pageSupplier) {
        AtomicReference<Pageable> loRef = new AtomicReference<>(pageRequest);
        AtomicInteger counter = new AtomicInteger(1);
        return Mono.defer(() -> pageSupplier.apply(loRef.get()))
                .repeat()
                .takeUntil(page -> {
                    final PageRequest next;
                    next = PageRequest.of(counter.incrementAndGet(), 10);
                    loRef.set(next);
                    return !(page.getTotalElements() > 0);
                })
                .collect(Collectors.flatMapping(Page::stream, Collectors.toList()));
    }

    private Mono<Page<PostEntity>> getSinglePage(Pageable pageable) {
        log.info("Fetching page: {}", pageable);
        return getWebClient()
                .get()
                .uri(HOST + "/posts?_page=" + pageable.getPageNumber() + "&_size=" + pageable.getPageSize())
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToFlux(PostEntity.class)
                .collectList()
                .map(t -> new PageImpl<>(t, Pageable.ofSize(t.size()), t.size()));
    }

    @SneakyThrows
    private WebClient getWebClient() {
        SslContext sslContext = SslContextBuilder
                .forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
        WebClient webClient = WebClient
                .builder()
                .clientConnector(new ReactorClientHttpConnector(
                                HttpClient.create()
                                        .secure(t -> t.sslContext(sslContext))
                                        .resolver(DefaultAddressResolverGroup.INSTANCE)
                        )
                ).build();
        return webClient;
    }

}


