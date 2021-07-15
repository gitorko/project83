package com.demo.project83;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Slf4j
public class ReactorObjectTest {

    @Test
    void fluxIntersectCommonApproach1() {
        Flux<ProjectDTO> fluxFromRequest = Flux.just(
                ProjectDTO.builder().name("p3").build(),
                ProjectDTO.builder().name("p1").build(),
                ProjectDTO.builder().name("p5").build()
        );

        Flux<ProjectEntity> fluxFromDb = Flux.just(
                ProjectEntity.builder().entityName("p5").build(),
                ProjectEntity.builder().entityName("p4").build(),
                ProjectEntity.builder().entityName("p1").build(),
                ProjectEntity.builder().entityName("p2").build()
        );

        Flux<ProjectEntity> commonFlux = fluxFromDb.filter(f -> {
            //Inefficient
            //Not for live stream or stream that can be subscribed only once.
            //toSteam converts your non-blocking asynchronous flux to a blocking stream API which will impact performance
            return fluxFromRequest.toStream().anyMatch(e -> e.getName().equals(f.getEntityName()));
        });
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext(ProjectEntity.builder().entityName("p5").build())
                .expectNext(ProjectEntity.builder().entityName("p1").build())
                .verifyComplete();
    }

    @Test
    void fluxIntersectCommonApproach2() {
        Flux<ProjectDTO> fluxFromRequest = Flux.just(
                ProjectDTO.builder().name("p3").build(),
                ProjectDTO.builder().name("p1").build(),
                ProjectDTO.builder().name("p5").build()
        );

        Flux<ProjectEntity> fluxFromDb = Flux.just(
                ProjectEntity.builder().entityName("p5").build(),
                ProjectEntity.builder().entityName("p4").build(),
                ProjectEntity.builder().entityName("p1").build(),
                ProjectEntity.builder().entityName("p2").build()
        );

        Flux<ProjectEntity> commonFlux = fluxFromRequest
                .map(dto -> dto.getName())
                .collect(Collectors.toSet())
                .flatMapMany(set -> {
                    return fluxFromDb
                            //Filter out matching
                            //Limitation is that you can only compare 1 value collected in set.
                            .filter(t -> set.contains(t.getEntityName()));
                });
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext(ProjectEntity.builder().entityName("p5").build())
                .expectNext(ProjectEntity.builder().entityName("p1").build())
                .verifyComplete();
    }

    @Test
    void fluxIntersectCommonApproach3() {
        Flux<ProjectDTO> fluxFromRequest = Flux.just(
                ProjectDTO.builder().name("p3").build(),
                ProjectDTO.builder().name("p1").build(),
                ProjectDTO.builder().name("p5").build()
        );

        Flux<ProjectEntity> fluxFromDb = Flux.just(
                ProjectEntity.builder().entityName("p5").build(),
                ProjectEntity.builder().entityName("p4").build(),
                ProjectEntity.builder().entityName("p1").build(),
                ProjectEntity.builder().entityName("p2").build()
        );

        Flux<ProjectEntity> commonFlux = fluxFromDb.join(fluxFromRequest, s -> Flux.never(), s -> Flux.never(), Tuples::of)
                //Filter out matching
                .filter(t -> t.getT1().getEntityName().equals(t.getT2().getName()))
                //Revert to single value
                .map(Tuple2::getT1)
                //Remove duplicates, if any
                .groupBy(f -> f)
                .map(GroupedFlux::key);
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext(ProjectEntity.builder().entityName("p1").build())
                .expectNext(ProjectEntity.builder().entityName("p5").build())
                .verifyComplete();
    }

    @Test
    void postGetAllTest() {
        DbService service = new DbService();
        Flux<Post> postFlux = service.getAllPosts()
                .flatMap(post -> {
                    Mono<List<Comment>> commentMono = service.getCommentByPostId(post.id).collectList();
                    return commentMono.map(comments -> Post.builder()
                            .id(post.id)
                            .message(post.message)
                            .user(post.user)
                            .comments(comments)
                            .build());
                });
        StepVerifier.create(postFlux)
                .assertNext(post -> {
                    assertEquals("post 1", post.getMessage());
                    assertEquals(1, post.getComments().size());
                })
                .assertNext(post -> {
                    assertEquals("post 2", post.getMessage());
                    assertEquals(2, post.getComments().size());
                })
                .verifyComplete();
    }

    @Test
    void postGetByIdTest() {
        DbService service = new DbService();
        Mono<List<Comment>> commentMono = service.getCommentByPostId(1l).collectList();
        Mono<Post> getByIdMono = service.getPostById(1l).zipWith(commentMono, (post, comments) -> {
            return Post.builder()
                    .id(post.id)
                    .message(post.message)
                    .user(post.user)
                    .comments(comments)
                    .build();
        });
        StepVerifier.create(getByIdMono)
                .assertNext(post -> {
                    assertEquals("post 1", post.getMessage());
                    assertEquals(1, post.getComments().size());
                })
                .verifyComplete();
    }

}

@Data
@Builder
class ProjectDTO {
    String name;
    String someExtraField;
}

@Data
@Builder
class ProjectEntity {
    String entityName;
    String dbField;
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
class Post {
    Long id;
    String message;
    String user;
    @Builder.Default
    List<Comment> comments = new ArrayList<>();
}

@Data
@AllArgsConstructor
@RequiredArgsConstructor
@Builder
class Comment {
    Long id;
    Long postId;
    String comment;
    String user;
}

class DbService {
    Flux<Post> postFlux = Flux.fromIterable(List.of(
            Post.builder().id(1l).message("post 1").user("jack").build(),
            Post.builder().id(2l).message("post 2").user("jill").build()));

    Flux<Comment> commentFlux = Flux.fromIterable(List.of(
            Comment.builder().id(1l).postId(1l).comment("comment 1").user("adam").build(),
            Comment.builder().id(2l).postId(2l).comment("comment 2").user("jane").build(),
            Comment.builder().id(3l).postId(2l).comment("comment 3").user("raj").build()));

    //Get all posts
    Flux<Post> getAllPosts() {
        return postFlux;
    }

    Mono<Post> getPostById(long id) {
        return postFlux.filter(e -> e.id == id).next();
    }

    //Get all reviews associated with the post.
    Flux<Comment> getCommentByPostId(long id) {
        return commentFlux.filter(e -> e.postId == id);
    }
}
