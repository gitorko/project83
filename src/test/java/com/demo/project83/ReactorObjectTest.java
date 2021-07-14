package com.demo.project83;

import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
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
                .expectNext(ProjectEntity.builder().entityName("p5").build())
                .expectNext(ProjectEntity.builder().entityName("p1").build())
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
