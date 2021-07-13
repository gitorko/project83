package com.demo.project83;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Slf4j
public class ReactorDemo {

    /**
     * ********************************************************************
     *  mono
     * ********************************************************************
     */
    @Test
    void monoTest() {
        //justOrEmpty
        Mono<String> mono1 = Mono.justOrEmpty("Jack");
        mono1.subscribe(System.out::println);
        StepVerifier.create(mono1)
                .expectNext("Jack")
                .verifyComplete();

        //Note: Reactive Streams do not accept null values
        Mono<String> mono2 = Mono.justOrEmpty(null);
        mono2.subscribe(System.out::println);
        StepVerifier.create(mono2)
                .verifyComplete();

        //Default value if empty.
        Mono<String> mono3 = mono2.defaultIfEmpty("Jill");
        mono3.subscribe(System.out::println);
        StepVerifier.create(mono3)
                .expectNext("Jill")
                .verifyComplete();

        //Use log to look at transitions.
        Mono<String> mono4 = Mono.just("Jack").log();
        mono4.subscribe(s -> {
            log.info("Got: {}", s);
        });
        StepVerifier.create(mono4)
                .expectNext("Jack")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux
     * ********************************************************************
     */
    @Test
    void fluxTest() {
        Flux flux = Flux.just("Jack", "Jill");
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("Jack", "Jill")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux - Avoid blocking calls that will hold thread
     * ********************************************************************
     */
    @Test
    void fluxSleepTest() {
        Flux flux = Flux.just("Jack", "Jill").map(e -> {
            log.info("Received: {}", e);
            //Bad idea to do Thread.sleep or any blocking call.
            //Instead use delayElements.
            return e;
        }).delayElements(Duration.ofSeconds(1));
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("Jack", "Jill")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux filter
     * ********************************************************************
     */
    @Test
    void fluxFilterTest() {
        //Get even numbers
        Flux flux = Flux.just(1, 2, 3, 4, 5)
                .filter(i -> i % 2 == 0);
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext(2, 4)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux array, list, stream
     * ********************************************************************
     */
    @Test
    public void fluxArrayTest() {
        Integer[] arr = {2, 5, 7, 8};
        Flux<Integer> flux1 = Flux.fromArray(arr);
        flux1.subscribe(System.out::println);
        StepVerifier.create(flux1)
                .expectNext(2, 5, 7, 8)
                .verifyComplete();

        List<String> fruitsList = Arrays.asList("apple", "oranges", "grapes");
        Flux<String> fruits = Flux.fromIterable(fruitsList);
        StepVerifier.create(fruits)
                .expectNext("apple", "oranges", "grapes")
                .verifyComplete();

        Stream<Integer> stream = List.of(1, 2, 3, 4, 5).stream();
        Flux<Integer> flux2 = Flux.fromStream(() -> stream);
        //Stream can be consumed only once
        StepVerifier.create(flux2)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux range
     * ********************************************************************
     */
    @Test
    public void fluxRangeTest() {
        Flux<Integer> flux1 = Flux.range(3, 2)
                .log()
                .map(i -> i + 100)
                .log();
        flux1.subscribe(System.out::println);
        StepVerifier.create(flux1)
                .expectNext(103, 104)
                .verifyComplete();

        Flux<Integer> numbers = Flux.range(1, 5);
        numbers.subscribe(n -> {
            log.info("number: {}", n);
        });
        StepVerifier.create(numbers)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  map
     * ********************************************************************
     */
    @Test
    public void fluxMapTest() {
        Flux<Integer> flux = Flux.just("Jack", "Ram")
                .log()
                .map(i -> i.length());
        StepVerifier
                .create(flux)
                .expectNext(4, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flatMap - should be used for non-blocking operations, or in short anything which returns back Mono,Flux.
     *  map - should be used when you want to do the transformation of an object /data in fixed time, synchronously
     * ********************************************************************
     */
    @Test
    void flatMapTest() {
        //Modification of object in chain - done via flatMap
        Mono<String> mono1 = Mono.just("Jack")
                .flatMap(ReactorDemo::appendGreet);
        StepVerifier.create(mono1)
                .expectNext("Hello Jack")
                .verifyComplete();

        //Modification of object in chain - done via zipWith
        Mono<String> mono2 = Mono.just("Jack")
                .zipWith(Mono.just("Hello "), ReactorDemo::getGreet);
        StepVerifier.create(mono2)
                .expectNext("Hello Jack")
                .verifyComplete();
    }

    private static Mono<String> appendGreet(String name) {
        return Mono.just("Hello " + name);
    }

    private static String getGreet(String name, String greet) {
        return greet + name;
    }

    /**
     * ********************************************************************
     *  flatMap
     * ********************************************************************
     */
    @Test
    void flatMap2Test() {
        Flux<String> flux = Flux.fromIterable(List.of("Jack", "Joe", "Jill"))
                .map(String::toUpperCase)
                .filter(s -> s.length() > 3)
                .flatMap(s -> splitString(s))
                .log();
        flux.subscribe(System.out::println);
        //Checking only first 2 chars.
        StepVerifier.create(flux)
                .expectNext("J", "A")
                .expectComplete();

        Flux<Integer> fluxFromJust = Flux.range(1, 3).log();
        Flux<Integer> integerFlux = fluxFromJust
                .flatMap(i -> getSomeFlux(i));
        StepVerifier
                .create(integerFlux)
                .expectNextCount(30)
                .verifyComplete();

        Flux<Integer> flux2 = Flux.just(1, 5, 10)
                .flatMap(num -> Flux.just(num * 10));
        StepVerifier
                .create(flux2)
                .expectNextCount(3)
                .verifyComplete();

        Flux flux3 = Flux.just("Jack", "Jill").flatMap(ReactorDemo::capitalize);
        flux3.subscribe(System.out::println);
        StepVerifier.create(flux3)
                .expectSubscription()
                .expectNext("JACK")
                .expectNext("JILL")
                .verifyComplete();

    }

    private Flux<String> splitString(String name) {
        return Flux.fromArray(name.split(""));
    }

    private Flux<Integer> getSomeFlux(Integer i) {
        return Flux.range(i, 10);
    }

    private static Mono<String> capitalize(String user) {
        return Mono.just(user.toUpperCase());
    }

    /**
     * ********************************************************************
     *  intersect - compare 2 flux for common
     * ********************************************************************
     */
    @Test
    void fluxIntersectCommon() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana").log();
        Flux<String> flux2 = Flux.just("apple", "orange").log();

        Flux<String> commonFlux = flux1.filterWhen(f -> ReactorDemo.checList1(flux2, f));
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    private static Mono<Boolean> checList1(Flux<String> flux, String fruit) {
        return flux.hasElement(fruit);
    }

    /**
     * ********************************************************************
     *  intersect - compare 2 flux for diff
     * ********************************************************************
     */
    @Test
    void fluxIntersectDiff() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana").log();
        Flux<String> flux2 = Flux.just("apple", "orange").log();

        Flux<String> diffFlux = flux1.filterWhen(f -> ReactorDemo.checList2(flux2, f));
        diffFlux.subscribe(System.out::println);
        StepVerifier.create(diffFlux)
                .expectNext("banana")
                .verifyComplete();
    }

    private static Mono<Boolean> checList2(Flux<String> flux, String fruit) {
        return flux.hasElement(fruit)
                .map(hasElement -> !hasElement);
    }

    /**
     * ********************************************************************
     *  flatMapMany - convert mono to flux
     * ********************************************************************
     */
    @Test
    void flatMapManyTest() {
        Flux<String> flux = Mono.just("the quick brown fox jumps over the lazy dog")
                .flatMapMany(word -> Flux.fromArray(word.split("")))
                .distinct()
                .sort();
        flux.subscribe(System.out::println);
        //26 letters in the alphabet
        StepVerifier.create(flux)
                .expectNextCount(26)
                .expectComplete();

        //Converts Mono of list to Flux.
        Mono<List<Integer>> mono = Mono.just(Arrays.asList(1, 2, 3));
        Flux<Integer> integerFlux1 = mono.flatMapMany(it -> Flux.fromIterable(it));
        integerFlux1.subscribe(System.out::println);
        StepVerifier
                .create(integerFlux1)
                .expectNext(1, 2, 3)
                .verifyComplete();

        Flux<Integer> integerFlux2 = mono.flatMapIterable(list -> list);
        integerFlux2.subscribe(System.out::println);
        StepVerifier
                .create(integerFlux2)
                .expectNext(1, 2, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  concatMap - Same as flatMap but order is preserved.
     *  concatMap takes more time but ordering is preserved.
     *  flatMap takes less time but ordering is lost.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void concatMapTest() {
        Flux.fromIterable(List.of("Jack", "Joe", "Jill"))
                .map(String::toUpperCase)
                .filter(s -> s.length() > 3)
                .concatMap(s -> splitStringAsync(s))
                .log()
                .subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(5);
    }

    private Flux<String> splitStringAsync(String name) {
        return Flux.fromArray(name.split(""))
                .delayElements(Duration.ofMillis(new Random().nextInt(1000)));
    }

    /**
     * ********************************************************************
     *  startWith - add new element to flux.
     * ********************************************************************
     */
    @Test
    public void startWith() {
        Flux<Integer> flux = Flux.range(1, 3);
        Flux<Integer> integerFlux = flux.startWith(0);
        StepVerifier.create(integerFlux)
                .expectNext(0, 1, 2, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  tuple
     * ********************************************************************
     */
    @Test
    void fluxIndex() {
        //append a number to each element.
        Flux<Tuple2<Long, String>> index = Flux
                .just("apple", "banana", "orange")
                .index();
        StepVerifier.create(index)
                .expectNext(Tuples.of(0L, "apple"))
                .expectNext(Tuples.of(1L, "banana"))
                .expectNext(Tuples.of(2L, "orange"))
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  takeWhile & skipWhile
     * ********************************************************************
     */
    @Test
    void takeWhileTest() {
        Flux<Integer> fluxFromJust = Flux.range(1, 10).log();
        Flux<Integer> takeWhile = fluxFromJust.takeWhile(i -> i <= 5);
        StepVerifier
                .create(takeWhile)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

        Flux<Integer> skipWhile = fluxFromJust.skipWhile(i -> i <= 5);
        StepVerifier
                .create(skipWhile)
                .expectNext(6, 7, 8, 9, 10)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux to mono list
     * ********************************************************************
     */
    @Test
    void fluxToMonoList() {
        Flux<String> flux = Flux.just("Jack", "Jill");
        Mono<List<String>> mono = flux.collectList();
        mono.subscribe(System.out::println);
        StepVerifier.create(mono)
                .expectNext(Arrays.asList("Jack", "Jill"))
                .verifyComplete();

        Mono<List<Integer>> listMono1 = Flux
                .just(1, 2, 3)
                .collectList();
        StepVerifier.create(listMono1)
                .expectNext(Arrays.asList(1, 2, 3))
                .verifyComplete();

        Mono<List<Integer>> listMono2 = Flux
                .just(5, 2, 4, 1, 3)
                .collectSortedList();
        StepVerifier.create(listMono2)
                .expectNext(Arrays.asList(1, 2, 3, 4, 5))
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  mono error
     * ********************************************************************
     */
    @Test
    void monoError() {
        Mono<Object> mono = Mono.error(new RuntimeException("My Error"))
                .onErrorReturn("Jack");
        mono.subscribe(System.out::println);
        StepVerifier.create(mono)
                .expectNext("Jack")
                .verifyComplete();

        Mono<Object> mono2 = Mono.error(new RuntimeException("My Error"))
                .onErrorResume(e -> Mono.just("Jack"));
        mono2.subscribe(System.out::println);
        StepVerifier.create(mono2)
                .expectNext("Jack")
                .verifyComplete();

        Mono<Object> error = Mono.error(new IllegalArgumentException())
                .doOnError(e -> log.error("Error: {}", e.getMessage()))
                .onErrorResume(s -> {
                    log.info("Inside on onErrorResume");
                    return Mono.just("Jack");
                })
                .log();
        StepVerifier.create(error)
                .expectNext("Jack")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux error propogate
     * ********************************************************************
     */
    @Test
    void errorPropogate() {
        Flux flux = Flux.just("Jack", "Jill").map(u -> {
            try {
                return ReactorDemo.checkName(u);
            } catch (CustomException e) {
                throw Exceptions.propagate(e);
            }
        });
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("JACK")
                .verifyError(CustomException.class);
    }

    private static String checkName(String name) throws CustomException {
        if (name.equals("Jill")) {
            throw new CustomException();
        }
        return name.toUpperCase();
    }

    protected static final class CustomException extends Exception {
        private static final long serialVersionUID = 0L;
    }

    /**
     * ********************************************************************
     *  flux that emits every second.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void fluxIntervalTake() {
        Flux<Long> interval = Flux.interval(Duration.ofSeconds(1))
                .log()
                .take(10);
        interval.subscribe(i -> log.info("Number: {}", i));
        TimeUnit.SECONDS.sleep(5);
        StepVerifier.withVirtualTime(() -> interval)
                .thenAwait(Duration.ofSeconds(5))
                .expectNextCount(4)
                .thenCancel()
                .verify();
    }

    /**
     * ********************************************************************
     *  flux that emits every day. Use of virtual time to simulate days.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void fluxIntervalVirtualTime() {
        StepVerifier.withVirtualTime(this::getTake)
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> getTake() {
        return Flux.interval(Duration.ofDays(1))
                .log()
                .take(10);
    }

    /**
     * ********************************************************************
     *  OnNext, OnError, OnComplete channels.
     * ********************************************************************
     */
    @Test
    void fluxError() {
        Flux flux = Flux.error(new RuntimeException("My Error"));
        flux.subscribe(
                onNext(),
                onError(),
                onComplete()
        );

        Flux.error(new RuntimeException("My Error"))
                .doOnSubscribe(s -> System.out.println("Subscribed!"))
                .doOnNext(p -> System.out.println("Next!"))
                .doOnComplete(() -> System.out.println("Completed!"))
                .doOnError((e) -> System.out.println("Error: " + e));

        StepVerifier.create(flux)
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(flux)
                .verifyError(RuntimeException.class);

        //Different approach
        Flux<Integer> fluxNumber = Flux.range(1, 5)
                .log()
                .map(i -> {
                    if (i == 4) {
                        throw new RuntimeException("Num Error!");
                    }
                    return i;
                });

        fluxNumber.subscribe(s -> {
                    log.info("Number: {}", s);
                },
                Throwable::printStackTrace,
                () -> {
                    log.info("Done!");
                });

        StepVerifier.create(fluxNumber)
                .expectNext(1, 2, 3)
                .expectError(RuntimeException.class)
                .verify();
    }

    private static Consumer<Object> onNext() {
        return o -> System.out.println("Received : " + o);
    }

    private static Consumer<Throwable> onError() {
        return e -> System.out.println("ERROR : " + e.getMessage());
    }

    private static Runnable onComplete() {
        return () -> System.out.println("Completed");
    }

    /**
     * ********************************************************************
     *  flux test - assertions
     * ********************************************************************
     */
    @Test
    void fluxStepVerify() {
        Flux flux = Flux.fromIterable(Arrays.asList("Jack", "Jill"));
        StepVerifier.create(flux)
                .expectNextMatches(user -> user.equals("Jack"))
                .assertNext(user -> Assertions.assertThat(user).isEqualTo("Jill"))
                .verifyComplete();

        //Wait for 2 elements.
        StepVerifier.create(flux)
                .expectNextCount(2)
                .verifyComplete();

        //Request 1 value at a time, get 2 values then cancel.
        Flux flux2 = Flux.fromIterable(Arrays.asList("Jack", "Jill", "Raj"));
        StepVerifier.create(flux2, 1)
                .expectNext("JACK")
                .thenRequest(1)
                .expectNext("JILL")
                .thenCancel();
    }

    /**
     * ********************************************************************
     *  then - will just replay the source terminal signal, resulting in a Mono<Void> to indicate that this never signals any onNext.
     *  thenEmpty - not only returns a Mono<Void>, but it takes a Mono<Void> as a parameter. It represents a concatenation of the source completion signal then the second, empty Mono completion signal. In other words, it completes when A then B have both completed sequentially, and doesn't emit data.
     *  thenMany - waits for the source to complete then plays all the signals from its Publisher<R> parameter, resulting in a Flux<R> that will "pause" until the source completes, then emit the many elements from the provided publisher before replaying its completion signal as well.
     * ********************************************************************
     */
    @Test
    void thenManyChainTest() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(ReactorDemo.deleteFromDb())
                .thenMany(ReactorDemo.saveToDb())
                .subscribe(System.out::println);
    }

    private static Flux<String> deleteFromDb() {
        return Flux.just("Deleted from db").log();
    }

    private static Flux<String> saveToDb() {
        return Flux.just("Saved to db").log();
    }

    private static Mono<Void> sendMail() {
        return Mono.empty();
    }

    @Test
    void thenEmpty() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(ReactorDemo.saveToDb())
                .thenEmpty(ReactorDemo.sendMail())
                .subscribe(System.out::println);
    }

    @Test
    void then() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(ReactorDemo.saveToDb())
                .then()
                .then(Mono.just("Ram"))
                .thenReturn("Done!")
                .subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  collectList
     * ********************************************************************
     */
    @Test
    void collectListTest() {
        Flux<String> flux = Flux.just(
                "yellow:banana",
                "red:apple");
        //Convert flux to list and iterate over it.
        flux.collectList()
                .flatMap(e -> {
                    e.forEach(System.out::println);
                    return Mono.empty();
                })
                .subscribe();

        flux.collectSortedList()
                .flatMap(e -> {
                    e.forEach(System.out::println);
                    return Mono.empty();
                })
                .subscribe();

        //Dont use infinite flux, will never return.
        //Flux.interval(Duration.ofMillis(1000)).collectList().subscribe();

        List<String> list3 = new ArrayList<>();
        flux.collectList().subscribe(list3::addAll);
        list3.forEach(System.out::println);
    }

    /**
     * ********************************************************************
     *  collectMap
     * ********************************************************************
     */
    @Test
    void collectMapTest() {
        Flux<String> flux = Flux.just(
                "yellow:banana",
                "red:apple");
        Map<String, String> map1 = new HashMap<>();
        flux.collectMap(
                item -> item.split(":")[0],
                item -> item.split(":")[1])
                .subscribe(map1::putAll);
        map1.forEach((key, value) -> System.out.println(key + " -> " + value));
    }

    /**
     * ********************************************************************
     *  collectMultimap
     * ********************************************************************
     */
    @Test
    void collectMultimapTest() {
        Flux<String> flux = Flux.just(
                "yellow:banana",
                "red:grapes",
                "red:apple");
        Map<String, Collection<String>> map1 = new HashMap<>();
        flux.collectMultimap(
                item -> item.split(":")[0],
                item -> item.split(":")[1])
                .subscribe(map1::putAll);
        map1.forEach((key, value) -> System.out.println(key + " -> " + value));
    }

    /**
     * ********************************************************************
     *  transform
     * ********************************************************************
     */
    @Test
    void transformTest() {
        //Function defines input and output
        Function<Flux<String>, Flux<String>> filterMap = name -> name.map(String::toUpperCase);
        Flux.fromIterable(List.of("Jack", "Joe", "Jill"))
                .transform(filterMap)
                .filter(s -> s.length() > 3)
                .log()
                .subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  merge - available for flux, not available for mono
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void mergeTest() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");
        //Eager will not wait till first flux finishes.
        Flux<String> flux = Flux.merge(flux1, flux2)
                .log();
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  mergeSequential - subscribe at same time, result merged in sequence.
     *  concat          - subscribe not at same time, result merged in sequence.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void mergeSequentialTest() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux = Flux.mergeSequential(flux1, flux2, flux1)
                .log();
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "a", "b")
                .verifyComplete();
    }

    @Test
    void mergeDelayTest() {
        Flux<String> flux1 = Flux.just("a", "b").map(s -> {
            if (s.equals("b")) {
                throw new IllegalArgumentException("error!");
            }
            return s;
        }).doOnError(e -> log.error("Error: {}", e));

        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux = Flux.mergeDelayError(1, flux1, flux2, flux1)
                .log();
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("a", "c", "d", "a")
                .expectError()
                .verify();
    }

    /**
     * ********************************************************************
     *  mergeWith - works with mono and flux.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void mergeWithTest() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");
        //Eager will not wait till first flux finishes.
        Flux<String> flux = flux1.mergeWith(flux2)
                .log();
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .verifyComplete();

        Mono aMono = Mono.just("a");
        Mono bMono = Mono.just("b");
        Flux flux3 = aMono.mergeWith(bMono);
        flux3.subscribe(System.out::println);
        StepVerifier.create(flux3)
                .expectNext("a", "b")
                .verifyComplete();

    }

    /**
     * ********************************************************************
     *  concatWith - works with mono and flux.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void concatWithTest() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");
        //Lazy will wait till first flux finishes.
        Flux<String> flux = flux1.concatWith(flux2).log();
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .verifyComplete();

        Mono<String> aFlux = Mono.just("a");
        Mono<String> bFlux = Mono.just("b");
        Flux<String> stringFlux = aFlux.concatWith(bFlux);
        stringFlux.subscribe(System.out::println);
        StepVerifier.create(stringFlux)
                .expectNext("a", "b")
                .verifyComplete();

    }

    /**
     * ********************************************************************
     *  concat - Only for flux. Not available for mono
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void concatTest() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux3 = Flux.concat(flux1, flux2).log();

        StepVerifier.create(flux3)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .verifyComplete();

        Flux<String> flux4 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux5 = Flux.just("c", "d");
        //Lazy will wait till first flux finishes.
        Flux<String> flux6 = Flux.concat(flux1, flux2).log();

        StepVerifier.create(flux6)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .verifyComplete();
    }

    @Test
    void concatDelayErrorTest() {
        Flux<String> flux1 = Flux.just("a", "b").map(s -> {
            if (s.equals("b")) {
                throw new IllegalArgumentException("error!");
            }
            return s;
        });
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux = Flux.concatDelayError(flux1, flux2)
                .log();
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("a", "c", "d")
                .expectError()
                .verify();
    }

    /**
     * ********************************************************************
     *  zip - waits for both flux to emit one element. 2-8 flux can be zipped
     *  returns a tuple
     *  works only with flux
     * ********************************************************************
     */
    @Test
    void fluxZipTest() {
        Flux<String> flux1 = Flux.just("red", "yellow");
        Flux<String> flux2 = Flux.just("apple", "banana");
        Flux<String> flux3 = Flux.zip(flux1, flux2)
                .map(tuple -> {
                    return (tuple.getT1() + " " + tuple.getT2());
                });
        flux3.subscribe(System.out::println);
        StepVerifier.create(flux3)
                .expectNext("red apple")
                .expectNext("yellow banana")
                .verifyComplete();

        //No tuple, operation on what to do is defined.
        Flux<Integer> firstFlux = Flux.just(1, 2, 3);
        Flux<Integer> secondFlux = Flux.just(10, 20, 30, 40);
        //Define how the zip should happen
        Flux<Integer> zip = Flux.zip(firstFlux, secondFlux, (num1, num2) -> num1 + num2);
        StepVerifier
                .create(zip)
                .expectNext(11, 22, 33)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  zipWith - works with flux and mono
     * ********************************************************************
     */
    @Test
    void fluxZipWithTest() {
        List<String> words = Arrays.asList("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog");

        //Every word gets a number, returns a tuple
        Flux.fromIterable(words)
                .zipWith(Flux.range(1, words.size()))
                .subscribe(System.out::println);

        //Returns a single string.
        Flux.fromIterable(words)
                .zipWith(Flux.range(1, 100), (word, line) -> {
                    return line + ". " + word;
                })
                .subscribe(System.out::println);

        //Print distinct chars with number
        Flux.fromIterable(words)
                .flatMap(word -> Flux.fromArray(word.split("")))
                .distinct()
                .sort()
                .zipWith(Flux.range(1, 100), (word, line) -> {
                    return line + ". " + word;
                })
                .subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  mono first
     * ********************************************************************
     */
    @Test
    void monoFirst() {
        Mono<String> mono1 = Mono.just("Jack").delayElement(Duration.ofSeconds(1));
        Mono<String> mono2 = Mono.just("Jill");
        //Return the mono which returns its value faster
        Mono<String> mono3 = Mono.firstWithValue(mono1, mono2);
        mono3.subscribe(System.out::println);
        StepVerifier.create(mono3)
                .expectNext("Jill")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  buffer
     * ********************************************************************
     */
    @Test
    public void bufferGroup() {
        Flux<List<Integer>> buffer = Flux
                .range(1, 7)
                .buffer(2);
        StepVerifier
                .create(buffer)
                .expectNext(Arrays.asList(1, 2))
                .expectNext(Arrays.asList(3, 4))
                .expectNext(Arrays.asList(5, 6))
                .expectNext(Arrays.asList(7))
                .verifyComplete();

    }

    @Test
    @SneakyThrows
    public void tickClock() {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1)).map(tick -> "fast tick " + tick);
        Flux slowClock = Flux.interval(Duration.ofSeconds(2)).map(tick -> "slow tick " + tick);
        Flux.merge(fastClock, slowClock).subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @SneakyThrows
    public void tickMergeClock() {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1)).map(tick -> "fast tick " + tick);
        Flux slowClock = Flux.interval(Duration.ofSeconds(2)).map(tick -> "slow tick " + tick);
        Flux clock = Flux.merge(slowClock, fastClock);
        Flux feed = Flux.interval(Duration.ofSeconds(1)).map(tick -> LocalTime.now());
        clock.withLatestFrom(feed, (tick, time) -> tick + " " + time).subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(15);
    }

    @Test
    @SneakyThrows
    public void tickZipClock() {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1)).map(tick -> "fast tick " + tick);
        Flux slowClock = Flux.interval(Duration.ofSeconds(2)).map(tick -> "slow tick " + tick);
        fastClock.zipWith(slowClock, (tick, time) -> tick + " " + time).subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @SneakyThrows
    public void emitterTest() {
        MyFeed myFeed = new MyFeed();
        Flux feedFlux = Flux.create(emmiter -> {
            myFeed.register(new MyListener() {
                @Override
                public void priceTick(String msg) {
                    emmiter.next(msg);
                }

                @Override
                public void error(Throwable error) {
                    emmiter.error(error);
                }
            });
        }, FluxSink.OverflowStrategy.LATEST);
        feedFlux.subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(15);
        System.out.println("Sending message!");
        for (int i = 0; i < 10; i++) {
            myFeed.sendMessage("HELLO_" + i);
        }

    }

    @Test
    void monoCancelSubscription() {
        Mono<String> helloMono = Mono.just("Jack")
                .log()
                .map(String::toUpperCase);
        helloMono.subscribe(s -> {
                    log.info("Got: {}", s);
                },
                Throwable::printStackTrace,
                () -> log.info("Finished"),
                Subscription::cancel
        );
        StepVerifier.create(helloMono)
                .expectNext("Jack".toUpperCase())
                .verifyComplete();
    }

    @Test
    void monoCompleteSubscriptionRequestBounded() {
        Mono<String> helloMono = Mono.just("Jack")
                .log()
                .map(String::toUpperCase);
        helloMono.subscribe(s -> {
                    log.info("Got: {}", s);
                },
                Throwable::printStackTrace,
                () -> log.info("Finished"),
                subscription -> {
                    subscription.request(5);
                }
        );

        StepVerifier.create(helloMono)
                .expectNext("Jack".toUpperCase())
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  doOn operators
     * ********************************************************************
     */
    @Test
    void doOnNextChain() {
        Mono<Object> helloMono = Mono.just("Jack")
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(s -> log.info("Subscribed!"))
                .doOnRequest(s -> log.info("Requested!"))
                .doOnNext(s -> log.info("Value: {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Value: {}", s)) //Will not be executed.
                .doOnSuccess(s -> log.info("Do on success {}", s))
                .doFinally(s -> log.info("Do on finally {}", s));
        helloMono.subscribe(s -> {
                    log.info("Got: {}", s);
                },
                Throwable::printStackTrace,
                () -> log.info("Finished"),
                Subscription::cancel
        );
        StepVerifier.create(helloMono)
                .verifyComplete();
    }

    @Test
    void fluxBackPressure() {
        Flux<Integer> fluxNumber = Flux.range(1, 5).log();

        //Fetches 2 at a time.
        fluxNumber.subscribe(new BaseSubscriber<>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        StepVerifier.create(fluxNumber)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

    }

    @Test
    void fluxBackPressureLimitRate() {
        Flux<Integer> fluxNumber = Flux.range(1, 5).log().limitRate(3);
        StepVerifier.create(fluxNumber)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    @SneakyThrows
    void connectableFlux() {
        //Hot Flux.
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();
        /*
                connectableFlux.connect();
                log.info("Sleeping!");
                TimeUnit.MILLISECONDS.sleep(300);
                connectableFlux.subscribe(i -> {
                    log.info("Sub1 Number: {}", i);
                });
                TimeUnit.MILLISECONDS.sleep(200);
                connectableFlux.subscribe(i -> {
                    log.info("Sub2 Number: {}", i);
                });
        */
        StepVerifier.create(connectableFlux)
                .then(connectableFlux::connect)
                .thenConsumeWhile(i -> i <= 5)
                .expectNext(6, 7, 8, 9, 10)
                .expectComplete()
                .verify();
    }

    @Test
    @SneakyThrows
    void connectableAutoFlux() {
        //Hot Flux.
        Flux<Integer> connectableFlux = Flux.range(1, 5)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(2);

        StepVerifier.create(connectableFlux)
                .then(connectableFlux::subscribe)
                .expectNext(1, 2, 3, 4, 5)
                .expectComplete()
                .verify();
    }

    @Test
    void subscribeOn() {
        Flux numbFlux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Map1 Num: {}, Thread: {}", i, Thread.currentThread().getName());
                    return i;
                }).subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map2 Num: {}, Thread: {}", i, Thread.currentThread().getName());
                    return i;
                });
        numbFlux.subscribe();
    }

    @Test
    void publishOn() {
        Flux numbFlux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Map1 Num: {}, Thread: {}", i, Thread.currentThread().getName());
                    return i;
                }).publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map2 Num: {}, Thread: {}", i, Thread.currentThread().getName());
                    return i;
                });
        numbFlux.subscribe();
    }

    @Test
    @SneakyThrows
    void readFile() {
        Mono<List<String>> listMono = Mono.fromCallable(() -> Files.readAllLines(Path.of("src/main/resources/file.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        listMono.subscribe(l -> log.info("Line: {}", l.size()));
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @SneakyThrows
    void switchTest() {
        Flux<Object> flux = emptyFlux()
                .switchIfEmpty(Flux.just("No empty!"))
                .log();
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("No empty!")
                .expectComplete()
                .verify();

        getHello().map(e -> {
            return e.get().toUpperCase();
        }).switchIfEmpty(Mono.error(new Throwable("error")))
                .subscribe(System.out::println);
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

    private Mono<Optional<String>> getHello() {
        return Mono.just(Optional.of("hello"));
    }

    @Test
    @SneakyThrows
    void deferTest() {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> deferJust = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        just.subscribe(l -> log.info("Time: {}", l));
        TimeUnit.SECONDS.sleep(2);
        just.subscribe(l -> log.info("Time: {}", l));

        deferJust.subscribe(l -> log.info("Time: {}", l));
        TimeUnit.SECONDS.sleep(2);
        deferJust.subscribe(l -> log.info("Time: {}", l));

    }

    @Test
    void combineLatestTest() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux = Flux.combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
                .log();
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("BC", "BD")
                .verifyComplete();
    }


    @Test
    public void monoSupplier() {
        Supplier<String> stringSupplier = () -> getName();
        Mono<String> mono = Mono.fromSupplier(stringSupplier);
        mono.subscribe(System.out::println);
    }

    @Test
    public void monoCallable() {
        Callable<String> stringCallable = () -> getName();
        Mono<String> mono = Mono.fromCallable(stringCallable);
        mono.subscribe(System.out::println);
    }

    @Test
    public void monoRunnable() {
        Runnable stringCallable = () -> getName();
        Mono<Object> mono = Mono.fromRunnable(stringCallable);
        mono.subscribe(System.out::println);
    }

    @Test
    public void monoSubscribeOn() {
        String name = getMonoName().subscribeOn(Schedulers.boundedElastic())
                .block();
        System.out.println(name);
    }

    private String getName() {
        return "John";
    }

    private Mono<String> getMonoName() {
        return Mono.fromSupplier(() -> {
            return "John";
        }).map(String::toUpperCase);
    }

}

class MyFeed {

    List<MyListener> listeners = new ArrayList<>();

    public void register(MyListener listener) {
        listeners.add(listener);
    }

    public void sendMessage(String msg) {
        listeners.forEach(e -> {
            e.priceTick(msg);
        });
    }
}

interface MyListener {
    void priceTick(String msg);

    void error(Throwable error);
}
