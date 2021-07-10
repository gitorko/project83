package com.demo.project83;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
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
     *     mono and subscribe.
     * ********************************************************************
     */
    @Test
    void monoHello() {
        Mono<String> mono1 = Mono.justOrEmpty("Jack");
        mono1.subscribe(System.out::println);
        StepVerifier.create(mono1)
                .expectNext("Jack")
                .verifyComplete();

        //Reactive Streams do not accept null values
        Mono<String> mono2 = Mono.justOrEmpty(null);
        mono2.subscribe(System.out::println);
        StepVerifier.create(mono2)
                .verifyComplete();

        //Default value if empty
        Mono<String> mono3 = mono2.defaultIfEmpty("Jill");
        mono3.subscribe(System.out::println);
        StepVerifier.create(mono3)
                .expectNext("Jill")
                .verifyComplete();

        Mono<String> helloMono = Mono.just("Jack").log();
        helloMono.subscribe(s -> {
            log.info("Got: {}", s);
        });
        StepVerifier.create(helloMono)
                .expectNext("Jack")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *     flux and subscribe.
     * ********************************************************************
     */
    @Test
    void fluxHello() {
        Flux flux = Flux.just("Hello", "World");
        flux.subscribe(System.out::println);

        StepVerifier.create(flux)
                .expectNext("Hello", "World")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *     flux sleep
     * ********************************************************************
     */
    @Test
    void fluxSleep() {
        Flux flux = Flux.just("Hello", "World").map(e -> {
            log.info("Received: {}", e);
            //Very bad idea to do Thread.sleep or any blocking call.
            //Instead use delayElements.
            return e;
        }).delayElements(Duration.ofSeconds(1));
        flux.subscribe(System.out::println);

        StepVerifier.create(flux)
                .expectNext("Hello", "World")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *     flux and subscribe.
     * ********************************************************************
     */
    @Test
    void fluxFilter() {
        Flux flux = Flux.just(1, 2, 3, 4, 5).filter(i -> i % 2 == 0);
        flux.subscribe(System.out::println);

        StepVerifier.create(flux)
                .expectNext(2, 4)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *    flux array
     * ********************************************************************
     */
    @Test
    public void fluxArray() {
        Integer[] arr = {2, 5, 7, 8};
        Flux.fromArray(arr)
                .subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *    flux list
     * ********************************************************************
     */
    @Test
    void fluxArrayList() {
        List<String> fruitsList = Arrays.asList("apple", "oranges", "grapes");
        Flux<String> fruits = Flux.fromIterable(fruitsList);
        StepVerifier.create(fruits)
                .expectNext("apple", "oranges", "grapes")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *    flux stream
     * ********************************************************************
     */
    @Test
    public void fluxStream() {
        Stream<Integer> stream = List.of(1, 2, 3, 4, 5).stream();
        Flux.fromStream(() -> stream).subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *    flux range
     * ********************************************************************
     */
    @Test
    public void fluxRange() {
        Flux.range(3, 10)
                .log()
                .map(i -> i + 100)
                .log()
                .subscribe(System.out::println);

        Flux<Integer> numbers = Flux.range(1, 5).log();
        StepVerifier.create(numbers)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

        numbers.subscribe(s -> {
            log.info("number: {}", s);
        });
    }

    /**
     * ********************************************************************
     *     flux flatMapMany
     * ********************************************************************
     */
    @Test
    void flatMapMany() {
        //Used to convert mono to flux
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
        Mono<List<Integer>> just = Mono.just(Arrays.asList(1, 2, 3));
        Flux<Integer> integerFlux = just.flatMapMany(it -> Flux.fromIterable(it));
        integerFlux.subscribe(System.out::println);
        StepVerifier
                .create(integerFlux)
                .expectNext(1, 2, 3)
                .verifyComplete();

        Flux<Integer> integerFlux2 = just.flatMapIterable(list -> list)
                .log();
        integerFlux2.subscribe(System.out::println);
        StepVerifier
                .create(integerFlux2)
                .expectNext(1, 2, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *     map
     * ********************************************************************
     */
    @Test
    public void fluxMap() {

        Flux<String> fluxFromJust = Flux.just("Jack", "Ram").log();
        Flux<Integer> filter = fluxFromJust.map(i -> i.length());
        StepVerifier
                .create(filter)
                .expectNext(4, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *     flatMap
     *     flatMap should be used for non-blocking operations, or in short anything which returns back Mono,Flux.
     *     map should be used when you want to do the transformation of an object /data in fixed time. The operations which are done synchronously.
     * ********************************************************************
     */
    @Test
    void flatMap() {
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

    @Test
    public void fluxFlatMap() {

        Flux<Integer> fluxFromJust = Flux.range(1, 3).log();
        Flux<Integer> integerFlux = fluxFromJust
                .flatMap(i -> getSomeFlux(i));

        StepVerifier
                .create(integerFlux)
                .expectNextCount(30)
                .verifyComplete();
    }

    private Flux<Integer> getSomeFlux(Integer i) {
        return Flux.range(i, 10);
    }

    /**
     * ********************************************************************
     *     startWith
     * ********************************************************************
     */
    @Test
    public void startWith() {
        Flux<Integer> just = Flux.range(1, 3);
        Flux<Integer> integerFlux = just.startWith(0);
        StepVerifier.create(integerFlux)
                .expectNext(0, 1, 2, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *     Tuple
     * ********************************************************************
     */
    @Test
    void fluxIndex() {
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
     *    takeWhile & skipWhile
     * ********************************************************************
     */
    @Test
    void takeWhile() {
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
     *    flux to mono list
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
     *    mono error
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
     *     Flux that emits every second.
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
     *     Flux that emits every day. Use of virtual time to simulate days.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void fluxIntervalVirtualTime() {
        Flux<Long> interval = getTake();
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
     *     OnNext, OnError, OnComplete channels.
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
     *     Flux step verify
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
     * then will just replay the source terminal signal, resulting in a Mono<Void> to indicate that this never signals any onNext.
     * thenEmpty not only returns a Mono<Void>, but it takes a Mono<Void> as a parameter. It represents a concatenation of the source completion signal then the second, empty Mono completion signal. In other words, it completes when A then B have both completed sequentially, and doesn't emit data.
     * thenMany waits for the source to complete then plays all the signals from its Publisher<R> parameter, resulting in a Flux<R> that will "pause" until the source completes, then emit the many elements from the provided publisher before replaying its completion signal as well.
     */
    @Test
    void thenMany() {

    }

    @Test
    void thenEmpty() {

    }

    @Test
    void then() {

    }

    /**
     * ********************************************************************
     *     Mono & Flux Transform
     * ********************************************************************
     */
    @Test
    void monoTransform() {
        Mono<String> mono = Mono.just("Jack").map(u -> u.toUpperCase());
        mono.subscribe(System.out::println);

        StepVerifier.create(mono)
                .expectSubscription()
                .expectNext("JACK")
                .verifyComplete();

        Flux<String> flux = Flux.just("Jack", "Jill").map(u -> u.toUpperCase());
        flux.subscribe(System.out::println);

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("JACK")
                .expectNext("JILL")
                .verifyComplete();

        Flux flux2 = Flux.just("Jack", "Jill").flatMap(ReactorDemo::capitalize);
        flux2.subscribe(System.out::println);

        StepVerifier.create(flux2)
                .expectSubscription()
                .expectNext("JACK")
                .expectNext("JILL")
                .verifyComplete();
    }

    private static Mono<String> capitalize(String user) {
        return Mono.just(user.toUpperCase());
    }

    /**
     * ********************************************************************
     *    mergeWith
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
    }

    /**
     * ********************************************************************
     *    concatWith
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
    }

    /**
     * ********************************************************************
     *    concat
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

    /**
     * ********************************************************************
     *    zip
     * ********************************************************************
     */
    @Test
    void fluxZip() {
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
        Flux<Integer> zip = Flux.zip(firstFlux, secondFlux, (num1, num2) -> num1 + num2);
        StepVerifier
                .create(zip)
                .expectNext(11, 22, 33)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *    zipWith
     * ********************************************************************
     */
    @Test
    void fluxZipWith() {
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
     *    mono first
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

    @Test
    void doOnNextChain() {
        Mono<Object> helloMono = Mono.just("Jack")
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(s -> {
                    log.info("Subscribed!");
                })
                .doOnRequest(s -> log.info("Requested!"))
                .doOnNext(s -> log.info("Value: {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Value: {}", s)) //Will not be executed.
                .doOnSuccess(s -> log.info("Do on success {}", s));

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
        Mono<List<String>> listMono = Mono.fromCallable(() -> Files.readAllLines(Path.of("src/main/resources/file" +
                ".txt")))
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
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
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
    void concatDelayTest() {
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

    @Test
    public void emptyMonoTest() {
        getHello().map(e -> {
            return e.get().toUpperCase();
        }).switchIfEmpty(Mono.error(new Throwable("error")))
                .subscribe(System.out::println);
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

    private Mono<Optional<String>> getHello() {
        return Mono.just(Optional.of("hello"));
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
