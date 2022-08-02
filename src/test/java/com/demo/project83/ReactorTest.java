package com.demo.project83;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.tools.agent.ReactorDebugAgent;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

/**
 * Reactive Streams Specification
 *
 * Publisher (Mono/Flux)
 *   - subscribe (data source, db, remote service)
 * Subscriber
 *   - onSubscribe
 *   - onNext
 *   - onError
 *   - onComplete
 * Subscription
 *   - request
 *   - cancel
 * Processor - Publisher + Subscriber
 *
 * Spring reactor is a Push + Pull data flow model
 */
@Slf4j
public class ReactorTest {

    /**
     * ********************************************************************
     *  mono
     * ********************************************************************
     */
    @Test
    void monoTest() {
        //justOrEmpty
        Mono<String> mono1 = Mono.justOrEmpty("apple");
        mono1.subscribe(System.out::println);
        StepVerifier.create(mono1)
                .expectNext("apple")
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
        Mono<String> mono4 = Mono.just("apple").log();
        mono4.subscribe(s -> {
            log.info("Got: {}", s);
        });
        StepVerifier.create(mono4)
                .expectNext("apple")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux
     * ********************************************************************
     */
    @Test
    void fluxTest() {
        Flux flux = Flux.just("apple", "orange");
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux - Avoid blocking calls that will hold thread
     * ********************************************************************
     */
    @Test
    void fluxSleepTest() {
        Flux flux = Flux.just("apple", "orange").map(e -> {
            log.info("Received: {}", e);
            //Bad idea to do Thread.sleep or any blocking call.
            //Instead use delayElements.
            return e;
        }).delayElements(Duration.ofSeconds(1));
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  block
     * ********************************************************************
     */
    @Test
    public void badTest() {
        //Never use .block() as it blocks the thread.
        String name = Mono.just("Jack")
                .block();
        System.out.println(name);
    }

    /**
     * ********************************************************************
     *  filter - filter out elements that dont meet condition
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
     *  flux from array, list, stream
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
        StepVerifier.create(flux2)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

        Flux<Integer> flux3 = Flux.fromIterable(List.of(1, 2, 3, 4, 5));
        StepVerifier.create(flux3)
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
     *  map - synchronous by nature
     * ********************************************************************
     */
    @Test
    public void fluxMapTest() {
        Flux<Integer> flux1 = Flux.just("apple", "orange")
                .log()
                .map(i -> i.length());
        StepVerifier
                .create(flux1)
                .expectNext(5, 6)
                .verifyComplete();

        Flux<Integer> flux2 = Flux.range(3, 2)
                .map(i -> i + 100);
        flux2.subscribe(System.out::println);
        StepVerifier.create(flux2)
                .expectNext(103, 104)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flatMap - transform object 1-1 or 1-N in asyncronous fashion, returns back Mono/Flux.
     *  map - transform an object 1-1 in fixed time in synchronous fashion
     * ********************************************************************
     */
    @Test
    void flatMapTest() {
        Flux flux1 = Flux.just("apple", "orange")
                .flatMap(ReactorTest::capitalizeReactive);
        flux1.subscribe(System.out::println);
        StepVerifier.create(flux1)
                .expectSubscription()
                .expectNext("APPLE")
                .expectNext("ORANGE")
                .verifyComplete();

        //capitalize will happen in blocking fashion. If this function takes long or does I/O then it will be blocking
        //Use map only when there is no IO involved in the function
        Flux flux2 = Flux.just("apple", "orange")
                .map(ReactorTest::capitalize);
        flux2.subscribe(System.out::println);
        StepVerifier.create(flux2)
                .expectSubscription()
                .expectNext("APPLE")
                .expectNext("ORANGE")
                .verifyComplete();
    }

    private static Mono<String> capitalizeReactive(String user) {
        return Mono.just(user.toUpperCase());
    }

    private static String capitalize(String user) {
        return user.toUpperCase();
    }

    /**
     * ********************************************************************
     *  flatMap - object modification
     * ********************************************************************
     */
    @Test
    void objectModificationTest() {
        //Modification of object in chain - done via flatMap
        //Ideally create a new object instead of modifying the existing object.
        Mono<String> mono1 = Mono.just("Apple")
                .flatMap(ReactorTest::appendGreet);
        StepVerifier.create(mono1)
                .expectNext("Hello Apple")
                .verifyComplete();

        //Modification of object in chain - done via zipWith
        //The 2nd argument for zipWith is a combinator function that determines how the 2 mono are zipped
        Mono<String> mono2 = Mono.just("Apple")
                .zipWith(Mono.just("Hello "), ReactorTest::getGreet);
        StepVerifier.create(mono2)
                .expectNext("Hello Apple")
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
     *  flatMap - Find all distinct chars in the list of names, convert to uppercase
     * ********************************************************************
     */
    @Test
    void flatMapTest2() {
        Flux<String> flux = Flux.fromIterable(List.of("Jack", "Joe", "Jill"))
                .map(String::toUpperCase)
                .flatMap(s -> splitString(s))
                .distinct();
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("J", "A", "C", "K", "O", "E", "I", "L")
                .verifyComplete();

    }

    private Flux<String> splitString(String name) {
        return Flux.fromArray(name.split(""));
    }

    /**
     * ********************************************************************
     *  concatMap - Same as flatMap but order is preserved, concatMap takes more time but ordering is preserved.
     *  flatMap - Takes less time but ordering is lost.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void concatMapTest() {
        Flux<String> flux = Flux.fromIterable(List.of("Jack", "Joe", "Jill"))
                .map(String::toUpperCase)
                .concatMap(s -> splitStringAsync(s))
                .distinct();

        StepVerifier.create(flux)
                .expectNext("J", "A", "C", "K", "O", "E", "I", "L")
                .verifyComplete();
    }

    private Flux<String> splitStringAsync(String name) {
        return Flux.fromArray(name.split(""))
                .delayElements(Duration.ofMillis(new Random().nextInt(1000)));
    }

    /**
     * ********************************************************************
     *  flatMapMany - similar to flatMap, but function returns flux
     * ********************************************************************
     */
    @Test
    void flatMapManyTest() {

        Flux flux1 = Mono.just("Jack")
                .flatMapMany(ReactorTest::capitalizeSplit);
        flux1.subscribe(System.out::println);
        StepVerifier.create(flux1)
                .expectSubscription()
                .expectNext("J", "A", "C", "K")
                .verifyComplete();

        Flux<String> flux2 = Mono.just("the quick brown fox jumps over the lazy dog")
                .flatMapMany(ReactorTest::capitalizeSplit)
                .distinct()
                .sort();
        flux2.subscribe(System.out::println);
        //26 letters in the alphabet
        StepVerifier.create(flux2)
                .expectNext("A", "B", "C")
                .expectComplete();

        Mono<List<Integer>> mono = Mono.just(Arrays.asList(1, 2, 3));
        Flux<Integer> flux3 = mono.flatMapMany(it -> Flux.fromIterable(it));
        flux3.subscribe(System.out::println);
        StepVerifier
                .create(flux3)
                .expectNext(1, 2, 3)
                .verifyComplete();
    }

    private static Flux<String> capitalizeSplit(String user) {
        return Flux.fromArray(user.toUpperCase().split(""));
    }

    /**
     * ********************************************************************
     *  flatMapIterable - convert mono of list to flux
     * ********************************************************************
     */
    @Test
    void flatMapIterableTest() {
        //Converts Mono of list to Flux.
        Mono<List<Integer>> mono = Mono.just(Arrays.asList(1, 2, 3));
        Flux<Integer> flux1 = mono.flatMapIterable(list -> list);
        flux1.subscribe(System.out::println);
        StepVerifier
                .create(flux1)
                .expectNext(1, 2, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  transform - accepts a Function functional interface. Used when similar transform is used in many places
     *  input is flux/mono
     *  output is flux/mono
     *  takes a flux/mono and returns a flux/mono
     * ********************************************************************
     */
    @Test
    void transformTest() {
        //Function defines input and output
        Function<Flux<String>, Flux<String>> filterMap = name -> name.map(String::toUpperCase);
        Flux<String> flux1 = Flux.fromIterable(List.of("Jack", "Joe", "Jill"))
                .transform(filterMap)
                .filter(s -> s.length() > 3);
        flux1.subscribe(System.out::println);
        StepVerifier
                .create(flux1)
                .expectNext("JACK", "JILL")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  switchIfEmpty - similar to defaultIfEmpty but return flux/mono
     *  defaultIfEmpty - return a fixed value.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void switchTest() {
        Flux<Object> flux1 = emptyFlux()
                .switchIfEmpty(Flux.just("empty!"))
                .log();
        StepVerifier.create(flux1)
                .expectSubscription()
                .expectNext("empty!")
                .expectComplete()
                .verify();

        Flux<Object> flux2 = emptyFlux()
                .defaultIfEmpty("empty!")
                .log();
        StepVerifier.create(flux2)
                .expectSubscription()
                .expectNext("empty!")
                .expectComplete()
                .verify();


    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

    /**
     * ********************************************************************
     *  switchIfEmpty with Optional
     * ********************************************************************
     */
    @Test
    public void switchOptionalTest() {

        Mono<String> mono1 = getHello1().map(e -> {
            return e.get().toUpperCase();
        }).switchIfEmpty(Mono.just("empty!"));
        StepVerifier.create(mono1)
                .expectNext("HELLO")
                .expectComplete()
                .verify();

        Mono<String> mono2 = getHello2().map(e -> {
            return e.get().toUpperCase();
        }).switchIfEmpty(Mono.just("empty!"));
        StepVerifier.create(mono2)
                .expectNext("empty!")
                .expectComplete()
                .verify();
    }

    private Mono<Optional<String>> getHello1() {
        return Mono.just(Optional.of("hello"));
    }

    //Optional evaluated to object or empty
    private Mono<Optional<String>> getHello2() {
        return Mono.justOrEmpty(Optional.empty());
    }

    /**
     * ********************************************************************
     *  switchIfEmpty - Used as if-else block
     * ********************************************************************
     */
    @Test
    void switchIfElseTest() {
        final String name = "Jack";
        Mono<String> mono = Mono.just(name)
                .flatMap(this::wishGoodMorning)
                .switchIfEmpty(Mono.defer(() -> this.wishGoodNight(name)));

        StepVerifier.create(mono)
                .expectNext("Good Night Jack")
                .verifyComplete();
    }

    private Mono<String> wishGoodMorning(String name) {
        log.info("wishGoodMorning {}", name);
        if (name.equals("Jack")) {
            return Mono.empty();
        } else {
            return Mono.just("Good Morning " + name);
        }
    }

    private Mono<String> wishGoodNight(String name) {
        log.info("wishGoodNight {}", name);
        return Mono.just("Good Night " + name);
    }

    /**
     * ********************************************************************
     *  intersect with filterWhen - compare 2 flux for common elements
     * ********************************************************************
     */
    @Test
    void fluxIntersectCommonTest() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana").log();
        //Without cache on flux2 it will subscribe many times.
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple").log().cache();

        Flux<String> commonFlux = flux1.filterWhen(f -> ReactorTest.checkList1(flux2, f));
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    private static Mono<Boolean> checkList1(Flux<String> flux, String fruit) {
        //toStream will block so should be avoided. Look at ReactorObjectTest for better approach.
        return Mono.just(flux.toStream().anyMatch(e -> e.equals(fruit)));
    }

    /**
     * ********************************************************************
     *  intersect with filter - compare 2 flux for common elements
     * ********************************************************************
     */
    @Test
    void fluxIntersectCommon2Test() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana").log();
        //Without cache on flux2 it will subscribe many times.
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple").log().cache();
        Flux<String> commonFlux = flux1.filter(f -> {
            //toStream will block so should be avoided. Look at ReactorObjectTest for better approach.
            return flux2.toStream().anyMatch(e -> e.equals(f));
        });
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  intersect with filterWhen - compare 2 flux for diff
     * ********************************************************************
     */
    @Test
    void fluxIntersectDiffTest() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana").log();
        //Without cache on flux2 it will subscribe many times.
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple").log().cache();

        Flux<String> diffFlux = flux1.filterWhen(f -> ReactorTest.checkList2(flux2, f));
        diffFlux.subscribe(System.out::println);
        StepVerifier.create(diffFlux)
                .expectNext("banana")
                .verifyComplete();
    }

    private static Mono<Boolean> checkList2(Flux<String> flux, String fruit) {
        //toStream will block so should be avoided. Look at ReactorObjectTest for better approach.
        return Mono.just(flux.toStream().anyMatch(e -> e.equals(fruit))).map(hasElement -> !hasElement);
    }

    /**
     * ********************************************************************
     *  intersect with filter - compare 2 flux for diff
     * ********************************************************************
     */
    @Test
    void fluxIntersectDiff2Test() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana").log();
        //Without cache on flux2 it will subscribe many times.
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple").log().cache();
        Flux<String> commonFlux = flux1.filter(f -> {
            //toStream will block so should be avoided. Look at ReactorObjectTest for better approach.
            return !flux2.toStream().anyMatch(e -> e.equals(f));
        });
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("banana")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  startWith - add new element to flux.
     * ********************************************************************
     */
    @Test
    public void startWithTest() {
        Flux<Integer> flux = Flux.range(1, 3);
        Flux<Integer> integerFlux = flux.startWith(0);
        StepVerifier.create(integerFlux)
                .expectNext(0, 1, 2, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  index
     * ********************************************************************
     */
    @Test
    void fluxIndexTest() {
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
        Flux<Integer> numbersFlux = Flux.range(1, 10);
        Flux<Integer> takeWhile = numbersFlux.takeWhile(i -> i <= 5);
        StepVerifier
                .create(takeWhile)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

        Flux<Integer> skipWhile = numbersFlux.skipWhile(i -> i <= 5);
        StepVerifier
                .create(skipWhile)
                .expectNext(6, 7, 8, 9, 10)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  collectList & collectSortedList- flux to mono of list
     * ********************************************************************
     */
    @Test
    void collectListTest() {
        Flux<String> flux = Flux.just("apple", "orange");
        Mono<List<String>> mono = flux.collectList();
        mono.subscribe(System.out::println);
        StepVerifier.create(mono)
                .expectNext(Arrays.asList("apple", "orange"))
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
     *  collectList
     * ********************************************************************
     */
    @Test
    void collectListTest2() {
        Mono<List<String>> monoList1 = Flux.just("banana", "apple")
                .collectList();
        StepVerifier.create(monoList1)
                .expectNext(Arrays.asList("banana", "apple"))
                .verifyComplete();

        Mono<List<String>> monoList2 = Flux.just("banana", "apple")
                .collectSortedList();
        StepVerifier.create(monoList2)
                .expectNext(Arrays.asList("apple", "banana"))
                .verifyComplete();

        //Dont use infinite flux, will never return.
        //Flux.interval(Duration.ofMillis(1000)).collectList().subscribe();

        List<String> list3 = new ArrayList<>();
        monoList1.subscribe(list3::addAll);
        list3.forEach(System.out::println);
    }

    /**
     * ********************************************************************
     *  collectMap
     * ********************************************************************
     */
    @Test
    void collectMapTest() {
        Mono<Map<Object, Object>> flux = Flux.just(
                "yellow:banana",
                "red:apple").collectMap(item -> item.split(":")[0], item -> item.split(":")[1]);

        Map<Object, Object> map1 = new HashMap<>();
        flux.subscribe(map1::putAll);
        map1.forEach((key, value) -> System.out.println(key + " -> " + value));

        StepVerifier.create(flux)
                .expectNext(Map.of("yellow", "banana", "red", "apple"))
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  collectMultimap
     * ********************************************************************
     */
    @Test
    void collectMultimapTest() {
        Mono<Map<String, Collection<Object>>> flux = Flux.just("yellow:banana", "red:grapes", "red:apple", "yellow:pineapple")
                .collectMultimap(
                        item -> item.split(":")[0],
                        item -> item.split(":")[1]);
        Map<Object, Collection<Object>> map1 = new HashMap<>();
        flux.subscribe(map1::putAll);
        map1.forEach((key, value) -> System.out.println(key + " -> " + value));

        StepVerifier.create(flux)
                .expectNext(Map.of("red", List.of("grapes", "apple"), "yellow", List.of("banana", "pineapple")))
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  concat - subscribes to publishers in sequence, order guaranteed, static function
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void concatTest() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux3 = Flux.concat(flux1, flux2);

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
     *  concatWith - subscribes to publishers in sequence, order guaranteed, instance function
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void concatWithTest() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux3 = flux1.concatWith(flux2).log();
        StepVerifier.create(flux3)
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
     *  merge - subscribes to publishers eagerly, order not guaranteed, static function
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
     *  mergeWith - subscribes to publishers in eagerly, order not guaranteed, instance function
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void mergeWithTest() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");
        //Eager will not wait till first flux finishes.
        Flux<String> flux3 = flux1.mergeWith(flux2).log();
        StepVerifier.create(flux3)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .verifyComplete();

        Mono aMono = Mono.just("a");
        Mono bMono = Mono.just("b");
        Flux flux4 = aMono.mergeWith(bMono);
        StepVerifier.create(flux4)
                .expectNext("a", "b")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  mergeSequential - subscribes to publishers eagerly, result is sequential.
     *  concat          - subscribes to publishers in sequence, result is sequential.
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
     *  zip - subscribes to publishers in eagerly, waits for both flux to emit one element. 2-8 flux can be zipped, returns a tuple, Static function
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

        //Third argument is combinator lambda
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
     *  zipWith - subscribes to publishers in eagerly, waits for both flux to emit one element. 2-8 flux can be zipped, returns a tuple, Instance function
     * ********************************************************************
     */
    @Test
    void fluxZipWithTest() {
        Flux<String> flux1 = Flux.just("red", "yellow");
        Flux<String> flux2 = Flux.just("apple", "banana");
        Flux<String> flux3 = flux1.zipWith(flux2)
                .map(tuple -> {
                    return (tuple.getT1() + " " + tuple.getT2());
                });
        StepVerifier.create(flux3)
                .expectNext("red apple")
                .expectNext("yellow banana")
                .verifyComplete();

        Flux<String> flux4 = Flux.fromIterable(Arrays.asList("apple", "orange", "banana"))
                .zipWith(Flux.range(1, 5), (word, line) -> {
                    return line + ". " + word;
                });
        StepVerifier.create(flux4)
                .expectNext("1. apple")
                .expectNext("2. orange")
                .expectNext("3. banana")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  Error Recover Handling
     *  onErrorReturn - Return value on error
     * ********************************************************************
     */
    @Test
    void onErrorReturnTest() {
        Mono<Object> mono1 = Mono.error(new RuntimeException("My Error"))
                .onErrorReturn("Jack");
        StepVerifier.create(mono1)
                .expectNext("Jack")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  Error Recover Handling
     *  onErrorResume - Resume chain with new mono/flux.
     * ********************************************************************
     */
    @Test
    void onErrorResumeTest() {
        Mono<Object> mono1 = Mono.error(new RuntimeException("My Error"))
                .onErrorResume(e -> Mono.just("Jack"));
        StepVerifier.create(mono1)
                .expectNext("Jack")
                .verifyComplete();

        Mono<Object> mono2 = Mono.error(new RuntimeException("My Error"))
                .onErrorResume(s -> {
                    log.info("Inside on onErrorResume");
                    return Mono.just("Jack");
                })
                .log();
        StepVerifier.create(mono2)
                .expectNext("Jack")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  Error Recover Handling
     *  onErrorContinue - Continue chain even if error occurs
     * ********************************************************************
     */
    @Test
    void onErrorContinueTest() {
        Flux<String> flux1 =
                Flux.just("a", "b", "c")
                        .map(e -> {
                            if (e.equals("b"))
                                throw new RuntimeException("My Error!");
                            return e;
                        })
                        .concatWith(Mono.just("d"))
                        .onErrorContinue((ex, value) -> {
                            log.info("Exception: {}", ex);
                            log.info("value: {}", value);
                        })
                        .log();
        StepVerifier.create(flux1)
                .expectNext("a", "c", "d")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  Error - Action
     *  doOnError - log the error, Side-effect operator.
     * ********************************************************************
     */
    @Test
    void doOnErrorTest() {
        Mono<Object> mono1 = Mono.error(new RuntimeException("My Error"))
                .doOnError(e -> log.error("Error: {}", e.getMessage()))
                .log();
        StepVerifier.create(mono1)
                .expectError(RuntimeException.class)
                .verify();
    }

    /**
     * ********************************************************************
     *  Error - Action
     *  onErrorMap - Transform an error emitted
     * ********************************************************************
     */
    @Test
    void onErrorMapTest() {
        Flux flux1 = Flux.just("Jack", "Jill").map(u -> {
            if (u.equals("Jill")) {
                //always do throw here, never do return.
                throw new IllegalArgumentException("Not valid");
            }
            if (u.equals("Jack")) {
                throw new ClassCastException("Not valid");
            }
            return u;
        }).onErrorMap(IllegalArgumentException.class, e -> {
            log.info("Illegal Arg error");
            throw new RuntimeException("Illegal Arg error!");
        }).onErrorMap(ClassCastException.class, e -> {
            log.info("Class cast error");
            throw new RuntimeException("Class cast error!");
        });

        StepVerifier.create(flux1)
                .expectErrorMessage("Class cast error!")
                .verify();
    }

    /**
     * ********************************************************************
     *  retry
     * ********************************************************************
     */
    @Test
    void retryTest() {
        Mono<String> mono = Mono.just("Jack")
                .flatMap(this::twoAttemptFunction)
                .retry(3);
        StepVerifier.create(mono)
                .assertNext(e -> {
                    assertThat(e).isEqualTo("Hello Jack");
                })
                .verifyComplete();
    }

    AtomicLong attemptCounter1 = new AtomicLong();

    private Mono<String> twoAttemptFunction(String name) {
        Long attempt = attemptCounter1.getAndIncrement();
        log.info("attempt value: {}", attempt);
        if (attempt < 2) {
            throw new RuntimeException("FAILURE");
        }
        return Mono.just("Hello " + name);
    }

    /**
     * ********************************************************************
     *  retryWhen
     * ********************************************************************
     */
    @Test
    void retryWhenTest() {
        attemptCounter2 = new AtomicLong();
        RetryBackoffSpec retryFilter1 = Retry.backoff(3, Duration.ofSeconds(1))
                .filter(throwable -> throwable instanceof RuntimeException);

        Mono<String> mono1 = Mono.just("Jack")
                .flatMap(this::greetAfter2Failure)
                .retryWhen(retryFilter1);
        StepVerifier.create(mono1)
                .assertNext(e -> {
                    assertThat(e).isEqualTo("Hello Jack");
                })
                .verifyComplete();

        attemptCounter2 = new AtomicLong();
        RetryBackoffSpec retryFilter2 = Retry.fixedDelay(1, Duration.ofSeconds(1))
                .filter(throwable -> throwable instanceof RuntimeException)
                .onRetryExhaustedThrow(((retryBackoffSpec, retrySignal) ->
                        Exceptions.propagate(retrySignal.failure())
                ));
        Mono<String> mono2 = Mono.just("Jack")
                .flatMap(this::greetAfter2Failure)
                .retryWhen(retryFilter2);
        StepVerifier.create(mono2)
                .expectErrorMessage("FAILURE")
                .verify();
    }

    AtomicLong attemptCounter2;

    private Mono<String> greetAfter2Failure(String name) {
        Long attempt = attemptCounter2.getAndIncrement();
        log.info("attempt value: {}", attempt);
        if (attempt < 2) {
            throw new RuntimeException("FAILURE");
        }
        return Mono.just("Hello " + name);
    }

    /**
     * ********************************************************************
     *  repeat - repeat an operation n times.
     * ********************************************************************
     */
    @Test
    void repeatTest() {
        Mono<List<String>> flux = getDate()
                .repeat(5)
                .collectList();
        StepVerifier.create(flux)
                .assertNext(e -> {
                    assertThat(e.size()).isEqualTo(6);
                })
                .verifyComplete();
    }

    private Mono<String> getDate() {
        return Mono.just("Time " + new Date());
    }

    /**
     * ********************************************************************
     *  doOn - doOnSubscribe, doOnNext, doOnError, doFinally, doOnComplete
     * ********************************************************************
     */
    @Test
    void doOnTest1() {
        Flux<Integer> numFlux = Flux.range(1, 5)
                .log()
                .map(i -> {
                    if (i == 4) {
                        throw new RuntimeException("Num Error!");
                    }
                    return i;
                });
        numFlux.subscribe(s -> {
                    log.info("Number: {}", s);
                },
                Throwable::printStackTrace,
                () -> {
                    log.info("Done!");
                });
        StepVerifier.create(numFlux)
                .expectNext(1, 2, 3)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void doOnTest2() {
        Flux<Object> flux = Flux.error(new RuntimeException("My Error"))
                .doOnSubscribe(s -> System.out.println("Subscribed!"))
                .doOnNext(p -> System.out.println("Next!"))
                .doOnComplete(() -> System.out.println("Completed!"))
                .doFinally((e) -> System.out.println("Signal: " + e))
                .doOnError((e) -> System.out.println("Error: " + e));

        StepVerifier.create(flux)
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(flux)
                .verifyError(RuntimeException.class);
    }

    @Test
    void doOnTest3() {
        Flux flux = Flux.error(new RuntimeException("My Error"));
        flux.subscribe(
                onNext(),
                onError(),
                onComplete()
        );
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
     *  StepVerifier - assertNext, thenRequest, thenCancel, expectError, expectErrorMessage
     * ********************************************************************
     */
    @Test
    void fluxStepVerifyTest() {
        Flux flux = Flux.fromIterable(Arrays.asList("Jack", "Jill"));
        StepVerifier.create(flux)
                .expectNextMatches(user -> user.equals("Jack"))
                .assertNext(user -> assertThat(user).isEqualTo("Jill"))
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

        Mono<Object> mono1 = Mono.error(new RuntimeException("My Error"));
        StepVerifier.create(mono1)
                .expectError(RuntimeException.class)
                .verify();
        StepVerifier.create(mono1)
                .expectErrorMessage("My Error")
                .verify();
    }

    /**
     * ********************************************************************
     *  flux error propagate
     * ********************************************************************
     */
    @Test
    void errorPropagateTest() {
        Flux flux1 = Flux.just("Jack", "Jill").map(u -> {
            try {
                return ReactorTest.checkName(u);
            } catch (CustomException e) {
                throw Exceptions.propagate(e);
            }
        });
        flux1.subscribe(System.out::println);
        StepVerifier.create(flux1)
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
     *  subscribeOn - influences upstream (whole chain)
     * ********************************************************************
     */
    @Test
    void subscribeOnTest() {
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

    /**
     * ********************************************************************
     *  publishOn - influences downstream
     * ********************************************************************
     */
    @Test
    void publishOnTest() {
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

    /**
     * ********************************************************************
     *  fromSupplier - returns a value
     *  fromCallable - returns a value or exception, runs blocking function on different thread
     * ********************************************************************
     */
    @Test
    public void monoSupplierTest() {
        Supplier<String> stringSupplier = () -> getName();
        Mono<String> mono = Mono.fromSupplier(stringSupplier)
                .log();
        mono.subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  fromSupplier - returns a value
     *  fromCallable - returns a value or exception, runs blocking function on different thread
     * ********************************************************************
     */
    @Test
    public void monoCallableTest() {
        Callable<String> stringCallable = () -> getName();
        Mono<String> mono = Mono.fromCallable(stringCallable)
                .log()
                .subscribeOn(Schedulers.boundedElastic());
        mono.subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  fromCallable - read file may be blocking so we dont want to block main thread.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void readFileTest() {
        Mono<List<String>> listMono = Mono.fromCallable(() -> Files.readAllLines(Path.of("src/test/resources/file.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        listMono.subscribe(l -> log.info("Line: {}", l.size()));
        TimeUnit.SECONDS.sleep(5);

        StepVerifier.create(listMono)
                .expectSubscription()
                .thenConsumeWhile(l -> {
                    assertThat(l.isEmpty()).isFalse();
                    return true;
                })
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  fromRunnable - runs blocking function on different thread, but doesnt return value
     * ********************************************************************
     */
    @Test
    public void monoRunnableTest() {
        Runnable stringCallable = () -> getName();
        Mono<Object> mono = Mono.fromRunnable(stringCallable)
                .log()
                .subscribeOn(Schedulers.boundedElastic());
        mono.subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  ParallelFlux - Will complete in 1 sec even when 3 ops take 3 seconds in sequence
     * ********************************************************************
     */
    @Test
    void fluxParallelTest() {
        log.info("Cores: {}", Runtime.getRuntime().availableProcessors());
        ParallelFlux<String> flux1 = Flux.just("apple", "orange", "banana")
                .parallel()
                .runOn(Schedulers.parallel())
                .map(ReactorTest::capitalizeString)
                .log();
        StepVerifier.create(flux1)
                .expectNextCount(3)
                .verifyComplete();


        Flux<String> flux2 = Flux.just("apple", "orange", "banana")
                .flatMap(name -> {
                    return Mono.just(name)
                            .map(ReactorTest::capitalizeString)
                            .subscribeOn(Schedulers.parallel());
                })
                .log();
        StepVerifier.create(flux2)
                .expectNextCount(3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flatMap Parallelism - Will complete in 1 sec even when 3 ops take 3 seconds in sequence
     * ********************************************************************
     */
    @Test
    void fluxParallelWithFlatMapTest() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana")
                .flatMap(name -> {
                    return Mono.just(name)
                            .map(ReactorTest::capitalizeString)
                            .subscribeOn(Schedulers.parallel());
                })
                .log();
        StepVerifier.create(flux1)
                .expectNextCount(3)
                .verifyComplete();
    }

    @SneakyThrows
    private static String capitalizeString(String element) {
        log.info("Capitalizing: {}", element);
        TimeUnit.SECONDS.sleep(1);
        return element.toUpperCase();
    }

    /**
     * ********************************************************************
     *  flatMap - fire-forget jobs with subscribe, Will run async jobs
     * ********************************************************************
     */
    @SneakyThrows
    @Test
    void fireForgetTest() {
        CountDownLatch latch = new CountDownLatch(3);
        Flux<Object> flux1 = Flux.just("apple", "orange", "banana")
                .flatMap(fruit -> {
                    Mono.just(fruit)
                            .map(e -> ReactorTest.capitalizeStringLatch(e, latch))
                            .subscribeOn(Schedulers.parallel())
                            .subscribe();
                    return Mono.empty();
                })
                .log();
        StepVerifier.create(flux1)
                .verifyComplete();
        latch.await(5, TimeUnit.SECONDS);
    }

    @SneakyThrows
    private static String capitalizeStringLatch(String element, CountDownLatch latch) {
        log.info("Capitalizing: {}", element);
        TimeUnit.SECONDS.sleep(1);
        latch.countDown();
        return element.toUpperCase();
    }

    /**
     * ********************************************************************
     *  flatMapSequential - Maintains order but executes in parallel
     * ********************************************************************
     */
    @Test
    void flatMapSequentialTest() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana")
                .flatMapSequential(name -> {
                    return Mono.just(name)
                            .map(ReactorTest::capitalizeString)
                            .subscribeOn(Schedulers.parallel());
                })
                .log();
        StepVerifier.create(flux1)
                .expectNext("APPLE", "ORANGE", "BANANA")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  withVirtualTime - flux that emits every second.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void fluxIntervalTakeTest() {
        VirtualTimeScheduler.getOrSet();
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
    void fluxIntervalVirtualTimeTest() {
        VirtualTimeScheduler.getOrSet();
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
     *  then - will just replay the source terminal signal, resulting in a Mono<Void> to indicate that this never signals any onNext.
     *  thenEmpty - not only returns a Mono<Void>, but it takes a Mono<Void> as a parameter. It represents a concatenation of the source completion signal then the second, empty Mono completion signal. In other words, it completes when A then B have both completed sequentially, and doesn't emit data.
     *  thenMany - waits for the source to complete then plays all the signals from its Publisher<R> parameter, resulting in a Flux<R> that will "pause" until the source completes, then emit the many elements from the provided publisher before replaying its completion signal as well.
     * ********************************************************************
     */
    @Test
    void thenManyChainTest() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(ReactorTest.deleteFromDb())
                .thenMany(ReactorTest.saveToDb())
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
    void thenEmptyTest() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(ReactorTest.saveToDb())
                .thenEmpty(ReactorTest.sendMail())
                .subscribe(System.out::println);
    }

    @Test
    void thenTest() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(ReactorTest.saveToDb())
                .then()
                .then(Mono.just("Ram"))
                .thenReturn("Done!")
                .subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  firstWithValue - first mono to return
     * ********************************************************************
     */
    @Test
    void monoFirstTest() {
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
    public void bufferGroupTest() {
        Flux<List<Integer>> flux1 = Flux
                .range(1, 7)
                .buffer(2);
        StepVerifier
                .create(flux1)
                .expectNext(Arrays.asList(1, 2))
                .expectNext(Arrays.asList(3, 4))
                .expectNext(Arrays.asList(5, 6))
                .expectNext(Arrays.asList(7))
                .verifyComplete();
    }

    @Test
    @SneakyThrows
    public void tickClockTest() {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1)).map(tick -> "fast tick " + tick);
        Flux slowClock = Flux.interval(Duration.ofSeconds(2)).map(tick -> "slow tick " + tick);
        Flux.merge(fastClock, slowClock).subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @SneakyThrows
    public void tickMergeClockTest() {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1)).map(tick -> "fast tick " + tick);
        Flux slowClock = Flux.interval(Duration.ofSeconds(2)).map(tick -> "slow tick " + tick);
        Flux clock = Flux.merge(slowClock, fastClock);
        Flux feed = Flux.interval(Duration.ofSeconds(1)).map(tick -> LocalTime.now());
        clock.withLatestFrom(feed, (tick, time) -> tick + " " + time).subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(15);
    }

    @Test
    @SneakyThrows
    public void tickZipClockTest() {
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

    /**
     * ********************************************************************
     *  cancel subscription
     * ********************************************************************
     */
    @Test
    void monoCancelSubscriptionTest() {
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
    }

    /**
     * ********************************************************************
     *  cancel subscription after n elements
     * ********************************************************************
     */
    @Test
    void monoCompleteSubscriptionRequestBoundedTest() {
        //Jill wont be fetched as subscription will be cancelled after 2 elements
        Flux<String> namesMono = Flux.just("Jack", "Jane", "Jill")
                .log()
                .map(String::toUpperCase);
        namesMono.subscribe(s -> {
                    log.info("Got: {}", s);
                },
                Throwable::printStackTrace,
                () -> log.info("Finished"),
                subscription -> subscription.request(2));
    }

    /**
     * ********************************************************************
     *  backpressure
     * ********************************************************************
     */
    @Test
    void fluxBackPressureTest() {
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
                    log.info("requesting next batch!");
                    request(requestCount);
                }
            }
        });
    }

    /**
     * ********************************************************************
     *  onBackpressureDrop - fetches all in unbounded request, but stores in internal queue, drops elements not used
     * ********************************************************************
     */
    @Test
    void fluxBackPressureDropTest() {
        Flux<Integer> fluxNumber = Flux.range(1, 15).log();

        //Fetches 2 at a time.
        fluxNumber
                .onBackpressureDrop(item -> {
                    log.info("Dropped {}", item);
                })
                .subscribe(new BaseSubscriber<>() {
                    private int count = 0;
                    private final int requestCount = 2;
                    private int batch = 0;

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        request(requestCount);
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        if (batch > 2) {
                            return;
                        }
                        count++;
                        if (count >= requestCount) {
                            count = 0;
                            batch++;
                            log.info("requesting next batch {}", batch);
                            request(requestCount);
                        }

                    }
                });
    }

    /**
     * ********************************************************************
     *  onBackpressureBuffer - fetches all in unbounded request, but stores in internal queue, but doesnt drop unused items
     * ********************************************************************
     */
    @Test
    void fluxBackPressureBuffetTest() {
        Flux<Integer> fluxNumber = Flux.range(1, 15).log();

        //Fetches 2 at a time.
        fluxNumber
                .onBackpressureBuffer()
                .subscribe(new BaseSubscriber<>() {
                    private int count = 0;
                    private final int requestCount = 2;
                    private int batch = 0;

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        request(requestCount);
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        if (batch > 2) {
                            return;
                        }
                        count++;
                        if (count >= requestCount) {
                            count = 0;
                            batch++;
                            log.info("requesting next batch {}", batch);
                            request(requestCount);
                        }

                    }
                });
    }

    /**
     * ********************************************************************
     *  onBackpressureError - To identify if receiver is overrun by items as producer is producing more elements than can be processed.
     * ********************************************************************
     */
    @Test
    void fluxBackPressureOnErrorTest() {
        Flux<Integer> fluxNumber = Flux.range(1, 15).log();

        //Fetches 2 at a time.
        fluxNumber
                .onBackpressureError()
                .subscribe(new BaseSubscriber<>() {
                    private int count = 0;
                    private final int requestCount = 2;
                    private int batch = 0;

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        request(requestCount);
                    }

                    @Override
                    protected void hookOnError(Throwable throwable) {
                        log.error("Error thrown is: {}", throwable.getMessage());
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        if (batch > 2) {
                            return;
                        }
                        count++;
                        if (count >= requestCount) {
                            count = 0;
                            batch++;
                            log.info("requesting next batch {}", batch);
                            request(requestCount);
                        }

                    }
                });
    }

    /**
     * ********************************************************************
     *  backpressure - limit rate
     * ********************************************************************
     */
    @Test
    void fluxBackPressureLimitRateTest() {
        Flux<Integer> fluxNumber = Flux.range(1, 5).log().limitRate(3);
        StepVerifier.create(fluxNumber)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  hot flux
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void connectableFluxTest() {
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .delayElements(Duration.ofSeconds(1))
                .publish();
        connectableFlux.connect();

        TimeUnit.SECONDS.sleep(3);
        connectableFlux.subscribe(i -> {
            log.info("Sub1 Number: {}", i);
        });

        TimeUnit.SECONDS.sleep(2);
        connectableFlux.subscribe(i -> {
            log.info("Sub2 Number: {}", i);
        });

        ConnectableFlux<Integer> connectableFlux2 = Flux.range(1, 10)
                .delayElements(Duration.ofSeconds(1))
                .publish();
        StepVerifier.create(connectableFlux2)
                .then(connectableFlux2::connect)
                .thenConsumeWhile(i -> i <= 5)
                .expectNext(6, 7, 8, 9, 10)
                .expectComplete()
                .verify();
    }

    /**
     * ********************************************************************
     *  hot flux - auto connect, min subscribers required before publisher emits
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void connectableAutoFluxTest() {
        //Hot Flux.
        Flux<Integer> connectableFlux = Flux.range(1, 5)
                .log()
                .delayElements(Duration.ofSeconds(1))
                .publish()
                .autoConnect(2);

        //2 subscribers
        StepVerifier.create(connectableFlux)
                .then(connectableFlux::subscribe)
                .expectNext(1, 2, 3, 4, 5)
                .expectComplete()
                .verify();
    }

    /**
     * ********************************************************************
     *  hot flux - ref count, if subscriber count goes down, publisher stops emitting
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void connectableRefCountTest() {
        //Hot Flux.
        Flux<Integer> connectableFlux = Flux.range(1, 15)
                .delayElements(Duration.ofSeconds(1))
                .doOnCancel(() -> {
                    log.info("Received cancel");
                })
                .publish()
                .refCount(2);

        //Min 2 subscribers required
        Disposable subscribe1 = connectableFlux.subscribe(e -> log.info("Sub1: " + e));
        Disposable subscribe2 = connectableFlux.subscribe(e -> log.info("Sub2: " + e));
        TimeUnit.SECONDS.sleep(3);
        subscribe1.dispose();
        subscribe2.dispose();
        TimeUnit.SECONDS.sleep(5);
    }

    /**
     * ********************************************************************
     *  defer
     * ********************************************************************
     */
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

    /**
     * ********************************************************************
     *  combineLatest - will change order based on time. Rarely used.
     * ********************************************************************
     */
    @Test
    void combineLatestTest() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux3 = Flux.combineLatest(flux1, flux2, (s1, s2) -> s1 + s2)
                .log();
        StepVerifier.create(flux3)
                .expectSubscription()
                .expectNext("bc", "bd")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  onSchedulersHook - if you have to use thread local
     * ********************************************************************
     */
    @Test
    public void schedulerHookTest() {
        Runnable stringCallable = () -> getName();
        Schedulers.onScheduleHook("myHook", runnable -> {
            log.info("before scheduled runnable");
            return () -> {
                log.info("before execution");
                runnable.run();
                log.info("after execution");
            };
        });
        Mono.just("Hello world")
                .subscribeOn(Schedulers.single())
                .subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  checkpoint
     * ********************************************************************
     */
    @Test
    void checkpointTest() {
        Flux flux = Flux.just("Jack", "Jill", "Joe")
                .checkpoint("before uppercase")
                .map(e -> e.toUpperCase())
                .checkpoint("after uppercase")
                .filter(e -> e.length() > 3)
                .checkpoint("after filter")
                .map(e -> new RuntimeException("Custom error!"));
        flux.subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  checkpoint
     * ********************************************************************
     */
    @Test
    void debugAgentTest() {
        ReactorDebugAgent.init();
        ReactorDebugAgent.processExistingClasses();
        Flux flux = Flux.just("a")
                .concatWith(Flux.error(new IllegalArgumentException("My Error!")))
                .onErrorMap(ex -> {
                    log.error("Exception: {}", ex.getMessage());
                    return new IllegalStateException("New Error!");
                });
        flux.subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  Flux.generate - programmatically create flux, synchronous
     * ********************************************************************
     */
    @Test
    void fluxGenerateTest() {
        Flux<Integer> flux = Flux.generate(() -> 1, (state, sink) -> {
            sink.next(state * 2);
            if (state == 10) {
                sink.complete();
            }
            return state + 1;
        });

        flux.subscribe(System.out::println);

        StepVerifier.create(flux)
                .expectNextCount(10)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  Flux.create - programmatically create flux, asynchronous
     * ********************************************************************
     */
    @Test
    void fluxCreateTest() {
        List<String> names = Arrays.asList("jack", "jill");
        Flux<String> flux = Flux.create(sink -> {
            names.forEach(sink::next);
            sink.complete();
        });

        StepVerifier.create(flux)
                .expectNextCount(2)
                .verifyComplete();
    }

    private String getName() {
        return "John";
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
