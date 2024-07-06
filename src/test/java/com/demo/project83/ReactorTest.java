package com.demo.project83;

import static com.demo.project83.common.HelperUtil.getCustomer;
import static com.demo.project83.common.HelperUtil.getCustomers;
import static com.demo.project83.common.HelperUtil.getName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.demo.project83.common.CompanyVO;
import com.demo.project83.common.Customer;
import com.demo.project83.common.Employee;
import com.demo.project83.common.HelperUtil;
import com.demo.project83.common.MyFeed;
import com.demo.project83.common.MyListener;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.GroupedFlux;
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
 * 1. Asynchronous
 * 2. Non-Blocking
 * 3. Backpressure
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
 *
 * Subscribers request for data. Publishers provide data
 * subscribers (downstream) and publishers (upstream)
 */
@Slf4j
public class ReactorTest {

    @BeforeAll
    public static void init() {
        BlockHound.install();
    }

    /**
     * ********************************************************************
     *  Mono
     * ********************************************************************
     */

    @Test
    void test_stepVerifier() {
        Mono.just("jack")
                .as(StepVerifier::create)
                .expectNext("jack")
                .verifyComplete();
    }

    @Test
    void test_mono() {
        //justOrEmpty
        Mono<String> mono = Mono.just("jack");
        mono.subscribe(System.out::println);
        StepVerifier.create(mono)
                .expectNext("jack")
                .verifyComplete();
    }

    @Test
    void test_justOrEmpty() {
        //justOrEmpty
        Mono<String> mono = Mono.justOrEmpty("jack");
        mono.subscribe(System.out::println);
        StepVerifier.create(mono)
                .expectNext("jack")
                .verifyComplete();
    }

    @Test
    void test_justOrEmpty_null() {
        //Note: Reactive Streams do not accept null values
        Mono<String> mono = Mono.justOrEmpty(null);
        mono.subscribe(System.out::println);
        StepVerifier.create(mono)
                .expectNextCount(0)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  log
     *  request(unbounded)
     *  default subscribe requests unbounded, all elements are requested
     * ********************************************************************
     */
    @Test
    void test_log() {
        //Note: Use log to look at transitions.
        Mono<String> mono = Mono.just("jack")
                .log();
        mono.subscribe(s -> {
            log.info("Got: {}", s);
        });
        StepVerifier.create(mono)
                .expectNext("jack")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux
     * ********************************************************************
     */
    @Test
    void test_flux() {
        Flux flux = Flux.just("jack", "raj");
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("jack", "raj")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  Avoid blocking operations that hold thread
     * ********************************************************************
     */
    @Test
    void test_delayElements() {
        Flux flux = Flux.just("jack", "raj")
                .map(e -> {
                    log.info("Received: {}", e);
                    //Bad idea to do Thread.sleep or any blocking call.
                    //Use delayElements.
                    return e;
                }).delayElements(Duration.ofSeconds(1));
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("jack", "raj")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  block
     *  Never use .block() as it blocks the thread.
     *  Can we used in tests but not in main code.
     * ********************************************************************
     */
    @Test
    void test_block() {
        String name = Mono.just("jack")
                .block();
        System.out.println(name);
    }

    /**
     * ********************************************************************
     *  flux from array, list, stream
     * ********************************************************************
     */
    @Test
    void test_fromArray() {
        Integer[] arr = {1, 2, 3, 4, 5};
        Flux<Integer> flux = Flux.fromArray(arr);
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void test_fromIterable() {
        Flux<String> flux = Flux.fromIterable(List.of("jack", "raj"));
        StepVerifier.create(flux)
                .expectNext("jack", "raj")
                .verifyComplete();
    }

    @Test
    void test_fromStream() {
        Flux<Integer> flux = Flux.fromStream(() -> List.of(1, 2, 3, 4, 5).stream());
        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flux range
     * ********************************************************************
     */
    @Test
    public void test_range() {
        Flux<Integer> flux = Flux.range(1, 5);
        flux.subscribe(n -> {
            log.info("number: {}", n);
        });
        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  map - synchronous by nature
     * ********************************************************************
     */
    @Test
    public void test_map() {
        Flux<String> flux1 = Flux.just("jack", "raj")
                .map(String::toUpperCase);
        StepVerifier
                .create(flux1)
                .expectNext("JACK", "RAJ")
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
     *  flatMap - transform object 1-1 or 1-N in asynchronous fashion, returns back Mono/Flux. Use when there is delay/IO involved.
     *  map - transform an object 1-1 in fixed time in synchronous fashion. Use when there is no delay/IO involved.
     *
     * flatMap - processing is concurrent
     * so all threads can run at same time not guarantee of being sequential.
     * ********************************************************************
     */
    @Test
    void test_flatMap() {
        Flux flux1 = Flux.just("jack", "raj")
                .flatMap(HelperUtil::capitalizeReactive);
        flux1.subscribe(System.out::println);
        //No guarantee of order, jack can come first or raj can come first.
        StepVerifier.create(flux1)
                .expectSubscription()
                .expectNextCount(2)
                .verifyComplete();

        //capitalize will happen in blocking fashion. If this function takes long or does I/O then it will be blocking
        //Use map only when there is no IO involved in the function
        Flux flux2 = Flux.just("jack", "raj")
                .map(HelperUtil::capitalize);
        flux2.subscribe(System.out::println);
        StepVerifier.create(flux2)
                .expectNext("JACK")
                .expectNext("RAJ")
                .verifyComplete();

        Flux flux3 = Flux.fromIterable(getCustomers())
                .flatMap(HelperUtil::capitalizeCustomerName);
        flux1.subscribe(System.out::println);
        //No guarantee of order
        StepVerifier.create(flux3)
                .expectNextCount(5)
                .verifyComplete();
    }

    /**
     * Here flatMap will run one after other as there is just 1 thread allocated for it.
     * You can also look at concatMap to do the same
     */
    @Test
    void test_flatMap_nonConcurrent() {
        Flux<Integer> flux = Flux.range(1, 10)
                .map(i -> i)
                .flatMap(i -> {
                    return Mono.just(i);
                }, 1);
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectSubscription()
                .expectNextCount(10)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flatMap - object modification
     * ********************************************************************
     */
    @Test
    void test_objectModification() {
        //Modification of object in chain - done via flatMap
        //Ideally create a new object instead of modifying the existing object.
        Mono<Customer> mono = Mono.just(getCustomer())
                .flatMap(e -> {
                    e.setCity("paris");
                    return Mono.just(e);
                });
        StepVerifier.create(mono)
                .assertNext(e -> {
                    assertThat(e.getCity()).isEqualTo("paris");
                })
                .verifyComplete();
    }

    @Test
    void test_objectModification_zipWith() {
        //Modification of object in chain - done via zipWith
        //The 2nd argument for zipWith is a combinator function that determines how the 2 mono are zipped
        Mono<Customer> mono = Mono.just(getCustomer())
                .zipWith(Mono.just("paris"), HelperUtil::changeCity);
        StepVerifier.create(mono)
                .assertNext(e -> {
                    assertThat(e.getCity()).isEqualTo("paris");
                })
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  distinct
     * ********************************************************************
     */
    @Test
    void test_distinct_flux() {
        Flux<String> flux = Flux.fromIterable(List.of("Jack", "Joe", "Jack", "Jill", "jack"))
                .map(String::toUpperCase)
                .distinct();
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("JACK", "JOE", "JILL")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  concatMap - works only on flux, same as flatMap but order is preserved, concatMap takes more time but ordering is preserved.
     *  flatMap - Takes less time but ordering is lost.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void test_concatMap() {
        Flux flux1 = Flux.just("jack", "raj")
                .concatMap(HelperUtil::capitalizeReactive);
        flux1.subscribe(System.out::println);
        //Guarantee of order, jack will come first then raj.
        StepVerifier.create(flux1)
                .expectSubscription()
                .expectNext("JACK", "RAJ")
                .verifyComplete();

        Flux flux2 = Flux.fromIterable(getCustomers())
                .concatMap(HelperUtil::capitalizeCustomerName);
        flux1.subscribe(System.out::println);
        StepVerifier.create(flux2)
                .expectSubscription()
                .expectNextCount(5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flatMapMany - similar to flatMap but flattens the flux
     * ********************************************************************
     */
    @Test
    void test_flatMapMany() {
        Flux<String> flux1 = Mono.just("the quick brown fox jumps over the lazy dog")
                .flatMapMany(e -> Flux.fromArray(e.toUpperCase().split("")))
                .distinct()
                .sort();
        flux1.subscribe(System.out::println);
        //26 letters in the alphabet
        StepVerifier.create(flux1)
                .expectNextCount(26)
                .expectComplete();

        Flux<Integer> flux2 = Mono.just(List.of(1, 2, 3))
                .flatMapMany(it -> Flux.fromIterable(it));
        flux2.subscribe(System.out::println);
        StepVerifier
                .create(flux2)
                .expectNext(1, 2, 3)
                .verifyComplete();

        Flux flux3 = Mono.just(getCustomers())
                .flatMapMany(e -> HelperUtil.capitalizeCustomerName(e));
        flux1.subscribe(System.out::println);
        StepVerifier.create(flux3)
                .expectSubscription()
                .expectNextCount(5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flatMapIterable - convert mono of list to flux
     * ********************************************************************
     */
    @Test
    void test_flatMapIterable() {
        Mono<List<Integer>> mono = Mono.just(Arrays.asList(1, 2, 3));
        Flux<Integer> flux = mono.flatMapIterable(list -> list);
        flux.subscribe(System.out::println);
        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flatMapIterable - convert mono of map to flux
     * ********************************************************************
     */
    @Test
    void test_flatMapIterable2() {
        Mono<Map<String, String>> mono = Mono.just(Map.of("foo", "bar"));
        Flux<Map.Entry<String, String>> flux = mono.flatMapIterable(list -> list.entrySet());
        flux.subscribe(System.out::println);
        StepVerifier
                .create(flux)
                .expectNext(Map.entry("foo", "bar"))
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
    void test_transform() {
        //Function defines input and output
        Function<Flux<String>, Flux<String>> upperCaseFunction = name -> name.map(String::toUpperCase);
        Flux<String> flux = Flux.fromIterable(List.of("Jack", "Joe"))
                .transform(upperCaseFunction);
        flux.subscribe(System.out::println);
        StepVerifier
                .create(flux)
                .expectNext("JACK", "JOE")
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
    void test_defaultIfEmpty() {
        Flux<Object> flux1 = Flux.empty()
                .defaultIfEmpty("empty")
                .log();
        StepVerifier.create(flux1)
                .expectNext("empty")
                .expectComplete()
                .verify();

        Flux<Object> flux2 = Flux.empty()
                .switchIfEmpty(Flux.just("empty"))
                .log();
        StepVerifier.create(flux2)
                .expectNext("empty")
                .expectComplete()
                .verify();
    }

    @Test
    void test_optional() {
        var mono1 = getHello(true)
                .defaultIfEmpty("NONE");
        StepVerifier.create(mono1)
                .expectNext("HELLO")
                .verifyComplete();

        var mono2 = getHello(false)
                .defaultIfEmpty("NONE");
        StepVerifier.create(mono2)
                .expectNext("NONE")
                .verifyComplete();

        var mono3 = getOptionalHello(true)
                .filter(Optional::isPresent)
                .map(Optional::get);
        StepVerifier.create(mono3)
                .expectNext("HELLO")
                .verifyComplete();

        var mono4 = getOptionalHello(false)
                .filter(Optional::isPresent)
                .map(Optional::get);
        StepVerifier.create(mono4)
                .expectNextCount(0)
                .verifyComplete();
    }

    private Mono<String> getHello(Boolean flag) {
        if (flag) {
            return Mono.just("HELLO");
        } else {
            return Mono.empty();
        }
    }

    private Mono<Optional<String>> getOptionalHello(Boolean flag) {
        if (flag) {
            return Mono.just(Optional.of("HELLO"));
        } else {
            return Mono.just(Optional.empty());
        }
    }

    /**
     * ********************************************************************
     *  switchIfEmpty with Optional
     * ********************************************************************
     */
    @Test
    public void test_switchIfEmpty() {
        Mono<Optional<Customer>> c1 = Mono.justOrEmpty(Optional.empty());
        Mono<Optional<Customer>> c2 = Mono.just(Optional.of(getCustomer()));

        Mono<Optional<Customer>> mono1 = c1
                .switchIfEmpty(Mono.just(Optional.of(new Customer())));
        StepVerifier.create(mono1)
                .expectNextCount(1)
                .expectComplete()
                .verify();

        Mono<Optional<Customer>> mono2 = c2
                .switchIfEmpty(Mono.just(Optional.empty()));
        StepVerifier.create(mono2)
                .expectNextCount(1)
                .expectComplete()
                .verify();
    }

    /**
     * ********************************************************************
     *  switchIfEmpty - Used as if-else block
     * ********************************************************************
     */
    @Test
    void test_switchIfEmpty_if_else() {
        final Customer customer = getCustomer();
        //No need to use Mono.defer on the switchIfEmpty
        Mono<String> mono = Mono.just(customer)
                .flatMap(e -> {
                    if (customer.getCity().equals("bangalore")) {
                        return Mono.just("Timezone:IST");
                    } else {
                        return Mono.empty();
                    }
                })
                .switchIfEmpty(Mono.just("Timezone:GMT"));

        StepVerifier.create(mono)
                .expectNext("Timezone:GMT")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  filterWhen - returns Mono
     *  filter - returns object
     * ********************************************************************
     */
    @Test
    void test_filterWhen() {
        Flux<String> flux1 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple").cache();
        Flux<String> flux2 = flux1.filterWhen(f -> Mono.just(f.equals("apple")));
        flux2.subscribe(System.out::println);
        StepVerifier.create(flux2)
                .expectNext("apple")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  filterWhen - returns Mono
     *  filter - returns object
     * ********************************************************************
     */
    @Test
    void test_filter() {
        Flux<String> flux1 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple").cache();
        Flux<String> flux2 = flux1.filter(f -> f.equals("apple"));
        flux2.subscribe(System.out::println);
        StepVerifier.create(flux2)
                .expectNext("apple")
                .verifyComplete();

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
     *  intersect (common) - compare 2 flux for common elements
     * ********************************************************************
     */
    @Test
    void test_intersect_inefficient() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana");
        //Without cache on flux2 it will subscribe many times.
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple").cache();
        Flux<String> commonFlux = flux1.filter(f -> {
            //toStream will block so should be avoided.
            //Inefficient - toStream will block so should be avoided.
            //Not for live stream or stream that can be subscribed only once.
            return flux2.toStream().anyMatch(e -> e.equals(f));
        });
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  intersect (common) - compare 2 flux for common elements
     * ********************************************************************
     */
    @Test
    void test_intersect_efficient_1() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana");
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple");
        Flux<String> commonFlux = flux1
                .collect(Collectors.toSet())
                .flatMapMany(set -> {
                    return flux2
                            //Filter out matching
                            //Limitation is that you can only compare 1 value collected in set.
                            .filter(t -> set.contains(t));
                });
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  intersect (common) - using join operator
     * ********************************************************************
     */
    @Test
    void test_intersect_efficient_2() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana");
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple");

        Flux<String> commonFlux = flux1.join(flux2, s -> Flux.never(), s -> Flux.never(), Tuples::of)
                //Filter out matching
                .filter(t -> t.getT1().equals(t.getT2()))
                //Revert to single value
                .map(Tuple2::getT1)
                //Remove duplicates, if any
                .groupBy(f -> f)
                .map(GroupedFlux::key);
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    @Test
    void test_intersect_efficient_3() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana");
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple");

        Mono<List<String>> monoList1 = flux1.collectList();
        Mono<List<String>> monoList2 = flux2.collectList();

        Flux<String> commonFlux = Mono.zip(monoList1, monoList2)
                .map(tuple -> {
                    List<String> list1 = tuple.getT1();
                    List<String> list2 = tuple.getT2();
                    list1.retainAll(list2);
                    return list1;
                }).flatMapIterable(e -> e);

        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    @Test
    void test_intersect_efficient_4() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana");
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple");

        Flux<String> commonFlux = flux2.filterWhen(element ->
                flux1.any(e -> e.equals(element))
        );

        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    @Test
    void test_intersect_efficient_5() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana");
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple");

        Flux<String> commonFlux = flux2.concatMap(element2 ->
                flux1.filter(element1 -> element1.equals(element2)).take(1)
        );
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    @Test
    void test_intersect_efficient_6() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana");
        Flux<String> flux2 = Flux.just("apple", "orange", "pumpkin", "papaya", "walnuts", "grapes", "pineapple");

        Flux<String> commonFlux = flux2.flatMap(element2 ->
                flux1.flatMap(element1 ->
                        element1.equals(element2) ? Flux.just(element1) : Flux.empty()
                ).take(1)
        );
        commonFlux.subscribe(System.out::println);
        StepVerifier.create(commonFlux)
                .expectNext("apple", "orange")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  startWith - add new element to flux.
     * ********************************************************************
     */
    @Test
    public void test_startWith() {
        Flux<Integer> flux1 = Flux.range(1, 3);
        Flux<Integer> flux2 = flux1.startWith(0);
        StepVerifier.create(flux2)
                .expectNext(0, 1, 2, 3)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  index
     * ********************************************************************
     */
    @Test
    void test_index() {
        //append a number to each element.
        Flux<Tuple2<Long, String>> flux = Flux
                .just("apple", "banana", "orange")
                .index();
        StepVerifier.create(flux)
                .expectNext(Tuples.of(0L, "apple"))
                .expectNext(Tuples.of(1L, "banana"))
                .expectNext(Tuples.of(2L, "orange"))
                .verifyComplete();
    }

    /**
     * ********************************************************************
     * takeWhile
     * ********************************************************************
     */
    @Test
    void test_takeWhile() {
        Flux<Integer> flux = Flux.range(1, 10);
        Flux<Integer> takeWhile = flux.takeWhile(i -> i <= 5);
        StepVerifier
                .create(takeWhile)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     * skipWhile
     * ********************************************************************
     */
    @Test
    void test_skipWhile() {
        Flux<Integer> flux = Flux.range(1, 10);
        Flux<Integer> skipWhile = flux.skipWhile(i -> i <= 5);
        StepVerifier
                .create(skipWhile)
                .expectNext(6, 7, 8, 9, 10)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  collectList - flux to mono of list
     * ********************************************************************
     */
    @Test
    void test_collectList() {
        Mono<List<Integer>> flux = Flux
                .just(1, 2, 3)
                .collectList();
        StepVerifier.create(flux)
                .expectNext(Arrays.asList(1, 2, 3))
                .verifyComplete();
    }

    /**
     * ********************************************************************
     * collectSortedList- flux to mono of list
     * ********************************************************************
     */
    @Test
    void test_collectSortedList() {
        Mono<List<Integer>> listMono2 = Flux
                .just(5, 2, 4, 1, 3)
                .collectSortedList();
        StepVerifier.create(listMono2)
                .expectNext(Arrays.asList(1, 2, 3, 4, 5))
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  collectMap
     * ********************************************************************
     */
    @Test
    void test_collectMap() {
        Mono<Map<Object, Object>> flux = Flux.just("yellow:banana", "red:apple")
                .map(item -> item.split(":"))
                .collectMap(item -> item[0], item -> item[1]);

        Map<Object, Object> map = new HashMap<>();
        flux.subscribe(map::putAll);
        map.forEach((key, value) -> System.out.println(key + " -> " + value));

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
    void test_collectMultimap() {
        Mono<Map<String, Collection<String>>> flux = Flux.just("yellow:banana", "red:grapes", "red:apple", "yellow:pineapple")
                .map(item -> item.split(":"))
                .collectMultimap(
                        item -> item[0],
                        item -> item[1]);
        Map<Object, Collection<String>> map = new HashMap<>();
        flux.subscribe(map::putAll);
        map.forEach((key, value) -> System.out.println(key + " -> " + value));

        StepVerifier.create(flux)
                .expectNext(Map.of("red", List.of("grapes", "apple"), "yellow", List.of("banana", "pineapple")))
                .verifyComplete();
    }

    /**
     * ********************************************************************
     * concat - subscribes to publishers in sequence, order guaranteed, static function
     * concatWith - subscribes to publishers in sequence, order guaranteed, instance function
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void test_concat() {
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
     * concat - subscribes to publishers in sequence, order guaranteed, static function
     * concatWith - subscribes to publishers in sequence, order guaranteed, instance function
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void test_concatWith() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux3 = flux1.concatWith(flux2);
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

    /**
     * ********************************************************************
     * concatDelayError - When one flux can throw an error
     * ********************************************************************
     */
    @Test
    void test_concatDelayError() {
        Flux<String> flux1 = Flux.just("a", "b", "c")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new RuntimeException("error!");
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("d", "e", "f");
        Flux<String> flux3 = Flux.concatDelayError(flux1, flux2);

        StepVerifier.create(flux3)
                .expectSubscription()
                .expectNext("a", "d", "e", "f")
                .expectError()
                .verify();
    }

    /**
     * ********************************************************************
     *  combineLatest - will change order based on time. Rarely used.
     * ********************************************************************
     */
    @Test
    void test_combineLatest() {
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
     *  merge - subscribes to publishers eagerly, order not guaranteed, static function
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void test_merge() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");
        //Eager will not wait till first flux3 finishes.
        Flux<String> flux3 = Flux.merge(flux1, flux2);

        StepVerifier.create(flux3)
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
    void test_mergeWith() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");
        //Eager will not wait till first flux finishes.
        Flux<String> flux3 = flux1.mergeWith(flux2);

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
    void test_mergeSequential() {
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux3 = Flux.mergeSequential(flux1, flux2, flux1);

        StepVerifier.create(flux3)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "a", "b")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     * mergeDelayError - when one flux can throw an error
     * ********************************************************************
     */
    @Test
    void test_mergeDelayError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new RuntimeException("error");
                    }
                    return s;
                }).doOnError(e -> log.error("Error: {}", e));

        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> flux3 = Flux.mergeDelayError(1, flux1, flux2, flux1);

        StepVerifier.create(flux3)
                .expectSubscription()
                .expectNext("a", "c", "d", "a")
                .expectError()
                .verify();
    }

    /**
     * ********************************************************************
     *  zip - subscribes to publishers in eagerly, waits for both flux to emit one element.
     *  2-8 flux can be zipped, returns a tuple, Static function
     * ********************************************************************
     */
    @Test
    void test_zip() {
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
     *  zipWith - subscribes to publishers in eagerly, waits for both flux to emit one element.
     *  2-8 flux can be zipped, returns a tuple, Instance function
     *  When 2 different size flux are combined zipWith return the smaller item size new flux.
     * ********************************************************************
     */
    @Test
    void test_zipWith() {
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
     * Cant do zipWith to combine mono & flux.
     * Use the operator join
     * ********************************************************************
     */
    @Test
    void test_join() {
        Mono<String> mono = Mono.just("green");
        Flux<String> flux1 = Flux.just("apple", "banana")
                .join(mono, s -> Flux.never(), s -> Flux.never(), Tuples::of)
                .flatMap(tuple -> {
                    return Mono.just(tuple.getT2() + " " + tuple.getT1());
                });
        StepVerifier.create(flux1)
                .expectNext("green apple")
                .expectNext("green banana")
                .verifyComplete();

        Flux<String> flux2 = Flux.just("apple", "banana")
                .zipWith(mono.cache().repeat())
                .flatMap(tuple -> {
                    return Mono.just(tuple.getT2() + " " + tuple.getT1());
                });
        StepVerifier.create(flux2)
                .expectNext("green apple")
                .expectNext("green banana")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     * error
     * ********************************************************************
     */
    @Test
    void test_onError() {
        Mono<String> mono1 = Mono.just("jack")
                .map(s -> {
                    throw new RuntimeException("ERROR");
                });
        mono1.subscribe(s -> log.info("name: {}", s), Throwable::printStackTrace);
        StepVerifier.create(mono1)
                .expectError(RuntimeException.class)
                .verify();

        System.out.println("********************************************************************");

        Mono<String> mono2 = Mono.just("jack")
                .flatMap(s -> {
                    return Mono.error(new RuntimeException("ERROR"));
                });
        mono2.subscribe(s -> log.info("name: {}", s), Throwable::printStackTrace);

        StepVerifier.create(mono2)
                .expectError(RuntimeException.class)
                .verify();
    }

    /**
     * ********************************************************************
     *  Error Recover Handling
     *  onErrorReturn - Return value on error
     * ********************************************************************
     */
    @Test
    void test_onErrorReturn() {
        Mono<Object> mono1 = Mono.error(new RuntimeException("error"))
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
    void test_onErrorResume() {
        Mono<Object> mono1 = Mono.error(new RuntimeException("error"))
                .onErrorResume(e -> Mono.just("Jack"));
        StepVerifier.create(mono1)
                .expectNext("Jack")
                .verifyComplete();

        Mono<Object> mono2 = Mono.error(new RuntimeException("error"))
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
    void test_onErrorContinue() {
        Flux<String> flux =
                Flux.just("a", "b", "c")
                        .map(e -> {
                            if (e.equals("b")) {
                                throw new RuntimeException("error");
                            }
                            return e;
                        })
                        .concatWith(Mono.just("d"))
                        .onErrorContinue((ex, value) -> {
                            log.info("Exception: {}", ex);
                            log.info("value: {}", value);
                        });
        StepVerifier.create(flux)
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
    void test_doOnError() {
        Mono<Object> mono1 = Mono.error(new RuntimeException("error"))
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
    void test_onErrorMap() {
        Flux flux = Flux.just("Jack", "Jill")
                .map(u -> {
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

        StepVerifier.create(flux)
                .expectErrorMessage("Class cast error!")
                .verify();
    }

    /**
     * ********************************************************************
     *  retry
     * ********************************************************************
     */
    @Test
    void test_retry() {
        AtomicLong attemptCounter = new AtomicLong();
        Mono<String> mono = Mono.just("Jack")
                .flatMap(n -> {
                    return this.twoAttemptFunction(attemptCounter, n);
                })
                .retry(3);
        StepVerifier.create(mono)
                .assertNext(e -> {
                    assertThat(e).isEqualTo("Hello Jack");
                })
                .verifyComplete();
    }

    private Mono<String> twoAttemptFunction(AtomicLong counter, String name) {
        Long attempt = counter.getAndIncrement();
        log.info("attempt value: {}", attempt);
        if (attempt < 2) {
            throw new RuntimeException("error");
        }
        return Mono.just("Hello " + name);
    }

    /**
     * ********************************************************************
     *  retryWhen
     * ********************************************************************
     */
    @Test
    void test_retryWhen() {
        AtomicLong attemptCounter1 = new AtomicLong();
        RetryBackoffSpec retryFilter1 = Retry.backoff(3, Duration.ofSeconds(1))
                .filter(throwable -> throwable instanceof RuntimeException);

        Mono<String> mono1 = Mono.just("Jack")
                .flatMap(e -> this.greetAfter2Failure(attemptCounter1, e))
                .retryWhen(retryFilter1);
        StepVerifier.create(mono1)
                .assertNext(e -> {
                    assertThat(e).isEqualTo("Hello Jack");
                })
                .verifyComplete();

        AtomicLong attemptCounter2 = new AtomicLong();
        RetryBackoffSpec retryFilter2 = Retry.fixedDelay(1, Duration.ofSeconds(1))
                .filter(throwable -> throwable instanceof RuntimeException)
                .onRetryExhaustedThrow(((retryBackoffSpec, retrySignal) ->
                        Exceptions.propagate(retrySignal.failure())
                ));
        Mono<String> mono2 = Mono.just("Jack")
                .flatMap(e -> this.greetAfter2Failure(attemptCounter2, e))
                .retryWhen(retryFilter2);
        StepVerifier.create(mono2)
                .expectErrorMessage("error")
                .verify();
    }

    private Mono<String> greetAfter2Failure(AtomicLong attemptCounter, String name) {
        Long attempt = attemptCounter.getAndIncrement();
        log.info("attempt value: {}", attempt);
        if (attempt < 2) {
            throw new RuntimeException("error");
        }
        return Mono.just("Hello " + name);
    }

    /**
     * ********************************************************************
     *  repeat - repeat an operation n times.
     * ********************************************************************
     */
    @Test
    void test_repeat() {
        Mono<List<String>> flux = Mono.defer(() -> {
                    return Mono.just("UUID " + UUID.randomUUID());
                })
                .repeat(5)
                .collectList();
        flux.subscribe(System.out::println);

        StepVerifier.create(flux)
                .assertNext(e -> {
                    assertThat(e.size()).isEqualTo(6);
                })
                .verifyComplete();
    }

    @Test
    void test_takeUntil() {
        AtomicLong counter = new AtomicLong();
        Mono<List<String>> flux = Mono.defer(() -> {
                    return Mono.just("UUID " + UUID.randomUUID());
                })
                .repeat()
                .takeUntil(e -> {
                    return counter.incrementAndGet() == 5;
                })
                .collectList();

        StepVerifier.create(flux)
                .assertNext(e -> {
                    assertThat(e.size()).isEqualTo(5);
                })
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  Subscribe onComplete, onError
     *  Never use this format of subscribe code, always use doOn operator
     * ********************************************************************
     */
    @Test
    void test_doOn() {
        Flux<Integer> numFlux = Flux.range(1, 5)
                .map(i -> {
                    if (i == 4) {
                        throw new RuntimeException("error");
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

    /**
     * ********************************************************************
     * doOn - doOnSubscribe, doOnNext, doOnError, doFinally, doOnComplete
     * ********************************************************************
     */
    @Test
    void test_test_doOn_2() {
        Flux<Object> flux = Flux.error(new RuntimeException("error"))
                .doOnSubscribe(s -> System.out.println("Subscribed!"))
                .doOnRequest(s -> System.out.println("Requested!"))
                .doOnNext(p -> System.out.println("Next!"))
                .doOnComplete(() -> System.out.println("Completed!"))
                .doFinally((e) -> System.out.println("Signal: " + e))
                .doOnError((e) -> System.out.println("Error: " + e));

        StepVerifier.create(flux)
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(flux)
                .verifyError(RuntimeException.class);

        Mono<Object> mono = Mono.error(new RuntimeException("error"))
                .doOnSubscribe(s -> System.out.println("Subscribed!"))
                .doOnRequest(s -> System.out.println("Requested!"))
                .doOnNext(p -> System.out.println("Next!"))
                .doFinally((e) -> System.out.println("Signal: " + e))
                .doOnError((e) -> System.out.println("Error: " + e))
                .doOnSuccess((e) -> System.out.println("Success!"));

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void test_doOn_3() {
        Flux flux = Flux.error(new RuntimeException("My Error"));
        flux.subscribe(
                onNext(),
                onError(),
                onComplete()
        );
    }

    private Consumer<Object> onNext() {
        return o -> System.out.println("Received : " + o);
    }

    private Consumer<Throwable> onError() {
        return e -> System.out.println("ERROR : " + e.getMessage());
    }

    private Runnable onComplete() {
        return () -> System.out.println("Completed");
    }

    /**
     * ********************************************************************
     *  StepVerifier - assertNext, thenRequest, thenCancel, expectError, expectErrorMessage
     * ********************************************************************
     */
    @Test
    void test_StepVerifier() {
        Flux flux1 = Flux.fromIterable(Arrays.asList("Jack", "Jill"));
        StepVerifier.create(flux1)
                .expectNextMatches(user -> user.equals("Jack"))
                .assertNext(user -> assertThat(user).isEqualTo("Jill"))
                .verifyComplete();

        //Wait for 2 elements.
        StepVerifier.create(flux1)
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
    void test_propagate() {
        Flux flux = Flux.just("Jack", "Jill")
                .map(u -> {
                    try {
                        return HelperUtil.checkName(u);
                    } catch (HelperUtil.CustomException e) {
                        throw Exceptions.propagate(e);
                    }
                });
        flux.subscribe(System.out::println);
        StepVerifier.create(flux)
                .expectNext("JACK")
                .verifyError(HelperUtil.CustomException.class);
    }

    /**
     * ********************************************************************
     *  subscribeOn - influences upstream (whole chain)
     * ********************************************************************
     */
    @Test
    void test_subscribeOn() {
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

    @SneakyThrows
    @Test
    void test_subscribeOn_t1() {
        Flux<Integer> flux1 = Flux.range(0, 2)
                .map(i -> {
                    //will run on incoming thread
                    log.info("Mapping for " + i + " is done by thread " + Thread.currentThread().getName());
                    return i;
                });
        Runnable r1 = () -> flux1.subscribe(s -> {
            log.info("Received " + s + " via " + Thread.currentThread().getName());
        });
        Thread t1 = new Thread(r1, "t1");
        log.info("Program thread :: " + Thread.currentThread().getName());
        t1.start();
        t1.join();
    }

    @SneakyThrows
    @Test
    void test_subscribeOn_t2() {
        Flux<Integer> flux2 = Flux.range(0, 2)
                .map(i -> {
                    //will run on incoming thread
                    log.info("Upstream: Mapping for {} is done by thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.single())
                .map(i -> {
                    //will run on new thread
                    log.info("Downstream: Mapping for {} is done by thread {}", i, Thread.currentThread().getName());
                    return i;
                });
        Runnable r2 = () -> flux2.subscribe(s -> {
            log.info("Received {} via {}", s, Thread.currentThread().getName());
        });
        Thread t2 = new Thread(r2, "t2");
        log.info("Program thread {}" + Thread.currentThread().getName());
        t2.start();
        t2.join();
    }

    @SneakyThrows
    @Test
    void test_subscribeOn_t3() {
        Flux<Integer> flux3 = Flux.range(0, 2)
                .map(i -> {
                    //will run on new thread
                    log.info("Upstream: Mapping for {} is done by thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    //will run on new thread
                    log.info("Downstream: Mapping for {} is done by thread {}", i, Thread.currentThread().getName());
                    return i;
                });
        Runnable r3 = () -> flux3.subscribe(s -> {
            log.info("Received {} via {}", s, Thread.currentThread().getName());
        });
        Thread t3 = new Thread(r3, "t2");
        log.info("Program thread {}" + Thread.currentThread().getName());
        t3.start();
        t3.join();
    }

    /**
     * ********************************************************************
     *  Schedulers
     *
     * parallel - for CPU intensive tasks (computation), thread pool workers = number of CPU cores
     * newParallel - same as above but new pool
     * boundedElastic - for IO intensive tasks (network calls), thread pool contains 10 * number of CPU cores
     * newBoundedElastic - same as above but new pool
     * immediate - keep the execution in the current thread
     * single - single reusable thread for all the callers
     * newSingle - same as above but new pool
     * elastic - unlimited threads (DON'T USE)
     *
     * We can have multiple publishOn methods which will keep switching the context.
     * The subscribeOn method can not do that. Only the very first subscribeOn method which is close to the source takes precedence.
     * ********************************************************************
     */
    @Test
    void test_Schedulers() {
        Flux numbFlux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Map1 Num: {}, Thread: {}", i, Thread.currentThread().getName());
                    return i;
                }).subscribeOn(Schedulers.newSingle("my-thread"))
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
    void test_publishOn() {
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
     *  publishOn - influences downstream
     * ********************************************************************
     */
    @Test
    void test_publishOn_2() {
        Flux numbFlux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Map1 Num: {}, Thread: {}", i, Thread.currentThread().getName());
                    return i;
                }).publishOn(Schedulers.newSingle("my-thread"))
                .map(i -> {
                    log.info("Map2 Num: {}, Thread: {}", i, Thread.currentThread().getName());
                    return i;
                });
        numbFlux.subscribe();
    }

    /**
     * ********************************************************************
     *  fromSupplier - returns a value
     *  fromCallable - returns a value or exception
     *  fromRunnable - doesnt return value
     * ********************************************************************
     */
    @Test
    public void test_fromSupplier() {
        Supplier<String> stringSupplier = () -> getName();
        Mono<String> mono = Mono.fromSupplier(stringSupplier);

        mono.subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  fromSupplier - returns a value
     *  fromCallable - returns a value or exception
     *  fromRunnable - doesnt return value
     * ********************************************************************
     */
    @Test
    public void test_fromCallable() {
        Callable<String> stringCallable = () -> getName();
        Mono<String> mono = Mono.fromCallable(stringCallable)
                .subscribeOn(Schedulers.boundedElastic());
        mono.subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  fromSupplier - returns a value
     *  fromCallable - returns a value or exception
     *  fromRunnable - doesnt return value
     * ********************************************************************
     */
    @Test
    public void test_fromRunnable() {
        Runnable stringCallable = () -> getName();
        Mono<Object> mono = Mono.fromRunnable(stringCallable)
                .subscribeOn(Schedulers.boundedElastic());
        mono.subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     *  fromCallable - read file may be blocking, we don't want to block main thread.
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void test_readFile_fromCallable() {
        Mono<List<String>> listMono = Mono.fromCallable(() -> Files.readAllLines(Path.of("src/test/resources/file.txt")))
                .subscribeOn(Schedulers.boundedElastic());
        listMono.subscribe(l -> log.info("Lines: {}", l));

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
     * Flux.using(
     *     resourceSupplier,
     *     (resource) -> return Publisher,
     *     (resource) -> clean this up
     * )
     *
     * share() creates a hot publisher, else it would be a cold publisher.
     * Cold publisher would read the file for each subscriber – that would mean opening and reading the same file many times.
     * ********************************************************************
     */
    @Test
    void test_readFile_using() {
        Path filePath = Paths.get("src/test/resources/file.txt");
        Flux<String> fileFlux = Flux.using(
                () -> Files.lines(filePath),
                Flux::fromStream,
                Stream::close
        );
        fileFlux.subscribe(l -> log.info("Lines: {}", l));

        Flux<String> fileFlux2 = fileFlux
                .subscribeOn(Schedulers.newParallel("file-copy", 3))
                .share();
        fileFlux2.subscribe(l -> log.info("Lines: {}", l));
    }

    /**
     * ********************************************************************
     *  ParallelFlux - Will complete in 1 sec even when 3 ops take 3 seconds in sequence
     * ********************************************************************
     */
    @Test
    void test_parallel() {
        log.info("Cores: {}", Runtime.getRuntime().availableProcessors());
        ParallelFlux<String> flux1 = Flux.just("apple", "orange", "banana")
                .parallel()
                .runOn(Schedulers.parallel())
                .map(HelperUtil::capitalizeString);
        StepVerifier.create(flux1)
                .expectNextCount(3)
                .verifyComplete();


        Flux<String> flux2 = Flux.just("apple", "orange", "banana")
                .flatMap(name -> {
                    return Mono.just(name)
                            .map(HelperUtil::capitalizeString)
                            .subscribeOn(Schedulers.parallel());
                });
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
    void test_parallel_2() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana")
                .flatMap(name -> {
                    return Mono.just(name)
                            .map(HelperUtil::capitalizeString)
                            .subscribeOn(Schedulers.parallel());
                });
        StepVerifier.create(flux1)
                .expectNextCount(3)
                .verifyComplete();
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
                            .map(e -> HelperUtil.capitalizeStringLatch(e, latch))
                            .subscribeOn(Schedulers.parallel())
                            .subscribe();
                    return Mono.empty();
                });
        StepVerifier.create(flux1)
                .verifyComplete();
        latch.await(5, TimeUnit.SECONDS);
    }

    /**
     * ********************************************************************
     *  flatMapSequential - Maintains order but executes in parallel
     * ********************************************************************
     */
    @Test
    void test_flatMapSequential() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana")
                .flatMapSequential(name -> {
                    return Mono.just(name)
                            .map(HelperUtil::capitalizeString)
                            .subscribeOn(Schedulers.parallel());
                });
        StepVerifier.create(flux1)
                .expectNext("APPLE", "ORANGE", "BANANA")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  flatMapSequential - Maintains order but executes in parallel
     * ********************************************************************
     */
    @Test
    void test_flatMapSequential_2() {
        Flux<String> flux1 = Flux.just("apple", "orange", "banana")
                .flatMapSequential(name -> {
                    return Mono.just(name)
                            .map(HelperUtil::capitalizeString)
                            .subscribeOn(Schedulers.parallel());
                }, 1)
                .log();
        StepVerifier.create(flux1)
                .expectNext("APPLE", "ORANGE", "BANANA")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  withVirtualTime - flux that emits every second.
     *  interval - blocks thread, so you will have to use sleep to see the output
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void test_withVirtualTime() {
        VirtualTimeScheduler.getOrSet();
        Flux<Long> intervalFlux = Flux.interval(Duration.ofSeconds(1))
                .log()
                .take(10);
        intervalFlux.subscribe(i -> log.info("Number: {}", i));
        TimeUnit.SECONDS.sleep(5);
        StepVerifier.withVirtualTime(() -> intervalFlux)
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(999))
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
    void test_withVirtualTime_2() {
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
    void test_thenManyChain() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(HelperUtil.deleteFromDb())
                .thenMany(HelperUtil.saveToDb())
                .subscribe(System.out::println);
    }

    @Test
    void test_thenEmpty() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(HelperUtil.saveToDb())
                .thenEmpty(HelperUtil.sendMail())
                .subscribe(System.out::println);
    }

    @Test
    void test_then() {
        Flux<String> names = Flux.just("Jack", "Jill");
        names.map(String::toUpperCase)
                .thenMany(HelperUtil.saveToDb())
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
    void test_monoFirst() {
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
    public void test_bufferGroup() {
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
    void test_tickClock() {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1)).map(tick -> "fast tick " + tick);
        Flux slowClock = Flux.interval(Duration.ofSeconds(2)).map(tick -> "slow tick " + tick);
        Flux.merge(fastClock, slowClock).subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @SneakyThrows
    public void test_tickMergeClock() {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1)).map(tick -> "fast tick " + tick);
        Flux slowClock = Flux.interval(Duration.ofSeconds(2)).map(tick -> "slow tick " + tick);
        Flux clock = Flux.merge(slowClock, fastClock);
        Flux feed = Flux.interval(Duration.ofSeconds(1)).map(tick -> LocalTime.now());
        clock.withLatestFrom(feed, (tick, time) -> tick + " " + time).subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(15);
    }

    @Test
    @SneakyThrows
    void test_tickZipClock() {
        Flux fastClock = Flux.interval(Duration.ofSeconds(1)).map(tick -> "fast tick " + tick);
        Flux slowClock = Flux.interval(Duration.ofSeconds(2)).map(tick -> "slow tick " + tick);
        fastClock.zipWith(slowClock, (tick, time) -> tick + " " + time).subscribe(System.out::println);
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @SneakyThrows
    void test_emitter() {
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
    void test_monoCancelSubscription() {
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
    void test_request() {
        //Jill won't be fetched as subscription will be cancelled after 2 elements
        Flux<String> namesMono = Flux.just("Jack", "Jane", "Jill")
                .log();
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
    void test_fluxBackPressure() {
        Flux<Integer> fluxNumber = Flux.range(1, 5).log();

        //Fetches 2 at a time.
        fluxNumber.subscribe(new BaseSubscriber<>() {
            private final int requestCount = 2;
            private int count = 0;

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
    void test_fluxBackPressureDrop() {
        Flux<Integer> fluxNumber = Flux.range(1, 15).log();

        //Fetches 2 at a time.
        fluxNumber
                .onBackpressureDrop(item -> {
                    log.info("Dropped {}", item);
                })
                .subscribe(new BaseSubscriber<>() {
                    private final int requestCount = 2;
                    private int count = 0;
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
    void test_fluxBackPressureBuffet() {
        Flux<Integer> fluxNumber = Flux.range(1, 15).log();

        //Fetches 2 at a time.
        fluxNumber
                .onBackpressureBuffer()
                .subscribe(new BaseSubscriber<>() {
                    private final int requestCount = 2;
                    private int count = 0;
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
    void test_fluxBackPressureOnError() {
        Flux<Integer> fluxNumber = Flux.range(1, 15).log();

        //Fetches 2 at a time.
        fluxNumber
                .onBackpressureError()
                .subscribe(new BaseSubscriber<>() {
                    private final int requestCount = 2;
                    private int count = 0;
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
    void test_fluxBackPressureLimitRate() {
        Flux<Integer> fluxNumber = Flux.range(1, 5)
                .log()
                .limitRate(3);
        StepVerifier.create(fluxNumber)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    /**
     * ********************************************************************
     *  cold flux - producing/emitting only when a subscriber subscribes, generates new sets of values for each new subscription, eg: spotify
     *  hot flux - emitting happens even there is no subscriber. All the subscribers get the value from the single data producer irrespective of the time they started subscribing, eg: radio
     * ********************************************************************
     */
    @Test
    @SneakyThrows
    void test_connectableFlux() {
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
    void test_connectableAutoFlux() {
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
    void test_connectableFlux_1() {
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
    void test_defer() {
        Mono<UUID> just = Mono.just(UUID.randomUUID());
        Mono<UUID> deferJust = Mono.defer(() -> Mono.just(UUID.randomUUID()));

        just.subscribe(l -> log.info("UUID: {}", l));
        just.subscribe(l -> log.info("UUID: {}", l));
        System.out.println();
        deferJust.subscribe(l -> log.info("UUID: {}", l));
        deferJust.subscribe(l -> log.info("UUID: {}", l));
    }

    /**
     * ********************************************************************
     *  onSchedulersHook - if you have to use thread local
     * ********************************************************************
     */
    @Test
    public void test_onScheduleHook() {
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
    void test_checkpoint() {
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
    void flux_test_debugAgent() {
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
     * Flux.generate - programmatically create flux, synchronous, cant emit without downstream subscriber asking for it.
     * Flux.create - programmatically create flux, asynchronous, can emit more elements without downstream subscriber asking for it.
     * ********************************************************************
     */
    @Test
    void test_flux_generate() {
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
        System.out.println();
    }

    /**
     * ********************************************************************
     * Flux.generate - programmatically create flux, synchronous
     * Flux.create - programmatically create flux, asynchronous
     *
     * buffer - buffer if downstream cant keep up
     * drop - drop if downstream cant keep up
     * error - singal error when downstream cant keep up
     * ignore - ignore downstream backpressure requests
     * latest - downstream will only get latest
     * ********************************************************************
     */
    @Test
    void test_flux_create() {
        List<String> names = Arrays.asList("jack", "jill");
        Flux<String> flux = Flux.create(sink -> {
            names.forEach(sink::next);
            sink.complete();
        });

        StepVerifier.create(flux)
                .expectNextCount(2)
                .verifyComplete();

        Flux<Integer> integerFlux = Flux.create((FluxSink<Integer> fluxSink) -> {
            IntStream.range(0, 5)
                    .peek(i -> System.out.println("going to emit - " + i))
                    .forEach(fluxSink::next);
            fluxSink.complete();
        });

        StepVerifier.create(integerFlux)
                .expectNextCount(5)
                .verifyComplete();

        Flux<Integer> integerFlux2 = Flux.create((FluxSink<Integer> fluxSink) -> {
            IntStream.range(0, 5)
                    .peek(i -> System.out.println("going to emit - " + i))
                    .forEach(fluxSink::next);
            fluxSink.complete();
        }, FluxSink.OverflowStrategy.DROP);

        StepVerifier.create(integerFlux2)
                .expectNextCount(5)
                .verifyComplete();
    }

    @Test
    void test_chain() {
        CompanyVO request = new CompanyVO();
        request.setName("Twitter");
        Mono.just(request)
                .map(HelperUtil::convertToEntity)
                .zipWith(HelperUtil.getNameSuffix(), HelperUtil::appendSuffix)
                .flatMap(HelperUtil::addCompanyOwner)
                .flatMap(HelperUtil::appendOrgIdToDepartment)
                .flatMap(HelperUtil::save)
                .subscribe(System.out::println);
    }

    /**
     * ********************************************************************
     * expand 	Finding the shortest path in a graph. Searching file system. Finding neighbor nodes in a network.
     * expandDeep 	Finding all possible combinations.
     * ********************************************************************
     */
    @Test
    void test_expand() {
        Employee CEO = new Employee("CEO");

        // Directors reporting to CEO
        Employee directorA = new Employee("Director of Dept A");
        Employee directorB = new Employee("Director of Dept B");
        CEO.addDirectReports(directorA, directorB);

        // Managers reporting to directors
        Employee managerA1 = new Employee("Manager 1 of Dept A");
        Employee managerA2 = new Employee("Manager 2 of Dept A");
        Employee managerB1 = new Employee("Manager 1 of Dept B");
        Employee managerB2 = new Employee("Manager 2 of Dept B");
        directorA.addDirectReports(managerA1, managerA2);
        directorB.addDirectReports(managerB1, managerB2);

        Mono.fromSupplier(() -> CEO)
                .expand(this::getDirectReports)
                .subscribe(System.out::println);
    }

    @Test
    void test_expandDeep() {
        Employee CEO = new Employee("CEO");

        // Directors reporting to CEO
        Employee directorA = new Employee("Director of Dept A");
        Employee directorB = new Employee("Director of Dept B");
        CEO.addDirectReports(directorA, directorB);

        // Managers reporting to directors
        Employee managerA1 = new Employee("Manager 1 of Dept A");
        Employee managerA2 = new Employee("Manager 2 of Dept A");
        Employee managerB1 = new Employee("Manager 1 of Dept B");
        Employee managerB2 = new Employee("Manager 2 of Dept B");
        directorA.addDirectReports(managerA1, managerA2);
        directorB.addDirectReports(managerB1, managerB2);

        Mono.fromSupplier(() -> CEO)
                .expandDeep(this::getDirectReports)
                .subscribe(System.out::println);
    }

    private Flux<Employee> getDirectReports(Employee employee) {
        return Flux.fromIterable(employee.getDirectReports());
    }

    @Test
    void test_fluxToMono() {
        Mono<List<String>> mono = Flux.just("jack", "raj").collectList();
        Flux<List<String>> flux = Flux.just("jack", "raj").collectList().flatMapMany(Flux::just);

        StepVerifier.create(mono)
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(flux)
                .expectNextCount(1)
                .verifyComplete();

    }

    @Test
    void test_compareMapWithList() {
        List<String> colors = List.of("red", "blue", "green");
        Map<String, String> fruitMap = Map.of("red", "apple", "green", "grapes");
        Mono<List<String>> flux1 = Mono.just(fruitMap)
                .flatMap(map -> {
                    return Flux.fromIterable(colors)
                            .flatMap(color -> {
                                if (map.containsKey(color)) {
                                    return Mono.just(map.get(color));
                                }
                                return Mono.empty();
                            }).collectList();
                });
        flux1.subscribe(System.out::println);

        StepVerifier.create(flux1)
                .expectNext(List.of("apple", "grapes"))
                .verifyComplete();

        Flux<String> flux2 = Mono.just(fruitMap)
                .flatMapMany(map ->
                        Flux.fromIterable(colors)
                                .flatMap(color -> {
                                    String fruit = fruitMap.get(color);
                                    return fruit != null ? Flux.just(fruit) : Flux.empty();
                                })
                );
        flux2.subscribe(System.out::println);
        StepVerifier.create(flux2)
                .expectNext("apple")
                .expectNext("grapes")
                .verifyComplete();
    }

    /**
     * ********************************************************************
     * timeout - if response doesnt come in certain time then timeout.
     * ********************************************************************
     */
    @Test
    void test_timeout() {
        Mono<String> mono = Mono.just("jack")
                .delayElement(Duration.ofSeconds(5))
                .timeout(Duration.ofSeconds(1))
                .onErrorReturn("raj");
        StepVerifier.create(mono)
                .expectNext("raj")
                .verifyComplete();
    }

    @Test
    void test_pageImpl() {
        List<String> names = List.of("Jack", "Raj", "Edward");
        PageRequest pageRequest = PageRequest.of(0, 5);
        Mono<PageImpl<String>> pageFlux = Flux.fromIterable(names)
                .collectList()
                .zipWith(Mono.just(names.size()))
                .map(t -> new PageImpl<>(t.getT1(), pageRequest, names.size()));

        pageFlux.subscribe(System.out::println);

        StepVerifier.create(pageFlux)
                .assertNext(e -> {
                    assertEquals(e.getNumberOfElements(), 3);
                    assertEquals("Jack", e.getContent().get(0));
                })
                .verifyComplete();
    }

    @Test
    void test_function() {
        Function<String, Mono<String>> stringSupplier = p -> Mono.just("hello " + p);
        Mono<String> mono = Mono.defer(() -> stringSupplier.apply("jack"));
        StepVerifier.create(mono)
                .expectNext("hello jack")
                .verifyComplete();
    }

    @SneakyThrows
    @Test
    void test_blockHound() {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                TimeUnit.SECONDS.sleep(2);
                return "";
            });
            Schedulers.parallel().schedule(task);
            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

    @Test
    void test_blockHound2() {
        Mono<String> mono = Mono.just("apple")
                .flatMap(name -> {
                    return Mono.just("name")
                            .map(n -> {
                                HelperUtil.sleep(1);
                                return n;
                            })
                            .subscribeOn(Schedulers.parallel());
                });

        StepVerifier.create(mono)
                .expectError(BlockingOperationError.class);
    }

}