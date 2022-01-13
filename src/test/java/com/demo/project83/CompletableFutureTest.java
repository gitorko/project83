package com.demo.project83;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@Slf4j
public class CompletableFutureTest {

    static AtomicInteger counter = new AtomicInteger();

    /**
     * get() is blocking call. So main thread has to wait.
     * Old way with Future. Dont use it.
     */
    @Test
    @SneakyThrows
    void blockingChain_test() {
        counter = new AtomicInteger();
        List<Future<String>> futureLst = new ArrayList<>();
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < 5; i++) {
            int finalI = i;
            Future<String> future = executor.submit(() -> greetHello("Jack_" + finalI));
            futureLst.add(future);
        }
        for (Future<String> future : futureLst) {
            //get is blocking the main thread here.
            String message = future.get();
            finishedGreetHello(message);
        }
        executor.shutdown();
        Assertions.assertEquals(5, counter.get());
    }

    /**
     * Callback attached so non blocking.
     *
     * Ability to provide call back functionality.
     * You can manually set the return response on a CompletableFuture which you cant do on Future. You can cancel it as well. 
     * You can chain & combine CompletableFutures which is not possible with Future.
     * Exception handling support in CompletableFutures which is not available in Future.
     *
     * Although chaining can be done manually but not advised to use this approach.
     * This example is for reference only.
     */
    @Test
    @SneakyThrows
    void nonBlockingChain_test() {
        counter = new AtomicInteger();
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < 5; i++) {
            int finalI = i;
            executor.submit(() -> {
                CompletableFutureTest.greetHelloChain("Jack_" + finalI, new CompletableFuture<>());
            });
        }
        //Give enough time for all threads to complete and return back with results.
        TimeUnit.SECONDS.sleep(10);
        executor.shutdown();
        Assertions.assertEquals(5, counter.get());
    }

    /**
     * When function does not return anything then use CompletableFuture.runAsync()
     * returns CompletableFuture<Void>
     */
    @Test
    @SneakyThrows
    void runAsync_test() {
        counter = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            int finalI = i;
            CompletableFuture.runAsync(() -> {
                greetHello("Jack_" + finalI);
            }).thenRun(() -> {
                counter.incrementAndGet();
                log.info("Completed!");
            });
        }
        //Give enough time for all threads to complete and return back with results.
        TimeUnit.SECONDS.sleep(5);
        Assertions.assertEquals(5, counter.get());
    }

    /**
     * Returns CompletableFuture<T>
     */
    @Test
    @SneakyThrows
    void supplyAsync_test() {
        counter = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            int finalI = i;
            CompletableFuture.supplyAsync(() -> {
                return greetHello("Jack_" + finalI);
            }).thenAccept(message -> {
                counter.incrementAndGet();
                log.info("Greeting: {}", message);
            });
        }
        //Give enough time for all threads to complete and return back with results.
        TimeUnit.SECONDS.sleep(5);
        Assertions.assertEquals(5, counter.get());
    }

    /**
     * thenApply will return a nested CompletionStage.
     * thenApply returns CompletionStage & return value of the function.
     */
    @Test
    @SneakyThrows
    void thenApply_test() {
        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            //Do some computation & return the result
            return "hello ";
        }).thenApply(message -> {
            return message + "world";
        }).thenApply(message -> {
            return message.toUpperCase();
        });
        // Returns type CompletionStage<CompletionStage<CompletionStage<String>>>.
        Assertions.assertEquals("HELLO WORLD", completableFuture.get());
    }

    /**
     * thenAccept will return a single CompletionStage, flattening effect like a flatMap
     * thenAccept takes a Consumer and returns a Void & only the completion state.
     */
    @Test
    @SneakyThrows
    void thenAccept_test() {
        counter = new AtomicInteger();
        CompletableFuture<Void> completableFuture = CompletableFuture.supplyAsync(() -> {
            //Do some computation & return the result
            return "hello world";
        }).thenAccept(message -> {
            log.info("Got Message: {}", message);
        }).thenRun(() -> {
            counter.incrementAndGet();
            log.info("Cant access previous result, just running!");
        });
        completableFuture.get();
        Assertions.assertEquals(1, counter.get());
    }

    /**
     * thenCompose() combines two futures where one future is dependent on the other
     * thenCompose will return a single CompletionStage, flattening effect like a flatMap
     */
    @Test
    @SneakyThrows
    void thenCompose_test() {
        //Notice the flattened return type. Combines 2 dependent future.
        CompletableFuture<String> completableFuture = getGreeting("Jack")
                .thenCompose(message -> CompletableFutureTest.transformMessage(message));
        Assertions.assertEquals("HELLO JACK", completableFuture.get());
    }

    /**
     * thenCombine() combines two independent futures.
     */
    @Test
    @SneakyThrows
    void thenCombine_test() {
        //Combines the 2 independent futures.
        CompletableFuture<String> completableFuture = getGreeting("Jack")
                .thenCombine(CompletableFutureTest.getCurrentDate(), (message, currentDate) -> {
                    return CompletableFutureTest.addDateToMessage(message, currentDate);
                });
        Assertions.assertTrue(completableFuture.get().contains("Hello Jack was sent on"));
    }

    @Test
    @SneakyThrows
    void exceptionally_test() {
        CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
            //Do some computation & return the result
            return "Stage 0";
        }).thenApply(result -> {
            return result + " -> Stage 1";
        }).exceptionally(ex -> {
            return "Error in stage 1 : " + ex.getMessage();
        }).thenApply(result -> {
            if (true) {
                throw new RuntimeException("My custom error!");
            }
            return result + " -> Stage 2";
        }).exceptionally(ex -> {
            return "Error in stage 2 : " + ex.getMessage();
        });
        Assertions.assertTrue(completableFuture.get().contains("Error in stage 2"));
    }

    @Test
    @SneakyThrows
    void allOf_test() {
        counter = new AtomicInteger();
        List<CompletableFuture<Void>> tasks = getListOfTasks();
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(tasks.get(0), tasks.get(1), tasks.get(2));
        allTasks.get();
        log.info("Waited for all tasks to complete and then returned!");
        Assertions.assertEquals(3, counter.get());
    }

    @Test
    @SneakyThrows
    void anyOf_test() {
        counter = new AtomicInteger();
        List<CompletableFuture<Void>> tasks = getListOfTasks();
        CompletableFuture<Object> allTasks = CompletableFuture.anyOf(tasks.get(0), tasks.get(1), tasks.get(2));
        allTasks.get();
        log.info("Waited for any one task to complete and then returned!");
        Assertions.assertTrue(counter.get() >= 1);
    }

    @Test
    @SneakyThrows
    void allOf_withTimeLimit_test() {
        counter = new AtomicInteger();
        List<CompletableFuture<Void>> tasks = getListOfTasks();
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(tasks.get(0), tasks.get(1), tasks.get(2));
        try {
            allTasks.get(4, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            log.error("timeout!", ex);
        }
        log.info("Waited for 4 seconds and returned!");
        Assertions.assertTrue(counter.get() >= 2);
    }

    @Test
    @SneakyThrows
    void allOf_iterate() {
        List<String> names = List.of("Jack", "Adam", "Ram", "Ajay");
        List<CompletableFuture<String>> customersFuture = names.stream()
                .map(userName -> checkName(userName))
                .collect(Collectors.toList());

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(customersFuture.toArray(new CompletableFuture[customersFuture.size()]));

        CompletableFuture<List<String>> allCustomersFuture = allFutures.thenApply(v -> customersFuture.stream()
                .map(pageContentFuture -> pageContentFuture.join())
                .filter(Objects::isNull)
                .collect(Collectors.toList()));

        List<String> customers = allCustomersFuture.get();
        Assertions.assertEquals(2, customers.size());
    }

    private static CompletableFuture<String> checkName(String userName) {
        return CompletableFuture.supplyAsync(() -> {
            if (userName.startsWith("A")) return userName;
            return null;
        });
    }

    private static String greetHello(String name) {
        log.info("Got name: {}", name);
        return "Hello " + name;
    }

    private static void finishedGreetHello(String result) {
        counter.incrementAndGet();
        log.info("Finished greet chain: {}", result);
    }

    private static void greetHelloChain(String name, CompletableFuture<String> completableFuture) {
        log.info("Got name: {}", name);
        completableFuture.complete("Hello " + name);
        completableFuture.whenComplete(CompletableFutureTest::finishedGreetHelloChain);
    }

    private static void finishedGreetHelloChain(String result, Throwable t) {
        counter.incrementAndGet();
        log.info("Finished greet chain: {}", result);
    }

    private static CompletableFuture<String> getGreeting(String userName) {
        return CompletableFuture.supplyAsync(() -> {
            return "Hello " + userName;
        });
    }

    private static CompletableFuture<Date> getCurrentDate() {
        return CompletableFuture.supplyAsync(() -> {
            return new Date();
        });
    }

    private static CompletableFuture<String> transformMessage(String message) {
        return CompletableFuture.supplyAsync(() -> {
            return message.toUpperCase();
        });
    }

    private static String addDateToMessage(String message, Date currentDate) {
        return message + " was sent on  " + currentDate;
    }

    //Each task is delayed by few seconds
    private static List<CompletableFuture<Void>> getListOfTasks() {
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        tasks.add(CompletableFuture.supplyAsync(() -> {
            return greetHello("Jack");
        }).thenAccept(message -> {
            counter.incrementAndGet();
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
            }
            log.info("Greeting: {}", message);
        }));
        tasks.add(CompletableFuture.supplyAsync(() -> {
            return greetHello("Raj");
        }).thenAccept(message -> {
            counter.incrementAndGet();
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
            }
            log.info("Greeting: {}", message);
        }));
        tasks.add(CompletableFuture.supplyAsync(() -> {
            return greetHello("Dan");
        }).thenAccept(message -> {
            counter.incrementAndGet();
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
            }
            log.info("Greeting: {}", message);
        }));
        return tasks;
    }

}
