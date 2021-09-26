package com.demo.project83;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class CompletableFutureTest {

    /**
     * get is blocking call. So main thread has to wait.
     */
    @Test
    @SneakyThrows
    void blocking_Test() {
        List<Future<String>> futureLst = new ArrayList<>();
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < 5; i++) {
            Future<String> future = executor.submit(CompletableFutureTest::processingJob1);
            futureLst.add(future);
        }
        for (Future<String> future : futureLst) {
            log.info(future.get() + " " + future.isDone());
        }
        executor.shutdown();
    }

    /**
     * callback attached so non blocking.
     *
     * Ability to provide call back functionality.
     * You can manually set the return response on a CompletableFuture which you cant do on Future. You can cancel it as well. 
     * You can chain & combine CompletableFutures which is not possible with Future.
     * Exception handling support in CompletableFutures which is not available in Future. 
     */
    @Test
    @SneakyThrows
    void nonBlocking_Callback_Test() {
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                CompletableFutureTest.processingJob2(new CompletableFuture<>());
            });
        }
        //Give enough time for all threads to complete and return back with results.
        TimeUnit.SECONDS.sleep(15);
        executor.shutdown();

    }


    /**
     * Does not return anything then use CompletableFuture.runAsync()
     */
    @Test
    @SneakyThrows
    void runAsync_Test() {
        for (int i = 0; i < 5; i++) {
            CompletableFuture.runAsync(() -> {
                CompletableFutureTest.processingJob2(new CompletableFuture<>());
            });
        }
        //Give enough time for all threads to complete and return back with results.
        TimeUnit.SECONDS.sleep(15);
    }

    /**
     * thenApply will return a nested CompletionStage.
     * thenAccept will return a single CompletionStage, flattening effect like a flatMap
     */
    @Test
    @SneakyThrows
    void thenApply_test() {
        CompletableFuture<String> completableFuture1 = CompletableFuture.supplyAsync(() -> {
            //Do some computation & return the result
            return "hello ";
        }).thenApply(message -> {
            return message + " world";
        }).thenApply(message -> {
            return message.toUpperCase();
        });
        // Returns type CompletionStage<CompletionStage<CompletionStage<String>>>.
        log.info(completableFuture1.get());

    }

    /**
     * thenAccept will return a single CompletionStage, flattening effect like a flatMap
     */
    @Test
    @SneakyThrows
    void thenAccept_test() {
        CompletableFuture<Void> completableFuture2 = CompletableFuture.supplyAsync(() -> {
            //Do some computation & return the result
            return "hello world";
        }).thenAccept(message -> {
            log.info("Got Message: {}", message);
        }).thenRun(() -> {
            log.info("Cant access previous result, just running!");
        });
        completableFuture2.get();
    }

    /**
     * thenCompose() combines two futures where one future is dependent on the other
     * thenCompose will return a single CompletionStage, flattening effect like a flatMap
     */
    @Test
    @SneakyThrows
    void thenCompose_test() {
        //Notice the flattened return type. Combines 2 dependent future.
        CompletableFuture<String> completableFuture = CompletableFutureTest.getGreeting("Jack")
                .thenCompose(message -> CompletableFutureTest.transalateMessage(message));
        log.info("Size of greeting: {}", completableFuture.get());
    }

    /**
     * thenCombine() combines two independent futures.
     */
    @Test
    @SneakyThrows
    void thenCombine_test() {
        //Combines the 2 independent futures.
        CompletableFuture<String> completableFuture = CompletableFutureTest.getGreeting("Jack")
                .thenCombine(CompletableFutureTest.getCurrentDate(), (message, currentDate) -> {
                    return CompletableFutureTest.updateLogTable(message, currentDate);
                });
        log.info(completableFuture.get());
    }

    @Test
    @SneakyThrows
    void exceptionally_test() {
        CompletableFuture<String> completableFuture1 = CompletableFuture.supplyAsync(() -> {
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
        log.info("Got Message: {}", completableFuture1.get());
    }

    @Test
    @SneakyThrows
    void allOf_test() {
        CompletableFuture<Void> task1 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });
        CompletableFuture<Void> task2 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });
        CompletableFuture<Void> task3 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });

        CompletableFuture<Void> allTasks = CompletableFuture.allOf(task1, task2, task3);
        allTasks.get();
        log.info("Waited for all tasks to complete and then returned!");
    }

    @Test
    @SneakyThrows
    void anyOf_test() {
        CompletableFuture<Void> task1 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });
        CompletableFuture<Void> task2 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });
        CompletableFuture<Void> task3 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });

        CompletableFuture<Object> allTasks = CompletableFuture.anyOf(task1, task2, task3);
        allTasks.get();
        log.info("Waited for any one task to complete and then returned!");
    }

    @Test
    @SneakyThrows
    void allOf_withTimeLimit_test() {
        CompletableFuture<Void> task1 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });
        CompletableFuture<Void> task2 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });
        CompletableFuture<Void> task3 = CompletableFuture.runAsync(() -> {
            CompletableFutureTest.processingJob2(new CompletableFuture<>());
        });

        CompletableFuture<Void> allTasks = CompletableFuture.allOf(task1, task2, task3);
        try {
            allTasks.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            //Do Nothing!
        }
        log.info("Waited for 3 seconds and returned!");
    }


    private static String processingJob1() {
        try {
            Integer randomSleep = new Random().nextInt(10);
            log.info("Running Thread: " + Thread.currentThread().getName());
            TimeUnit.SECONDS.sleep(randomSleep);
            return "Completed Thread: " + Thread.currentThread().getName();
        } catch (InterruptedException ex) {
            //No action
        }
        return "Failed!";
    }

    private static void processingJob2(CompletableFuture<String> completableFuture) {
        try {
            Integer randomSleep = new Random().nextInt(10);
            log.info("Running Thread: " + Thread.currentThread().getName());
            TimeUnit.SECONDS.sleep(randomSleep);
            completableFuture.complete("Completed Thread: " + Thread.currentThread().getName());
            completableFuture.whenComplete(CompletableFutureTest::finishedRunningJob);
        } catch (InterruptedException ex) {
            //No action.
        }
    }

    private static void finishedRunningJob(String result, Throwable t) {
        log.info("Got Result: {}", result);
    }

    //CompletableFuture with different return types.
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

    private static CompletableFuture<String> transalateMessage(String message) {
        return CompletableFuture.supplyAsync(() -> {
            return message.replaceAll("Hello", "Aloha");
        });
    }

    private static String updateLogTable(String message, Date currentDate) {
        return message + " was sent on  " + currentDate;
    }

}
