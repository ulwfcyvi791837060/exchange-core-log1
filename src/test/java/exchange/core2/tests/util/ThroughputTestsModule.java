/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.tests.util;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.api.ApiPlaceOrder;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.AffinityLock;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static exchange.core2.core.common.OrderAction.ASK;
import static exchange.core2.core.common.OrderType.GTC;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

@Slf4j
public class ThroughputTestsModule {


    /**
     * 由性能吞吐量 PerfThroughput 这个类调用
     * 吞吐量测试Impl
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 10:26
      * @param container 容器
     * @param totalTransactionsNumber 总交易数
     * @param targetOrderBookOrdersTotal 目标订单簿订单总数
     * @param numAccounts 数量帐户
     * @param iterations
     * @param currenciesAllowed 允许的货币
     * @param numSymbols
     * @param allowedSymbolTypes 允许的符号类型
     * @return void
     * @throws
     **/
    public static void throughputTestImpl(final ExchangeTestContainer container,
                                          final int totalTransactionsNumber,
                                          final int targetOrderBookOrdersTotal,
                                          final int numAccounts,
                                          final int iterations,
                                          final Set<Integer> currenciesAllowed,
                                          final int numSymbols,
                                          final ExchangeTestContainer.AllowedSymbolTypes allowedSymbolTypes) throws Exception {

        try (
                //假设我们现在有一个Java进程在运行，而我们希望将它绑定到某个特定的CPU上：
                //AffinityLock al = AffinityLock.acquireLock();
                final AffinityLock cpuLock = AffinityLock.acquireLock()) {

            //ExchangeTestContainer 构造方法里赋值
            final ExchangeApi api = container.api;

            //产生随机符号 一个符号
            final List<CoreSymbolSpecification> coreSymbolSpecifications = ExchangeTestContainer.generateRandomSymbols(numSymbols, currenciesAllowed, allowedSymbolTypes);

            //产生用户 2K货币帐户
            final List<BitSet> usersAccounts = UserCurrencyAccountsGenerator.generateUsers(numAccounts, currenciesAllowed);

            //产生多个符号 TestOrdersGenerator
            final TestOrdersGenerator.MultiSymbolGenResult genResult = TestOrdersGenerator.generateMultipleSymbols(
                    // 核心符号规格
                    coreSymbolSpecifications,
                    // 总交易数
                    totalTransactionsNumber,
                    //用户帐号
                    usersAccounts,
                    //1K待定限价单
                    targetOrderBookOrdersTotal);

            final CountDownLatch latchFill = new CountDownLatch(genResult.getApiCommandsFill().size());

            container.setConsumer(cmd -> latchFill.countDown()); //latchFill.countDown() 暂时将在最后一个节点的消费者真正执行
            //提交命令 api.submitCommand()
            genResult.getApiCommandsFill().forEach(api::submitCommand); //生产者
            //等待初始化完成
            latchFill.await();

            //闩锁基准
            final CountDownLatch latchBenchmark = new CountDownLatch(genResult.getApiCommandsBenchmark().size());
            container.setConsumer(cmd -> latchBenchmark.countDown()); //暂时将在最后一个节点的消费者真正执行

            //开始计时
            long t = System.currentTimeMillis();
            genResult.getApiCommandsBenchmark().forEach(api::submitCommand);
            //等待执行完成
            latchBenchmark.await();
            //结束计时
            t = System.currentTimeMillis() - t;

            float perfMt = (float) genResult.getApiCommandsBenchmark().size() / (float) t / 1000.0f;
            log.info(" {} MT/s  共" + (float) genResult.getApiCommandsBenchmark().size() +"条命令", String.format("%.3f", perfMt));
            log.info("每秒{}万次 M为6位 万为4位 ", perfMt*100);
            log.info("用时{} ms ", t);

            //不打日志 每秒1161.5977万次
            //只打日志 每秒3.0832531万次

            /*2019-10-25/18:20:13.685/CST [main] INFO  e.c.tests.util.ThroughputTestsModule -  0.032 MT/s  共998974.0条命令
            2019-10-25/18:20:13.685/CST [main] INFO  e.c.tests.util.ThroughputTestsModule - 每秒3.2264519万次 M为6位 万为4位
            2019-10-25/18:20:13.685/CST [main] INFO  e.c.tests.util.ThroughputTestsModule - 用时30962 ms*/

        }
    }



   /* final int numThreads = runnables.size();
    final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
    final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
            try {
        final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
        final CountDownLatch afterInitBlocker = new CountDownLatch(1);
        final CountDownLatch allDone = new CountDownLatch(numThreads);
        for (final Runnable submittedTestRunnable : runnables) {
            threadPool.submit(new Runnable() {
                public void run() {
                    allExecutorThreadsReady.countDown();
                    try {
                        afterInitBlocker.await();
                        submittedTestRunnable.run();
                    } catch (final Throwable e) {
                        exceptions.add(e);
                    } finally {
                        allDone.countDown();
                    }
                }
            });
        }
        // wait until all threads are ready
        assertTrue("Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent",
                allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));

        //开始计时
        long t = System.currentTimeMillis();
        // start all test runners
        afterInitBlocker.countDown();

        //等待闩锁基准
        allDone.await();
        //结束计时
        t = System.currentTimeMillis() - t;

        float perfMt = (float) 10000 / (float) t / 1000.0f;
        log.info(" {} MT/s",  String.format("%.3f", perfMt));

    } finally {
        threadPool.shutdownNow();
    }*/

    public static void assertConcurrent(final String message, final List<? extends Runnable> runnables, final int maxTimeoutSeconds) throws InterruptedException {
        final int numThreads = runnables.size();
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            final CountDownLatch allDone = new CountDownLatch(numThreads);
            for (final Runnable submittedTestRunnable : runnables) {
                threadPool.submit(new Runnable() {
                    public void run() {
                        allExecutorThreadsReady.countDown();
                        try {
                            afterInitBlocker.await();
                            submittedTestRunnable.run();
                        } catch (final Throwable e) {
                            exceptions.add(e);
                        } finally {
                            allDone.countDown();
                        }
                    }
                });
            }
            // wait until all threads are ready
            assertTrue("Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent",
                    allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));

            // start all test runners
            afterInitBlocker.countDown();
            assertTrue(message +" timeout! More than " + maxTimeoutSeconds + " seconds", allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS));
        } finally {
            threadPool.shutdownNow();
        }
        assertTrue(message + " failed with exception(s) " + exceptions, exceptions.isEmpty());
    }

}
