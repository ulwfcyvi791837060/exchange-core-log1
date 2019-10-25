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

import com.google.common.collect.Lists;
import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.*;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.api.binary.BatchAddAccountsCommand;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.reports.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.orderbook.OrderBookFastImpl;
import exchange.core2.core.processors.journalling.DiskSerializationProcessor;
import exchange.core2.core.processors.journalling.JournallingProcessor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.hamcrest.core.Is;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static exchange.core2.core.utils.UnsafeUtils.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
@Slf4j
public final class ExchangeTestContainer implements AutoCloseable {

    static final int RING_BUFFER_SIZE_DEFAULT = 64 * 1024;
    static final int RISK_ENGINES_ONE = 1;
    static final int MATCHING_ENGINES_ONE = 1;
    static final int MGS_IN_GROUP_LIMIT_DEFAULT = 128;


    public final ExchangeCore exchangeCore;
    public final ExchangeApi api;

    @Setter
    private Consumer<OrderCommand> consumer = cmd -> {
    };

    public static final Consumer<OrderCommand> CHECK_SUCCESS = cmd -> assertEquals(CommandResultCode.SUCCESS, cmd.resultCode);

    public ExchangeTestContainer() {
        this(RING_BUFFER_SIZE_DEFAULT, MATCHING_ENGINES_ONE, RISK_ENGINES_ONE, MGS_IN_GROUP_LIMIT_DEFAULT, null);
    }

    /**
     * 构造方法
     * @Author zenghuikang
     * @Description 
     * @Date 2019/10/21 10:18
      * @param bufferSize
     * @param matchingEnginesNum
     * @param riskEnginesNum
     * @param msgsInGroupLimit
     * @param stateId
     * @return 
     * @throws 
     **/
    public ExchangeTestContainer(final int bufferSize,
                                 final int matchingEnginesNum,
                                 final int riskEnginesNum,
                                 final int msgsInGroupLimit,
                                 final Long stateId) {

        this.exchangeCore = ExchangeCore.builder()
                //以下表达式相当于往构造方法传参数
                .resultsConsumer((cmd, seq) -> consumer.accept(cmd)) //暂时是最后的disruptor 节点处理器 对给定的参数执行此操作。
                //.serializationProcessor(new DiskSerializationProcessor("./dumps")) linux
                .serializationProcessor(new DiskSerializationProcessor("dumps")) //windows ul

                //ul add JournallingProcessor 加上这个每秒只能处理700个命令
                //.journallingHandler(new JournallingProcessor())
                .ringBufferSize(bufferSize)
                .matchingEnginesNum(matchingEnginesNum) //1
                .riskEnginesNum(riskEnginesNum) //1
                .msgsInGroupLimit(msgsInGroupLimit) //1536
                .threadAffinityMode(THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE)
                .waitStrategy(CoreWaitStrategy.BUSY_SPIN)
                .orderBookFactory(symbolType -> new OrderBookFastImpl(OrderBookFastImpl.DEFAULT_HOT_WIDTH, symbolType))
//                .orderBookFactory(OrderBookNaiveImpl::new)
                .loadStateId(stateId) // Loading from persisted state
                .build();

        //启动disruptor
        this.exchangeCore.startup();
        api = this.exchangeCore.getApi();
    }

//    public ExchangeTestContainer(final ExchangeCore exchangeCore) {
//
//        this.exchangeCore = exchangeCore;
//        this.exchangeCore.startup();
//        api = this.exchangeCore.getApi();
//    }


    public void initBasicSymbols() {

        addSymbol(TestConstants.SYMBOLSPEC_EUR_USD);
        addSymbol(TestConstants.SYMBOLSPEC_ETH_XBT);
    }

    public void initFeeSymbols() {

        addSymbol(TestConstants.SYMBOLSPECFEE_XBT_LTC);
    }

    public void initBasicUsers() throws InterruptedException {

        final List<ApiCommand> cmds = new ArrayList<>();

        cmds.add(ApiAddUser.builder().uid(TestConstants.UID_1).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_1).transactionId(1L).amount(10_000_00L).currency(TestConstants.CURRENECY_USD).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_1).transactionId(2L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_XBT).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_1).transactionId(3L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_ETH).build());

        cmds.add(ApiAddUser.builder().uid(TestConstants.UID_2).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_2).transactionId(1L).amount(20_000_00L).currency(TestConstants.CURRENECY_USD).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_2).transactionId(2L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_XBT).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(TestConstants.UID_2).transactionId(3L).amount(1_0000_0000L).currency(TestConstants.CURRENECY_ETH).build());

        submitCommandsSync(cmds);
    }

    public void createUserWithMoney(long uid, int currency, long amount) throws InterruptedException {
        final List<ApiCommand> cmds = new ArrayList<>();
        cmds.add(ApiAddUser.builder().uid(uid).build());
        cmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(getRandomTransactionId()).amount(amount).currency(currency).build());
        submitCommandsSync(cmds);
    }

    public void addMoneyToUser(long uid, int currency, long amount) throws InterruptedException {
        final List<ApiCommand> cmds = new ArrayList<>();
        cmds.add(ApiAdjustUserBalance.builder().uid(uid).transactionId(getRandomTransactionId()).amount(amount).currency(currency).build());
        submitCommandsSync(cmds);
    }


    public void addSymbol(final CoreSymbolSpecification symbol) {
        addSymbols(new BatchAddSymbolsCommand(symbol));
    }

    public void addSymbols(final List<CoreSymbolSpecification> symbols) {
        Lists.partition(symbols, 1024).forEach(partition -> addSymbols(new BatchAddSymbolsCommand(partition)));
    }

    public void addSymbols(final BatchAddSymbolsCommand symbols) {
        submitMultiCommandSync(ApiBinaryDataCommand.builder().transferId(getRandomTransactionId()).data(symbols).build());
    }

    private int getRandomTransactionId() {
        return (int) (System.nanoTime() & Integer.MAX_VALUE);
    }

    /**
     * 用户帐户初始化
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 17:44
      * @param userCurrencies
     * @return org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap
     * @throws
     **/
    public final IntLongHashMap userAccountsInit(List<BitSet> userCurrencies) throws InterruptedException {

        final int totalAccounts = userCurrencies.stream().skip(1).mapToInt(BitSet::cardinality).sum();
        final int numUsers = userCurrencies.size() - 1;
        final CountDownLatch usersLatch = new CountDownLatch(totalAccounts + numUsers);

        //消费者
        consumer = cmd -> {
            if (cmd.resultCode == CommandResultCode.SUCCESS
                    && (cmd.command == OrderCommandType.ADD_USER ||
                    //平衡调整
                    cmd.command == OrderCommandType.BALANCE_ADJUSTMENT)) {
                // 减少锁存器的计数，如果计数达到零，则释放所有等待线程。totalAccounts + numUsers
                usersLatch.countDown();
            } else {
                throw new IllegalStateException("Unexpected command" + cmd);
            }
        };

        final long amountToAdd = 1_000_000_000_000L;

        IntStream.rangeClosed(1, numUsers).forEach(uid -> {
            api.submitCommand(ApiAddUser.builder().uid(uid).build());
            userCurrencies.get(uid).stream().forEach(currency ->
                    api.submitCommand(ApiAdjustUserBalance.builder().uid(uid).transactionId(uid * 1000 + currency).amount(amountToAdd).currency(currency).build()));

//            if (uid > 1000000 && uid % 1000000 == 0) {
//                log.debug("uid: {} usersLatch: {}", uid, usersLatch.getCount());
//            }
        });

        //减少锁存器的计数，如果计数达到零，则释放所有等待线程。totalAccounts + numUsers
        usersLatch.await();

        consumer = cmd -> {
        };

        final IntLongHashMap globalAmountPerCurrency = new IntLongHashMap();
        userCurrencies.forEach(user -> user.stream().forEach(cur -> globalAmountPerCurrency.addToValue(cur, amountToAdd)));
        return globalAmountPerCurrency;
    }

    public void usersInit(int numUsers, Set<Integer> currencies) throws InterruptedException {

        int totalCommands = numUsers * (1 + currencies.size());
        final CountDownLatch usersLatch = new CountDownLatch(totalCommands);
        consumer = cmd -> {
            if (cmd.resultCode == CommandResultCode.SUCCESS
                    && (cmd.command == OrderCommandType.ADD_USER || cmd.command == OrderCommandType.BALANCE_ADJUSTMENT)) {
                usersLatch.countDown();
            } else {
                throw new IllegalStateException("Unexpected command" + cmd);
            }
        };


        LongStream.rangeClosed(1, numUsers)
                .forEach(uid -> {
                    api.submitCommand(ApiAddUser.builder().uid(uid).build());
                    currencies.forEach(currency -> {
                        int transactionId = currency;
                        api.submitCommand(ApiAdjustUserBalance.builder().uid(uid).transactionId(transactionId).amount(10_0000_0000L).currency(currency).build());
                    });
                    if (uid > 1000000 && uid % 1000000 == 0) {
                        log.debug("uid: {} usersLatch: {}", uid, usersLatch.getCount());
                    }
                });
        usersLatch.await();

        consumer = cmd -> {
        };

    }

    // TODO slow (due allocations)
    public void usersInitBatch(int numUsers, Set<Integer> currencies) {
        int fromUid = 0;
        final int batchSize = 1024;
        while (usersInitBatch(fromUid, Math.min(fromUid + batchSize, numUsers + 1), currencies)) {
            fromUid += batchSize;
        }
    }

    public boolean usersInitBatch(int uidStartIncl, int uidStartExcl, Set<Integer> currencies) {

        if (uidStartIncl > uidStartExcl) {
            return false;
        }

        final LongObjectHashMap<IntLongHashMap> users = new LongObjectHashMap<>();
        for (int uid = uidStartIncl; uid < uidStartExcl; uid++) {
            final IntLongHashMap accounts = new IntLongHashMap();
            currencies.forEach(currency -> accounts.put(currency, 10_0000_0000L));
            users.put(uid, accounts);
            if (uid > 100000 && uid % 100000 == 0) {
                log.debug("uid: {}", uid);
            }
        }
        submitMultiCommandSync(ApiBinaryDataCommand.builder().transferId(getRandomTransactionId()).data(new BatchAddAccountsCommand(users)).build());

        return true;
    }


    public void resetExchangeCore() throws InterruptedException {
        //同步提交命令
        submitCommandSync(ApiReset.builder().build(), CHECK_SUCCESS);
    }

    public void submitCommandSync(ApiCommand apiCommand, CommandResultCode expectedResultCode) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        consumer = cmd -> {
            //expectedResultCode 预期结果代码
            assertThat(cmd.resultCode, Is.is(expectedResultCode));
            latch.countDown();
        };
        api.submitCommand(apiCommand);
        latch.await();
        consumer = cmd -> {
        };
    }


    /**
     * 同步提交命令
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 14:16
      * @param apiCommand
     * @param validator
     * @return void
     * @throws
     **/
    public void submitCommandSync(ApiCommand apiCommand, Consumer<OrderCommand> validator) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        consumer = cmd -> {
            validator.accept(cmd);
            latch.countDown();
        };
        api.submitCommand(apiCommand);
        latch.await();
        consumer = cmd -> {
        };
    }

    public void submitMultiCommandSync(ApiCommand dataCommand) {
        final CountDownLatch latch = new CountDownLatch(1);
        consumer = cmd -> {
            if (cmd.command != OrderCommandType.BINARY_DATA
                    && cmd.command != OrderCommandType.PERSIST_STATE_RISK
                    && cmd.command != OrderCommandType.PERSIST_STATE_MATCHING) {
                throw new IllegalStateException("Unexpected command");
            }
            if (cmd.resultCode == CommandResultCode.SUCCESS) {
                latch.countDown();
            } else if (cmd.resultCode != CommandResultCode.ACCEPTED) {
                //throw new IllegalStateException("Unexpected result code");
            }
        };
        api.submitCommand(dataCommand);
        try {
            latch.await();
        } catch (InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
        consumer = cmd -> {
        };
    }


    void submitCommandsSync(List<ApiCommand> apiCommand) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(apiCommand.size());
        consumer = cmd -> {
            assertEquals(CommandResultCode.SUCCESS, cmd.resultCode);
            latch.countDown();
        };
        apiCommand.forEach(api::submitCommand);
        latch.await();
        consumer = cmd -> {
        };
    }

    public L2MarketData requestCurrentOrderBook(final int symbol) {
        BlockingQueue<OrderCommand> queue = attachNewConsumerQueue();
        api.submitCommand(ApiOrderBookRequest.builder().symbol(symbol).size(-1).build());
        OrderCommand orderBookCmd = waitForOrderCommands(queue, 1).get(0);
        L2MarketData actualState = orderBookCmd.marketData;
        assertNotNull(actualState);
        return actualState;
    }

    BlockingQueue<OrderCommand> attachNewConsumerQueue() {
        final BlockingQueue<OrderCommand> results = new LinkedBlockingQueue<>();
        consumer = cmd -> results.add(cmd.copy());
        return results;
    }

    List<OrderCommand> waitForOrderCommands(BlockingQueue<OrderCommand> results, int c) {
        return Stream.generate(() -> {
            try {
                return results.poll(10000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                throw new IllegalStateException();
            }
        })
                .limit(c)
                .collect(Collectors.toList());
    }










    /**
     * 产生随机符号方法
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 18:22
      * @param num
     * @param currenciesAllowed
     * @param allowedSymbolTypes
     * @return java.util.List<exchange.core2.core.common.CoreSymbolSpecification>
     * @throws
     **/
    public static List<CoreSymbolSpecification> generateRandomSymbols(final int num,
                                                                      final Collection<Integer> currenciesAllowed,
                                                                      final AllowedSymbolTypes allowedSymbolTypes) {
        final Random random = new Random(1L);

        final Supplier<SymbolType> symbolTypeSupplier;

        switch (allowedSymbolTypes) {
            case FUTURES_CONTRACT:
                symbolTypeSupplier = () -> SymbolType.FUTURES_CONTRACT;
                break;

            case CURRENCY_EXCHANGE_PAIR:
                symbolTypeSupplier = () -> SymbolType.CURRENCY_EXCHANGE_PAIR;
                break;

            case BOTH:
            default:
                symbolTypeSupplier = () -> random.nextBoolean() ? SymbolType.FUTURES_CONTRACT : SymbolType.CURRENCY_EXCHANGE_PAIR;
                break;
        }

        final List<Integer> currencies = new ArrayList<>(currenciesAllowed);
        final List<CoreSymbolSpecification> result = new ArrayList<>();
        for (int i = 0; i < num; ) {
            int baseCurrency = currencies.get(random.nextInt(currencies.size()));
            int quoteCurrency = currencies.get(random.nextInt(currencies.size()));
            if (baseCurrency != quoteCurrency) {
                final SymbolType type = symbolTypeSupplier.get();
                final long makerFee = random.nextInt(1000);
                final long takerFee = makerFee + random.nextInt(500);
                //核心符号规范
                final CoreSymbolSpecification symbol = CoreSymbolSpecification.builder()
                        .symbolId(TestConstants.SYMBOL_AUTOGENERATED_RANGE_START + i)
                        .type(type)
                        //todo 期货可以是任何价值
                        .baseCurrency(baseCurrency) // TODO for futures can be any value
                        .quoteCurrency(quoteCurrency)
                        .baseScaleK(100)
                        .quoteScaleK(10)
                        .takerFee(takerFee)
                        .makerFee(makerFee)
                        .build();

                result.add(symbol);

                //log.debug("{}", symbol);
                i++;
            }
        }
        return result;
    }

    @Override
    public void close() {
        exchangeCore.shutdown();
    }

    public enum AllowedSymbolTypes {
        //期货合约
        FUTURES_CONTRACT,
        //货币兑换对
        CURRENCY_EXCHANGE_PAIR,
        //二者
        BOTH
    }
}
