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
package exchange.core2.core;

import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.CoreWaitStrategy;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.dto.RequestDto;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.processors.*;
import exchange.core2.core.processors.journalling.ISerializationProcessor;
import exchange.core2.core.processors.journalling.JournallingProcessor;
import exchange.core2.core.utils.UnsafeUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;
import java.util.function.ObjLongConsumer;

@Slf4j
public final class ExchangeCore {

    private final Disruptor<OrderCommand> disruptor;

    private final ExchangeApi api;

    // core can be started and stopped only once
    private boolean started = false;
    private boolean stopped = false;

    /*private RequestDtoEventBusinessHandler requestDtoEventBusinessHandler() {
        RequestDtoEventBusinessHandler requestDtoEventBusinessHandler = new RequestDtoEventBusinessHandler();
        requestDtoEventBusinessHandler.setItemRepository(applicationContext.getBean(ItemRepository.class));
        return requestDtoEventBusinessHandler;
    }*/

    /**
     * 使用@Builder来进行对象赋值，我们直接在类上加@Builder之后，我们的继承就被无情的屏蔽了，这主要是由于构造方法与父类冲突的问题导致的，
     * 事实上，我们可以把@Builder注解加到子类的全参构造方法上就可以了！
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/22 9:31
      * @param resultsConsumer  ExchangeTestContainer 传过来  接受一个object类型和一个long类型的输入参数，无返回值。
     * @param journallingHandler
     * @param serializationProcessor
     * @param ringBufferSize
     * @param matchingEnginesNum
     * @param riskEnginesNum
     * @param msgsInGroupLimit
     * @param threadAffinityMode
     * @param waitStrategy
     * @param orderBookFactory
     * @param loadStateId
     * @return
     * @throws
     **/
    @Builder
    public ExchangeCore(final ObjLongConsumer<OrderCommand> resultsConsumer,
                        final JournallingProcessor journallingHandler,
                        final ISerializationProcessor serializationProcessor,
                        final int ringBufferSize,
                        final int matchingEnginesNum,
                        final int riskEnginesNum,
                        final int msgsInGroupLimit,
                        final UnsafeUtils.ThreadAffinityMode threadAffinityMode,
                        final CoreWaitStrategy waitStrategy,
                        final Function<CoreSymbolSpecification, IOrderBook> orderBookFactory,
                        final Long loadStateId) {

        //全项目就一个圆
        this.disruptor = new Disruptor<>(
                OrderCommand::new,
                ringBufferSize,
                UnsafeUtils.affinedThreadFactory(threadAffinityMode),
                // 多个网关线程正在写入
                // multiple gateway threads are writing
                ProducerType.MULTI,
                waitStrategy.create());

        this.api = new ExchangeApi(disruptor.getRingBuffer());

        // 创建和附加异常处理程序
        // creating and attaching exceptions handler
        final DisruptorExceptionHandler<OrderCommand> exceptionHandler = new DisruptorExceptionHandler<>("main", (ex, seq) -> {
            log.error("Exception thrown on sequence={}", seq, ex);
            // TODO 重新发布时抛出异常
            // TODO re-throw exception on publishing
            disruptor.getRingBuffer().publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
        });

        disruptor.setDefaultExceptionHandler(exceptionHandler);




        // [日志（J）]与风险保留（R1）+匹配引擎（ME）并行 start
        // 2. [journalling (J)] in parallel with risk hold (R1) + matching engine (ME)
        /*if (journallingHandler != null) {
            //afterGrouping后顺序执行
            afterGrouping.handleEventsWith(journallingHandler::onEvent);
        }*/

        // 解码处理
        EventHandler<OrderCommand> unmarshallHandler = new EventHandler() { // 最慢
            @Override
            public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
                log.debug("ThreadId: {} process unmarshall: {} seq: {}", Thread.currentThread().getId(), event,sequence);
                //System.out.println(Thread.currentThread().getId() + " process unmarshall " + event + ", seq: " + sequence);
                /*String msg = new String(((OrderCommand)event).getRequestDto().getPayLoad());
                RequestDto requestDto = JSONObject.parseObject(msg, RequestDto.class);
                ((OrderCommand)event).getRequestDto().setItemId(requestDto.getItemId());
                ((OrderCommand)event).getRequestDto().setUserId(requestDto.getUserId());*/
            }
        };

    /*//消费者 消费者 RequestDtoEvent
    // 定义消费链，先并行处理日志、解码和复制，再处理结果上报
    disruptor
            .handleEventsWith(unmarshallHandler)
            .then(requestDtoEventBusinessHandler())
            .then(requestDtoEventJmsOutputer, requestDtoEventDbOutputer)
            //置null
            .then(new RequestDtoEventGcHandler());*/

        //RequestDtoEventBusinessHandler requestDtoEventBusinessHandler = requestDtoEventBusinessHandler();

        // 定义消费链，先并行处理日志、解码和复制，再处理结果上报
        //消费者 消费者 RequestDtoEvent
        disruptor
                .handleEventsWith(unmarshallHandler);
                //.then(requestDtoEventBusinessHandler);

        disruptor.after(unmarshallHandler)
                 .handleEventsWith((cmd, seq, eob) -> {
                    /**
                     * 函数式接口,有一个抽象方法，会被lambda表达式的定义所覆盖。
                     * 处理两个两个参数,且第二个参数必须为long类型
                     **/
                    resultsConsumer.accept(cmd, seq);
                    // TODO 待办事项缓慢？（易失性操作）
                    // TODO SLOW ?(volatile operations)
                    api.processResult(seq, cmd);
                });


    }

    public synchronized void startup() {
        if (!started) {
            log.debug("Starting disruptor...");
            disruptor.start();
            started = true;
        }
    }

    public ExchangeApi getApi() {
        return api;
    }

    private static final EventTranslator<OrderCommand> SHUTDOWN_SIGNAL_TRANSLATOR = (cmd, seq) -> {
        cmd.command = OrderCommandType.SHUTDOWN_SIGNAL; //关机信号
        cmd.resultCode = CommandResultCode.NEW;
    };

    public synchronized void shutdown() {
        if (!stopped) {
            stopped = true;
            // TODO 首先停止接受新事件
            // TODO stop accepting new events first
            log.info("Shutdown disruptor...");
            disruptor.getRingBuffer().publishEvent(SHUTDOWN_SIGNAL_TRANSLATOR);
            disruptor.shutdown();
            log.info("Disruptor stopped");
        }
    }

    @SuppressWarnings(value = {"unchecked"})
    private static EventHandler<OrderCommand>[] newEventHandlersArray(int size) {
        return new EventHandler[size];
    }
}
