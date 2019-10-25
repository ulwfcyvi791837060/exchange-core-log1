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

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.api.reports.ReportQuery;
import exchange.core2.core.common.api.reports.ReportResult;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.dto.RequestDto;
import exchange.core2.core.orderbook.OrderBookEventsHelper;
import exchange.core2.core.processors.BinaryCommandsProcessor;
import exchange.core2.core.utils.SerializationUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.Wire;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

@Slf4j
public final class ExchangeApi {

    private final RingBuffer<OrderCommand> ringBuffer;

    // 承诺缓存 (TODO 可以更改为排队)
    // promises cache (TODO can be changed to queue)
    private final Map<Long, Consumer<OrderCommand>> promises = new ConcurrentHashMap<>();

    public ExchangeApi(RingBuffer<OrderCommand> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void processResult(final long seq, final OrderCommand cmd) {
        final Consumer<OrderCommand> consumer = promises.remove(seq);
        if (consumer != null) {
            consumer.accept(cmd);
        }
    }

    /**
     * 提交命令
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 9:50
      * @param cmd
     * @return void
     * @throws
     **/
    public void submitCommand(ApiCommand cmd) {
        //log.debug("{}", cmd);

        // TODO 性能基准实例
        // TODO benchmark instanceof performance

        if (cmd instanceof ApiPlaceOrder) {
            //Api下单
            //发布事件 下单
            ringBuffer.publishEvent(NEW_ORDER_TRANSLATOR, (ApiPlaceOrder) cmd);
        } else if (cmd instanceof ApiCancelOrder) {
            //发布事件
            ringBuffer.publishEvent(CANCEL_ORDER_TRANSLATOR, (ApiCancelOrder) cmd);
        } else if (cmd instanceof ApiMoveOrder) {
            //发布事件
            ringBuffer.publishEvent(MOVE_ORDER_TRANSLATOR, (ApiMoveOrder) cmd);
        } else{
            throw new IllegalArgumentException("不支持的命令类型 Unsupported command type: " + cmd.getClass().getSimpleName());
        }
    }



    /**
     * 定义 事件翻译器一 新订单
     * Disruptor3.0提供了一种富Lambda风格的API，旨在帮助开发者屏蔽直接操作RingBuffer的复杂性，所以3.0以上版本发布消息更好的办法是通过事件发布者(Event Publisher)或事件翻译器(Event Translator)API。
     * private static final EventTranslatorOneArg<LongEvent, ByteBuffer> TRANSLATOR =
     *         new EventTranslatorOneArg<LongEvent, ByteBuffer>()
     *         {
     *             public void translateTo(LongEvent event, long sequence, ByteBuffer bb)
     *             {
     *                 event.set(bb.getLong(0));
     *             }
     *         };
     *
     *     public void onData(ByteBuffer bb)
     *     {
     *         ringBuffer.publishEvent(TRANSLATOR, bb);
     *     }
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 9:29
      * @param api 相当于 ByteBuffer
      * @param cmd 相当于 LongEvent
     * @return
     * @throws
     **/
    private static final EventTranslatorOneArg<OrderCommand, ApiPlaceOrder> NEW_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.PLACE_ORDER;
        cmd.requestDto = api.requestDto;
        cmd.resultCode = CommandResultCode.NEW;
    };


    private static final EventTranslatorOneArg<OrderCommand, ApiMoveOrder> MOVE_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.MOVE_ORDER;
        cmd.price = api.newPrice;
        //cmd.price2
        cmd.orderId = api.id;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };

    private static final EventTranslatorOneArg<OrderCommand, ApiCancelOrder> CANCEL_ORDER_TRANSLATOR = (cmd, seq, api) -> {
        cmd.command = OrderCommandType.CANCEL_ORDER;
        cmd.orderId = api.id;
        cmd.price = -1;
        cmd.size = -1;
        cmd.symbol = api.symbol;
        cmd.uid = api.uid;
        cmd.timestamp = api.timestamp;
        cmd.resultCode = CommandResultCode.NEW;
    };



    /**
     * 下新订单
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/21 9:24
      * @param userCookie
     * @param price
     * @param reservedBidPrice
     * @param size
     * @param action
     * @param orderType
     * @param symbol
     * @param uid
     * @param callback
     * @return long
     * @throws
     **/
    public long placeNewOrder(
            int userCookie,
            long price,
            long reservedBidPrice,
            long size,
            OrderAction action,
            OrderType orderType,
            int symbol,
            long uid,
            RequestDto requestDto,
            Consumer<OrderCommand> callback) {

        final long seq = ringBuffer.next();
        try {
            OrderCommand cmd = ringBuffer.get(seq);
            cmd.command = OrderCommandType.PLACE_ORDER;
            cmd.resultCode = CommandResultCode.NEW;
            cmd.requestDto = requestDto;

            promises.put(seq, callback);

        } finally {
            ringBuffer.publish(seq);
        }
        return seq;
    }


}
