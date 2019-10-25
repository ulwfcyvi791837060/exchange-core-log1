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
package exchange.core2.tests.perf;

import exchange.core2.tests.util.ExchangeTestContainer;
import exchange.core2.tests.util.TestConstants;
import exchange.core2.tests.util.ThroughputTestsModule;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public final class PerfThroughput {

    // TODO 如果测试失败，则关闭中断器
    // TODO shutdown disruptor if test fails
    /**
     * 测试吞吐量交换
     * @Author zenghuikang
     * @Description
     * @Date 2019/10/22 12:24
      * @param
     * @return void
     * @throws
     **/
    @Test
    public void testThroughputExchange() throws Exception {
        try (final ExchangeTestContainer container = new ExchangeTestContainer(2 * 1024, 1, 1, 1536, null)) {
            ThroughputTestsModule.throughputTestImpl(
                    container,
                    1_000_000,//1_000_000,//3_000_000,
                    1000,
                    2000,
                    5,
                    //货币兑换
                    TestConstants.CURRENCIES_EXCHANGE,
                    1,
                    ExchangeTestContainer.AllowedSymbolTypes.CURRENCY_EXCHANGE_PAIR);
        }
    }



}