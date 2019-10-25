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
package exchange.core2.core.processors.journalling;

import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Journal writer
 * <p>
 * - stateful handler
 * - not thread safe! 不是线程安全的
 */
@Slf4j
public class JournallingProcessor {

    private static final int MB = 1024 * 1024;
    private static final int FILE_SIZE_TRIGGER = 1024 * MB; // split files by size
    private static final String FILE_NAME_PATTERN = "F:\\workspace2.1.0\\workspace_2.1.0\\exchange-core\\exchange\\data/%s_%04d.log";
    private static final String DATE_FORMAT = "yyyy-MM-dd_HHmmss";

    private static final int BUFFER_SIZE = 65536;
    private static final int BUFFER_FLUSH_TRIGER = BUFFER_SIZE - 256;

    private RandomAccessFile raf;
    private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private int filesCounter = 0;

    private long writtenBytes = 0;

    private final String today = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATE_FORMAT));

    // TODO 异步创建新文件，然后切换引用
    // TODO asynchronously create new file and then switch reference

    public void onEvent(OrderCommand cmd, long seq, boolean eob) throws IOException {

        // log.debug("Writing {}", cmd);
        //buffer.putInt(cmd.symbol); // TODO Header

        /*// 12 bytes
        if (cmd.command == OrderCommandType.MOVE_ORDER || cmd.command == OrderCommandType.PLACE_ORDER) {
        }

        // 1 byte
        if (cmd.command == OrderCommandType.PLACE_ORDER) {
        }*/

        //写入相应的文件
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(String.format(FILE_NAME_PATTERN, today, filesCounter), true)));
            out.write(cmd.toString()+"\r\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
