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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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
public class JournallingProcessorUl {

    private static final int MB = 1024 * 1024;
    private static final int FILE_SIZE_TRIGGER = 1024 * MB; // split files by size
    private static final String FILE_NAME_PATTERN = "/exchange/data/%s_%04d.olog";
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

//        log.debug("Writing {}", cmd);

        //buffer.putInt(cmd.symbol); // TODO Header

        // TODO 分配大缓冲区，只需移动指针
        // TODO allocate big buffer, just move pointer

        // 25 bytes
        buffer.putLong(cmd.timestamp); // 8 bytes
        buffer.put(cmd.command.getCode()); // 1 byte
        buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta
        buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary

        // 12 bytes
        if (cmd.command == OrderCommandType.MOVE_ORDER || cmd.command == OrderCommandType.PLACE_ORDER) {
            buffer.putLong(cmd.price); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.size); // 8 bytes - can be compressed
        }

        // 1 byte
        if (cmd.command == OrderCommandType.PLACE_ORDER) {
            int actionAndType = (cmd.action.getCode() << 2) & cmd.orderType.getCode();
            buffer.put((byte) actionAndType); // 1 byte
        }

        if (eob || buffer.position() >= BUFFER_FLUSH_TRIGER) {
//            log.debug("Flushing {} bytes", buffer.position());
            flushBufferSync();
        }
    }

    /**
     * 同步刷新缓冲区
     * @throws IOException
     */
    private void flushBufferSync() throws IOException {
        File f = new File(".");
        String absolutePath = f.getAbsolutePath();
        System.out.println(absolutePath);

        String path = absolutePath + String.format(FILE_NAME_PATTERN, today, filesCounter);
        File file=new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /*//要读写的数据源
        File f = new File("D:\\reviewIO\\RandomAccessFileTest.txt");
        //指定文件不存在就创建同名文件
        if(!f.exists()){
            f.createNewFile();
        }*/

        raf = new RandomAccessFile(path, "rwd");
        raf.write(buffer.array(), 0, buffer.position());
        writtenBytes += buffer.position();
        buffer.clear();

        if (writtenBytes >= FILE_SIZE_TRIGGER) {
            // todo 开始异步准备新文件，但仅一次
            // todo start preparing new file asynchronously, but ONLY ONCE
            startNewFile();
            writtenBytes = 0;
        }
    }

    //@PostConstruct
    private void startNewFile() throws IOException {
        filesCounter++;
        if (raf != null) {
            raf.close();
        }
        raf = new RandomAccessFile(String.format(FILE_NAME_PATTERN, today, filesCounter), "rwd");
    }
}
