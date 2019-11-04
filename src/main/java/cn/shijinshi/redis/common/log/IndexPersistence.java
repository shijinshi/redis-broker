package cn.shijinshi.redis.common.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * 在双写过程中，我们通过 {@link LogPersistence} 来作持久化的队列。
 * 如果进程挂了，那么当程序启动时，自然可以继续读取队列中的数据。
 *
 * 但是，这里该怎么知道从队列中哪个位置读取数据呢？
 * 我们采取的思路是，每当程序从队列中读取请求，在正确处理请求后，
 * 应该告知LogPersistence，那么IndexPersistence则定时地将这个位置
 * 存在磁盘上。等下次程序启动时，就可以从磁盘上获取正确的位置。
 *
 * @author Gui Jiahai
 */
public class IndexPersistence implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(IndexPersistence.class);

    public static final String SUFFIX = ".index";

    //文件名格式必须为 0000000000000000098.index
    //前面为19位，足够表示所有的正长整数
    public static final FilenameFilter INDEX_FILTER = (dir, name) -> {
        if (name.length() == 25 && name.endsWith(SUFFIX)) {
            String str = name.substring(0, 19);
            try {
                return Long.parseLong(str) > 0;
            } catch (NumberFormatException ignored) {
            }
        }
        return false;
    };

    private final File dir;

    private IndexEntry curIndexEntry = new IndexEntry(-1, -1);

    private FileOutputStream output;
    private volatile boolean closed = false;

    public IndexPersistence(File dir) {
        this.dir = Objects.requireNonNull(dir);
    }

    public void persist(IndexEntry indexEntry) throws IOException {
        if (closed) {
            throw new IOException("Index file stream closed");
        }

        if (this.curIndexEntry.equals(indexEntry)) {
            return;
        }

        if (this.curIndexEntry.getSequence() != indexEntry.getSequence() || this.output == null) {
            if (this.output != null) {
                Closeable c = this.output;
                this.output = null;
                try {
                    c.close();
                } catch (IOException ignored) {}
            }

            this.output = new FileOutputStream(newFile(indexEntry.getSequence()), true);
        }
        this.curIndexEntry = indexEntry;

        String line = String.format("%019d\n", indexEntry.getOffset());
        output.write(line.getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    private File newFile(long sequence) {
        return new File(this.dir, String.format("%019d%s", sequence, SUFFIX));
    }

    public IndexEntry latestIndex() {
        File[] list = dir.listFiles(INDEX_FILTER);
        if (list == null || list.length == 0) {
            return new IndexEntry(0, 0);
        }
        Arrays.sort(list);

        File latest = list[list.length - 1];
        logger.info("Getting latest index entry, index file: {}", latest);

        RandomAccessFile input;
        try {
            input = new RandomAccessFile(latest, "r");
        } catch (FileNotFoundException e) {
            logger.warn("Cannot get index file, and return IndexEntry(0, 0)", e);
            return new IndexEntry(0, 0);
        }
        try {
            //正常情况下，每行记录应该是40字节
            //这里担心数据不完整，所以，做一个简单的校验，去掉损坏的部分
            long size = input.length();
            size -= (size % 20);
            if (size <= 0) {
                logger.warn("Cannot get complete index entry, and return IndexEntry(0, 0)");
                return new IndexEntry(0, 0);
            }
            size -= 20;
            if (size <= 0) {
                size = 0;
            }
            input.seek(size);
            byte[] bytes = new byte[20];
            input.readFully(bytes);

            IndexEntry indexEntry = new IndexEntry(parse(latest.getName()),
                    parse(new String(bytes, StandardCharsets.UTF_8)));

            logger.info("Got latest index entry: {}", indexEntry);
            return indexEntry;

        } catch (Exception e) {
            logger.error("Failed to read from IndexFile", e);
            throw new IllegalStateException(e);
        } finally {
            try {
                input.close();
            } catch (IOException ignored) {}
        }
    }

    private long parse(String string) {
        if (string.length() > 19) {
            string = string.substring(0, 19);
        }
        return Long.parseLong(string);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            Closeable c = this.output;
            this.output = null;
            if (c != null) {
                try {
                    c.close();
                } catch (IOException ignored) {}
            }
        }
    }

}
