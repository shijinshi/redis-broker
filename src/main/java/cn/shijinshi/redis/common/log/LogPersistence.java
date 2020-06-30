package cn.shijinshi.redis.common.log;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * 在同步任务中，需要一个持久化的阻塞式队列去缓存redis请求报文。
 * 所以，这里需要设计一个控件，拥有InputStream和OutputStream的功能，
 * 通过OutputStream向队列中写入请求，通过InputStream读取请求。
 * 但是每当FileInputStream读到文件末尾时，会返回-1，由于是阻塞式队列，
 * 所以不应该返回-1，而是阻塞等待，直到OutputStream写入新的数据时，才返回新的数据。
 *
 * 1、日志文件的命名方式为 19位正整数的序号 + ".log"，也就说第一个文件名称为 0000000000000000001.log，
 *     后续文件应该在前者的正整数序号的基础上加一， 即 0000000000000000002.log
 * 2、在写的过程中，判断文件大小是否达到最大阈值(MAX_SIZE)，如果是，则创建新的文件，否则，继续在原文件上写
 * 3、在读的过程中，根据初始化信息，我们可以知道从哪个文件(inputSequence)的哪个位置(inputPosition)开始读。
 *    如果FileInputStream.read 返回-1，那么应该有两种情况：
 *    1) 如果该文件小于最大阈值(MAX_SIZE)，则应该是没有新的数据写入，此时，应阻塞一段时间后，重新去读
 *    2) 如果该文件大于或等于最大阈值(MAX_SIZE)，则应查看是否有新的文件生成
 *       i) 如果是，则说明数据已经写的新文件中了，应该从新文件中读取
 *       ii) 否，则阻塞等待一段时间，继续从原始文件中读取
 *
 * @author Gui Jiahai
 */
public class LogPersistence implements Closeable {

    public static final String SUFFIX = ".log";

    //文件名格式必须为 0000000000000000098.log
    //前面为19位，足够表示所有的正长整数
    public static final FilenameFilter LOG_FILTER = (dir, name) -> {
        if (name.length() == 23 && name.endsWith(SUFFIX)) {
            String str = name.substring(0, 19);
            try {
                return Long.parseLong(str) > 0;
            } catch (NumberFormatException ignored) {
            }
        }
        return false;
    };

    private static final long DELAY_MILLIS = 1000;

    private final File dir;
    private final LogProperties properties;

    private final InternalOutput output;
    private final InternalInput input;

    private long ackedSequence = -1;
    private long ackedOffset = -1;

    private final Access access;

    public LogPersistence(File dir, LogProperties properties, IndexEntry indexEntry, Access access) {
        this.dir = dir;
        this.properties = properties;

        this.access = Objects.requireNonNull(access);
        if (access.isReadable()) {
            this.input = new InternalInput(indexEntry.getSequence(), indexEntry.getOffset());
        } else {
            this.input = null;
        }
        this.output = access.isWritable() ? new InternalOutput() : null;
    }

    public InputStream getInputStream() {
        if (!access.isReadable()) {
            throw new IllegalStateException("Cannot get InputStream cause it's not readable");
        }
        return this.input;
    }

    public OutputStream getOutputStream() {
        if (!access.isWritable()) {
            throw new IllegalStateException("Cannot get OutputStream cause it's not writable");
        }
        return this.output;
    }

    private FileOutputStream createOutput() throws IOException {
        String[] list = dir.list(LOG_FILTER);
        long newSeq = 1L;
        if (list != null && list.length != 0) {
            Arrays.sort(list);
            String last = list[list.length - 1];
            long seq = Long.parseLong(last.substring(0, 19));
            if (new File(dir, last).length() >= this.properties.getSegmentBytes()) {
                newSeq = seq + 1;
            } else {
                newSeq = seq;
            }
        }
        return new FileOutputStream(new File(dir, fileName(newSeq)), true);
    }

    private FileInputStream createInput(long expectedSequence, long[] actualSequence) throws IOException {
        String[] list = dir.list(LOG_FILTER);
        if (list == null || list.length == 0) {
            return null;
        }

        List<String> files = new ArrayList<>(Arrays.asList(list));

        String target = fileName(expectedSequence);
        if (files.contains(target)) {
            actualSequence[0] = expectedSequence;
            return new FileInputStream(new File(dir, target));
        } else {
            files.add(target);
            files.sort(String::compareTo);
            int index = files.indexOf(target);
            if (index == files.size() - 1) {
                return null;
            } else {
                String file = files.get(index + 1);
                long seq = Long.parseLong(file.substring(0, 19));
                actualSequence[0] = seq;
                return new FileInputStream(new File(dir, files.get(1)));
            }
        }
    }

    private boolean hasNewFile(long sequence) {
        String[] list = dir.list(LOG_FILTER);
        if (list == null || list.length == 0) {
            return false;
        }
        List<String> files = Arrays.asList(list);
        files.sort(String::compareTo);
        int index = files.indexOf(fileName(sequence));
        return index < files.size() - 1;
    }

    public synchronized void ack(long ackedBytes) {
        if (!access.isReadable()) {
            throw new IllegalStateException("Cannot make ack cause it's not readable");
        }

        ArrayDeque<IndexEntry> history = this.input.history;
        ackedOffset += ackedBytes;
        if (!history.isEmpty()) {
            IndexEntry entry = history.peek();
            if (ackedOffset >= entry.getOffset()) {
                ackedOffset -= entry.getOffset();

                history.remove();

                IndexEntry next = history.peek();
                if (next == null) {
                    ackedSequence = this.input.curSequence;
                } else {
                    ackedSequence = next.getSequence();
                }
            } else {
                ackedSequence = entry.getSequence();
            }
        } else {
            ackedSequence = this.input.curSequence;
        }
    }

    public synchronized IndexEntry getAckedIndex() {
        if (ackedSequence < 0 || ackedOffset < 0) {
            return null;
        }
        return new IndexEntry(this.ackedSequence, this.ackedOffset);
    }

    private String fileName(long sequence) {
        return String.format("%019d%s", sequence, SUFFIX);
    }

    private boolean waitForOutput() {
        return access.isWritable() && !this.output.isClosed();
    }

    @Override
    public void close() {
        if (this.input != null) {
            try {
                this.input.close();
            } catch (IOException ignored) {
            }
        }
        if (this.output != null) {
            try {
                this.output.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void checkClosed(boolean closed) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }

    private class InternalInput extends InputStream {

        private FileInputStream reader;
        private long curSequence;
        private final ArrayDeque<IndexEntry> history = new ArrayDeque<>();
        private boolean closed = false;

        InternalInput(long sequence, long offset) {
            this.curSequence = sequence;
            init(offset);
        }

        private void init(long offset) {
            try {
                checkAndExit(false, true, offset);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            if (reader == null) {
                ackedSequence = -1;
                ackedOffset = 0;
            } else {
                ackedSequence = curSequence;
                ackedOffset = offset;
            }
        }

        @Override
        public int read() throws IOException {
            boolean eof = false;
            for (;;) {
                boolean exit = false;
                if (eof || reader == null) {
                    exit = checkAndExit(eof, false, 0);
                }
                if (reader == null) {
                    return -1;
                }
                int b = reader.read();
                if (b == -1) {
                    if (exit) {
                        return -1;
                    }
                } else {
                    return b;
                }
                eof = true;
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            boolean eof = false;
            for (;;) {
                boolean exit = false;
                if (eof || reader == null) {
                    exit = checkAndExit(eof, false, 0);
                }
                if (reader == null) {
                    return -1;
                }
                int n = reader.read(b, off, len);
                if (n == -1) {
                    if (exit) {
                        return -1;
                    }
                } else {
                    return n;
                }
                eof = true;
            }
        }

        private boolean checkAndExit(boolean eof, boolean once, long offset) throws IOException {
            for (;;) {
                checkClosed(closed);

                while (reader == null) {
                    long[] actualSequence = new long[1];
                    synchronized (this) {
                        checkClosed(closed);
                        reader = createInput(curSequence, actualSequence);
                    }
                    if (reader != null) {
                        if (curSequence != actualSequence[0]) {
                            curSequence = actualSequence[0];
                            //如果不是指定的文件，则应该从起始位置开始读取
                            offset = 0;
                        }
                        //只有在程序启动时，第一次读取数据时，才会用到offset
                        //从第二次开始，应该都是从0开始读取数据

                        long copyOffset = offset;
                        while (copyOffset > 0) {
                            int available = reader.available();
                            if (available <= 0) {
                                String msg = String.format("Unable to skip %d bytes", offset);
                                throw new IOException(msg);
                            }
                            long a = Math.min((long) available, copyOffset);
                            copyOffset -= reader.skip(a);
                        }

                    } else if (once) {
                        return false;
                    } else if (parkExit()) {
                        return true;
                    }
                }

                if (!eof) {
                    //直接读取数据，如果没有数据，再重新进入以下逻辑
                    return false;
                }

                if (hasNewFile(curSequence)) {
                    //如果当前文件没有数据，则判断是否有创建新文件
                    //如果有创建新文件，那么以后的数据都会写向新文件
                    //所以，这时候，在当前文件读取完之后，应该转向新文件
                    if (reader.available() > 0) {
                        //重新判断当前文件是否残留数据
                        return false;
                    } else {
                        //当前文件的已经超过MAX_SIZE时
                        long size = reader.getChannel().size();
                        history.offer(new IndexEntry(curSequence, size));
                        //关闭当前文件，序列号加一，准备读取新文件
                        eof = false;
                        curSequence ++;

                        Closeable c = reader;
                        reader = null;

                        try {
                            c.close();
                        } catch (IOException ignored) {}
                        continue;
                    }
                }

                //如果当前文件没有数据了，则阻塞等待
                return parkExit();
            }
        }

        private boolean checkAgain = true;
        private boolean parkExit() {
            if (!waitForOutput()) {
                if (checkAgain) {
                    checkAgain = false;
                    return false;
                }
                return true;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(DELAY_MILLIS));
            return false;
        }

        @Override
        public void close() throws IOException {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
            }
            Closeable c;
            if ((c = reader) != null) {
                c.close();
            }
        }
    }

    class InternalOutput extends OutputStream {
        private FileOutputStream writer;
        private long size;

        private volatile boolean closed = false;

        @Override
        public void write(int b) throws IOException {
            check();
            writer.write(b);
            size ++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            check();
            writer.write(b, off, len);
            size += len;
        }

        private void check() throws IOException {
            if (writer == null) {
                synchronized (this) {
                    checkClosed(closed);
                    writer = createOutput();
                    size = writer.getChannel().size();
                }
            }
            if (size >= properties.getSegmentBytes()) {
                synchronized (this) {
                    checkClosed(closed);
                    FileOutputStream s = writer;
                    writer = null;
                    s.flush();

                    try {
                        s.close();
                    } catch (IOException ignored) {}

                    writer = createOutput();
                }
            }
        }

        @Override
        public void flush() throws IOException {
            OutputStream w;
            if ((w = writer) != null) {
                w.flush();
            }
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public synchronized void close() throws IOException {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
            }
            OutputStream o;
            if ((o = writer) != null) {
                o.flush();
                o.close();
            }
        }
    }

}
