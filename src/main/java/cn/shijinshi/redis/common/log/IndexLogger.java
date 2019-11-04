package cn.shijinshi.redis.common.log;

import cn.shijinshi.redis.common.Shutdown;
import cn.shijinshi.redis.common.util.ExecutorUtil;
import cn.shijinshi.redis.common.util.SimpleThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * IndexLogger主要是用于整合 {@link LogPersistence}
 * 和 {@link IndexPersistence}。
 *
 * @author Gui Jiahai
 */
public class IndexLogger implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(IndexLogger.class);

    private final LogPersistence logPersistence;
    private final Access access;

    private IndexPersistence indexPersistence;
    private Cleaner cleaner;
    private ScheduledExecutorService scheduledExecutor;

    private InputStream inputStream;
    private OutputStream outputStream;

    public IndexLogger(LogProperties properties, Access access) {
        this.access = Objects.requireNonNull(access);

        File dir = new File(properties.getDir());
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new IllegalStateException("File exists, but not a directory : " + dir.getAbsolutePath());
            }
        } else {
            if (!dir.mkdirs()) {
                throw new IllegalStateException("Cannot mkdirs with file: " + dir.getAbsolutePath());
            }
            logger.info("create directory: {}", dir.getAbsolutePath());
        }

        if (access.isReadable()) {
            this.indexPersistence = new IndexPersistence(dir);
            this.cleaner = new Cleaner(dir);
            this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
                    new SimpleThreadFactory("index-logger-scheduler", true));
            this.scheduledExecutor.scheduleWithFixedDelay(() -> {
                try {
                    persistIndex();
                } catch (Throwable t) {
                    logger.error("Failed to persist acked index entry", t);
                }
            }, properties.getPersistIndexMs(), properties.getPersistIndexMs(), TimeUnit.MILLISECONDS);
            this.scheduledExecutor.scheduleAtFixedRate(() -> {
                try {
                    cleaner.clean();
                } catch (Throwable t) {
                    logger.error("Failed to clean acked log file", t);
                }
            }, properties.getCleanSec(), properties.getCleanSec(), TimeUnit.SECONDS);
        }

        IndexEntry latestIndex = this.indexPersistence == null ? null : this.indexPersistence.latestIndex();
        this.logPersistence = new LogPersistence(dir, properties, latestIndex, access);

        Shutdown.addRunnable(this::close);
    }

    public void ack(long ackedBytes) {
        if (!access.isReadable()) {
            throw new IllegalStateException("Cannot ack cause it's not readable");
        }
        this.logPersistence.ack(ackedBytes);
    }

    private synchronized void persistIndex() throws IOException {
        IndexEntry ackedIndex = this.logPersistence.getAckedIndex();
        if (ackedIndex != null) {
            indexPersistence.persist(ackedIndex);
        }
    }

    public InputStream getInputStream() {
        if (inputStream == null) {
            inputStream = new BufferedInputStream(logPersistence.getInputStream());
        }
        return inputStream;
    }

    public OutputStream getOutputStream() {
        if (outputStream == null) {
            outputStream = new BufferedOutputStream(logPersistence.getOutputStream());
        }
        return outputStream;
    }

    @Override
    public void close() {
        ExecutorUtil.shutdown(scheduledExecutor, 10, TimeUnit.SECONDS);
        if (access.isReadable()) {
            try {
                persistIndex();
            } catch (IOException ignored) {}
        }

        if (this.logPersistence != null) {
            this.logPersistence.close();
        }
        if (this.indexPersistence != null) {
            this.indexPersistence.close();
        }
    }
}
