package cn.shijinshi.redis.common.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Objects;

/**
 * 清除过期数据
 *
 * log日志用于存放请求数据，那么index日志用于存放当前的已消费位置。
 * 意味着，从index日志中，可以知道哪些log日志可以清除的。
 *
 * @author Gui Jiahai
 */
public class Cleaner {

    private static final Logger logger = LoggerFactory.getLogger(Cleaner.class);

    private final FilenameFilter logFilter = LogPersistence.LOG_FILTER;
    private final FilenameFilter indexFilter = IndexPersistence.INDEX_FILTER;

    private final File dir;

    public Cleaner(File dir) {
        this.dir = Objects.requireNonNull(dir);
    }

    public void clean() {
        if (!dir.exists()) {
            return;
        }

        File[] indexFiles = dir.listFiles(indexFilter);
        if (indexFiles == null || indexFiles.length == 0) {
            return;
        }
        Arrays.sort(indexFiles);
        File latestIndex = indexFiles[indexFiles.length - 1];

        //删除索引文件
        indexFiles[indexFiles.length - 1] = null;
        for (File indexFile : indexFiles) {
            if (indexFile != null) {
                if (!indexFile.delete()) {
                    logger.warn("Failed to delete index file: {}", indexFile);
                }
            }
        }


        String indexFileName = latestIndex.getName();
        String sequenceStr = null;
        if (indexFileName.endsWith(IndexPersistence.SUFFIX)) {
            sequenceStr = indexFileName.substring(0, indexFileName.length() - IndexPersistence.SUFFIX.length());
        }
        if (sequenceStr == null || sequenceStr.isEmpty()) {
            logger.warn("Cannot get sequence string from index file name: {}", indexFileName);
            return;
        }

        File[] logFiles = dir.listFiles(logFilter);
        if (logFiles == null || logFiles.length == 0) {
            return;
        }

        for (File logFile : logFiles) {
            if (logFile.getName().endsWith(LogPersistence.SUFFIX)) {
                String logFilePrefix = logFile.getName().substring(0,
                        logFile.getName().length() - LogPersistence.SUFFIX.length());
                if (logFilePrefix.compareToIgnoreCase(sequenceStr) < 0) {
                    if (!logFile.delete()) {
                        logger.warn("Failed to delete log file: {}", logFile);
                    }
                }
            }
        }
    }

}
