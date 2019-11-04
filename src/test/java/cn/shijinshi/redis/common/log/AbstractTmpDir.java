package cn.shijinshi.redis.common.log;

import cn.shijinshi.redis.common.util.FileUtil;

import java.io.File;
import java.util.UUID;

/**
 * @author Gui Jiahai
 */
public class AbstractTmpDir {

    protected File dir;

    public void setup() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        dir = new File(tmpDir, "redis-" + uuid);
        if (!dir.mkdirs()) {
            throw new IllegalStateException("Cannot mkdirs for path: " + dir.getAbsolutePath());
        }

        System.out.println(dir);
    }


    protected String logFileName(long seq) {
        return String.format("%019d%s", seq, LogPersistence.SUFFIX);
    }

    protected String indexFileName(long seq) {
        return String.format("%019d%s", seq, IndexPersistence.SUFFIX);
    }

    public void after() {
        FileUtil.delete(dir);
    }

}
