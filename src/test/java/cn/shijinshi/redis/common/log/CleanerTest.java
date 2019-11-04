package cn.shijinshi.redis.common.log;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author Gui Jiahai
 */
public class CleanerTest extends AbstractTmpDir {

    private Cleaner cleaner;

    @Before
    public void setup() {
        super.setup();
        cleaner = new Cleaner(dir);
    }

    @Test
    public void test() throws IOException {
        Assert.assertTrue(new File(dir, indexFileName(13)).createNewFile());
        Assert.assertTrue(new File(dir, indexFileName(14)).createNewFile());

        Assert.assertTrue(new File(dir, logFileName(13)).createNewFile());
        Assert.assertTrue(new File(dir, logFileName(14)).createNewFile());
        Assert.assertTrue(new File(dir, logFileName(15)).createNewFile());

        cleaner.clean();

        String[] indexList = dir.list(IndexPersistence.INDEX_FILTER);
        Assert.assertArrayEquals(indexList, new String[]{indexFileName(14)});

        String[] logList = dir.list(LogPersistence.LOG_FILTER);
        Assert.assertArrayEquals(logList, new String[]{logFileName(14), logFileName(15)});
    }

    @After
    public void after() {
        super.after();
    }

}
