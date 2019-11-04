package cn.shijinshi.redis.common.log;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Gui Jiahai
 */
public class IndexLoggerTest extends AbstractTmpDir {

    @Before
    @Override
    public void setup() {
        super.setup();
    }

    @Test
    public void test() throws IOException {
        LogProperties properties = new LogProperties();
        properties.setDir(dir.getAbsolutePath());
        properties.setPersistIndexMs(100);

        IndexPersistence persistence = new IndexPersistence(dir);

        IndexLogger logger = new IndexLogger(properties, Access.RW);

        logger.getOutputStream().write("HelloWorld".getBytes());
        logger.getOutputStream().flush();

        byte[] buf = new byte[1024];
        int total = 0, n;
        while ((n = logger.getInputStream().read(buf, total, buf.length - total)) != -1) {
            total += n;
            if (total >= 10) {
                break;
            }
        }

        Assert.assertEquals(total, 10);
        Assert.assertEquals(new String(buf, 0, total), "HelloWorld");

        logger.ack(5);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
        Assert.assertEquals(persistence.latestIndex(), new IndexEntry(1, 5));

        logger.ack(5);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
        Assert.assertEquals(persistence.latestIndex(), new IndexEntry(1, 10));

        logger.close();
        persistence.close();
    }

    @After
    @Override
    public void after() {
        super.after();
    }
}
