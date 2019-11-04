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
public class LogPersistenceTest extends AbstractTmpDir {

    @Before
    @Override
    public void setup() {
        super.setup();
    }

    @Test(expected = IllegalStateException.class)
    public void test_Access_R() {
        try (LogPersistence persistence = new LogPersistence(dir, new LogProperties(), new IndexEntry(0, 0), Access.R)) {
            persistence.getOutputStream();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void test_Access_W() {
        try (LogPersistence persistence = new LogPersistence(dir, new LogProperties(), new IndexEntry(0, 0), Access.W)) {
            persistence.getInputStream();
        }
    }

    @Test
    public void test_input_output() throws IOException {
        LogProperties properties = new LogProperties();
        properties.setSegmentBytes(5);

        LogPersistence persistence = new LogPersistence(dir, properties, null, Access.W);
        persistence.getOutputStream().write("Hello".getBytes());
        persistence.getOutputStream().write("World".getBytes());
        persistence.close();

        persistence = new LogPersistence(dir, new LogProperties(), new IndexEntry(1, 2), Access.R);
        byte[] buf = new byte[1024];
        int read = persistence.getInputStream().read(buf);
        Assert.assertEquals(read, 3);
        Assert.assertEquals(new String(buf, 0, read), "llo");

        read = persistence.getInputStream().read(buf);
        Assert.assertEquals(read, 5);
        Assert.assertEquals(new String(buf, 0, read), "World");

        read = persistence.getInputStream().read();
        Assert.assertEquals(read, -1);

        persistence.ack(8);
        Assert.assertEquals(persistence.getAckedIndex(), new IndexEntry(2, 5));

        persistence.close();
    }

    @Test
    public void test_input_output_RW() throws IOException {
        LogPersistence persistence = new LogPersistence(dir, new LogProperties(), new IndexEntry(0, 0), Access.RW);
        persistence.getOutputStream().write("HelloWorld".getBytes());
        persistence.getOutputStream().flush();

        new Thread(() -> {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            try {
                persistence.getOutputStream().close();
            } catch (IOException ignored) {
            }
        }).start();
        long start = System.currentTimeMillis();
        byte[] buf = new byte[1024];
        int n;
        int total = 0;
        while ((n = persistence.getInputStream().read(buf)) != -1) {
            total += n;
        }
        Assert.assertEquals(total, 10);
        Assert.assertTrue(System.currentTimeMillis() - start >= 1000);

        persistence.close();
    }

    @After
    @Override
    public void after() {
        super.after();
    }
}
