package cn.shijinshi.redis.common.log;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Gui Jiahai
 */
public class IndexPersistenceTest extends AbstractTmpDir {

    private IndexPersistence indexPersistence;

    @Before
    @Override
    public void setup() {
        super.setup();
        indexPersistence = new IndexPersistence(dir);
    }

    @Test
    public void test() throws IOException {
        Assert.assertEquals(new IndexEntry(0, 0), indexPersistence.latestIndex());

        IndexEntry current;

        current = new IndexEntry(1, 1);
        indexPersistence.persist(current);
        String[] list = dir.list(IndexPersistence.INDEX_FILTER);
        Assert.assertArrayEquals(list, new String[]{indexFileName(1)});Assert.assertEquals(current, indexPersistence.latestIndex());


        current = new IndexEntry(1, 2);
        indexPersistence.persist(current);
        list = dir.list(IndexPersistence.INDEX_FILTER);
        Assert.assertArrayEquals(list, new String[]{indexFileName(1)});
        Assert.assertEquals(current, indexPersistence.latestIndex());

        current = new IndexEntry(2, 3);
        indexPersistence.persist(new IndexEntry(2, 3));
        list = dir.list(IndexPersistence.INDEX_FILTER);
        Assert.assertArrayEquals(list, new String[]{indexFileName(1), indexFileName(2)});
        Assert.assertEquals(current, indexPersistence.latestIndex());

        List<String> lines = Files.readLines(new File(dir, indexFileName(1)), StandardCharsets.UTF_8);
        Assert.assertEquals(lines, Arrays.asList("0000000000000000001", "0000000000000000002"));

        lines = Files.readLines(new File(dir, indexFileName(2)), StandardCharsets.UTF_8);
        Assert.assertEquals(lines, Collections.singletonList("0000000000000000003"));

    }


    @After
    @Override
    public void after() {
        indexPersistence.close();

        super.after();
    }
}
