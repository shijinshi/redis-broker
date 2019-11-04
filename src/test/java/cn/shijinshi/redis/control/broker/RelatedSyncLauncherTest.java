package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.log.IndexLogger;
import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.control.rpc.Answer;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.control.rpc.State;
import cn.shijinshi.redis.control.rpc.Type;
import cn.shijinshi.redis.sync.RequestSynchronizer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

/**
 * @author Gui Jiahai
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({RelatedSyncLauncher.class, RequestSynchronizer.class})
@PowerMockIgnore("javax.management.*")
public class RelatedSyncLauncherTest {

    private RelatedSyncLauncher launcher;

    @Mock
    private BrokerProperties properties;
    @Mock
    private IndexLogger logger;
    @Mock
    private RequestSynchronizer synchronizer;

    @Before
    public void setup() throws Exception {
        doNothing().when(synchronizer).resume_();
        PowerMockito.mockStatic(Executors.class, invocation -> new ScheduledThreadPoolExecutor(1) {
            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                return null;
            }
        });

        PowerMockito.whenNew(RequestSynchronizer.class).withAnyArguments().thenReturn(synchronizer);

        launcher = new RelatedSyncLauncher(logger, properties);
        launcher.start();
    }

    @Test
    public void test_sync_run() {
        when(synchronizer.isPaused()).thenReturn(Boolean.TRUE);
        Indication indication = new Indication(1, Type.SYNC_RUN);
        Answer answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.OK);
        verify(synchronizer, times(1)).resume_();
    }

    @Test
    public void test_sync_pause() {
        Answer answer;
        Indication indication = new Indication(1, Type.SYNC_PAUSE);

        when(synchronizer.isPaused()).thenReturn(Boolean.TRUE);
        answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.BAD);

        when(synchronizer.isPaused()).thenReturn(Boolean.FALSE);
        answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.OK);

        verify(synchronizer, times(1)).pause();
    }

    @Test
    public void test_sync_discard() {
        Answer answer;
        Indication indication = new Indication(1, Type.SYNC_DISCARD);
        indication.setMessage("tick");

        when(synchronizer.isPaused()).thenReturn(Boolean.FALSE);
        answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.BAD);

        when(synchronizer.isPaused()).thenReturn(Boolean.TRUE);
        answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.OK);

        verify(synchronizer, times(1)).resumeTick("tick".getBytes());
    }

    @After
    public void after() {
        launcher.stop();
    }

}
