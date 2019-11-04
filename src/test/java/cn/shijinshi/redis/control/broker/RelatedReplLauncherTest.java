package cn.shijinshi.redis.control.broker;

import cn.shijinshi.redis.common.prop.BrokerProperties;
import cn.shijinshi.redis.control.rpc.Answer;
import cn.shijinshi.redis.control.rpc.Indication;
import cn.shijinshi.redis.control.rpc.State;
import cn.shijinshi.redis.control.rpc.Type;
import cn.shijinshi.redis.replicate.ReplicationJob;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

/**
 * @author Gui Jiahai
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({RelatedReplLauncher.class})
@PowerMockIgnore("javax.management.*")
public class RelatedReplLauncherTest {

    @InjectMocks
    private RelatedReplLauncher launcher;
    @Mock
    private ReplicationJob job;
    @Mock
    private BrokerProperties properties;

    @Before
    public void setup() throws Exception {
//        job = PowerMockito.mock(ReplicationJob.class);

        doNothing().when(job).start();
        doNothing().when(job).interrupt();
        PowerMockito.when(job.isAlive()).thenReturn(Boolean.TRUE);

        PowerMockito.whenNew(ReplicationJob.class).withAnyArguments().thenReturn(job);
    }

    @Test
    public void test() {
        launcher.start();
        test_active();
        test_2_in_completion();
        test_in_completion();
        test_2_on_completion();
    }

    @Test
    public void test_error_in_completion() {
        launcher.start();
        Indication indication = new Indication(1, Type.REPL_IN_COMPLETION);
        indication.setMessage("tick");
        Answer answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.BAD);
    }

    @Test
    public void test_error_active() {
        launcher.start();
        test_active();
        Indication indication = new Indication(1, Type.REPL_ACTIVE);
        Answer answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.BAD);
    }

    private void test_active() {
        Indication indication = new Indication(1, Type.REPL_ACTIVE);
        Answer answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.OK);
        PowerMockito.verifyNew(ReplicationJob.class, times(1));
    }

    private Answer sendPing() {
        Indication indication = new Indication(1, Type.REPL_PING);
        return launcher.apply(indication);
    }

    private void test_2_in_completion() {
        Assert.assertEquals(sendPing().getState(), State.OK);
        launcher.onNext(job);
        Assert.assertEquals(sendPing().getState(), State.NEXT);
        Assert.assertEquals(sendPing().getState(), State.OK);
    }

    private void test_in_completion() {
        Indication indication = new Indication(1, Type.REPL_IN_COMPLETION);
        indication.setMessage("tick");
        Answer answer = launcher.apply(indication);
        Assert.assertEquals(answer.getState(), State.OK);
    }

    private void test_2_on_completion() {
        Assert.assertEquals(sendPing().getState(), State.OK);
        launcher.onCompleted(job);
        Assert.assertEquals(sendPing().getState(), State.NEXT);
    }

}
