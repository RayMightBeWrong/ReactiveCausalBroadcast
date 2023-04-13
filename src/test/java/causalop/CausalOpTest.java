package causalop;

import io.reactivex.rxjava3.core.Observable;
import org.junit.Assert;
import org.junit.Test;

public class CausalOpTest {
    @Test
    public void testOk() {
        var l = Observable.just(
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0),
                        new CausalMessage<String>("c", 1, 1, 2)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testReorder() {
        var l = Observable.just(
                        new CausalMessage<String>("c", 1, 1, 2),
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testDupl() {
        var l = Observable.just(
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0),
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("c", 1, 1, 2)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGap() {
        var l = Observable.just(
                        new CausalMessage<String>("c", 1, 1, 2),
                        new CausalMessage<String>("a", 1, 0, 1)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();
    }
}
