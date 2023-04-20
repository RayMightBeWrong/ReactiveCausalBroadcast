package causalop;

import io.reactivex.rxjava3.core.Observable;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CausalOpTest {
    @Test
    public void testOk() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = Observable.just(
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2),
                        new CausalMessage<String>("c", 1, vv3)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testReorder() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = Observable.just(
                        new CausalMessage<String>("c", 1, vv3),
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testDupl() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = Observable.just(
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2),
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("c", 1, vv3)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGap() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = Observable.just(
                        new CausalMessage<String>("c", 1, vv3),
                        new CausalMessage<String>("a", 1, vv1)
                )
                .lift(new CausalOperator<String>(2))
                .toList().blockingGet();
    }
}
