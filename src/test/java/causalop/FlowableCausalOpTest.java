package causalop;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.internal.util.AtomicThrowable;
import io.reactivex.rxjava3.subscribers.DefaultSubscriber;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FlowableCausalOpTest {
    @Test
    public void testOk() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = new ArrayList<String>();
        Flowable.just(
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2),
                        new CausalMessage<String>("c", 1, vv3)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .blockingSubscribe(new DefaultSubscriber<>(){
                    @Override
                    protected void onStart() {
                        request(1);
                    }

                    @Override
                    public void onNext(String s) {
                        l.add(s);
                        request(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });

        System.out.println(l);
        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testOkInfiniteCredits() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = Flowable.just(
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2),
                        new CausalMessage<String>("c", 1, vv3)
                )
                .lift(new FlowableCausalOperator<String>(2))
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

        var l = new ArrayList<String>();
        Flowable.just(
                        new CausalMessage<String>("c", 1, vv3),
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .blockingSubscribe(new DefaultSubscriber<>(){
                    @Override
                    protected void onStart() {
                        request(1);
                    }

                    @Override
                    public void onNext(String s) {
                        l.add(s);
                        request(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testReorderInfiniteCredits() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = Flowable.just(
                        new CausalMessage<String>("c", 1, vv3),
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testReorder2() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 2); vv3.put(1, 1);
        Map<Integer, Integer> vv4 = new HashMap<>();
        vv4.put(0, 1); vv4.put(1, 2);

        var l = new ArrayList<String>();
        Flowable.just(
                        new CausalMessage<String>("c", 0, vv3),
                        new CausalMessage<String>("d", 1, vv4),
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .blockingSubscribe(new DefaultSubscriber<>(){
                    @Override
                    protected void onStart() {
                        request(1);
                    }

                    @Override
                    public void onNext(String s) {
                        l.add(s);
                        request(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });

        System.out.println(l);
        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c","d"});
    }

    @Test
    public void testDupl() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = new ArrayList<String>();
        Flowable.just(
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2),
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("c", 1, vv3)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .blockingSubscribe(new DefaultSubscriber<>(){
                    @Override
                    protected void onStart() {
                        request(1);
                    }

                    @Override
                    public void onNext(String s) {
                        l.add(s);
                        request(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testDuplInfiniteCredits() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv2 = new HashMap<>();
        vv2.put(0, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = Flowable.just(
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("b", 0, vv2),
                        new CausalMessage<String>("a", 1, vv1),
                        new CausalMessage<String>("c", 1, vv3)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGap() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = new ArrayList<String>();
        final AtomicThrowable t = new AtomicThrowable();
        Flowable.just(
                        new CausalMessage<String>("c", 1, vv3),
                        new CausalMessage<String>("a", 1, vv1)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .blockingSubscribe(new DefaultSubscriber<>(){
                    @Override
                    protected void onStart() {
                        request(1);
                    }

                    @Override
                    public void onNext(String s) {
                        l.add(s);
                        request(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                         t.set(throwable);
                    }

                    @Override
                    public void onComplete() {

                    }
                });

        Throwable thr = t.get();
        if(thr != null) throw (RuntimeException) thr;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGapInfiniteCredits() {
        Map<Integer, Integer> vv1 = new HashMap<>();
        vv1.put(1, 1);
        Map<Integer, Integer> vv3 = new HashMap<>();
        vv3.put(0, 1); vv3.put(1, 2);

        var l = Flowable.just(
                        new CausalMessage<String>("c", 1, vv3),
                        new CausalMessage<String>("a", 1, vv1)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .toList().blockingGet();
    }
}
