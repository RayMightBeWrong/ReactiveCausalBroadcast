package causalop;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.internal.util.AtomicThrowable;
import io.reactivex.rxjava3.subscribers.DefaultSubscriber;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class FlowableCausalOpTest {
    @Test
    public void testOk() {
        var l = new ArrayList<String>();
        Flowable.just(
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0),
                        new CausalMessage<String>("c", 1, 1, 2)
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
        var l = Flowable.just(
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0),
                        new CausalMessage<String>("c", 1, 1, 2)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testReorder() {
        var l = new ArrayList<String>();
        Flowable.just(
                        new CausalMessage<String>("c", 1, 1, 2),
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0)
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
        var l = Flowable.just(
                        new CausalMessage<String>("c", 1, 1, 2),
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test
    public void testDupl() {
        var l = new ArrayList<String>();
        Flowable.just(
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0),
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("c", 1, 1, 2)
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
        var l = Flowable.just(
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("b", 0, 1, 0),
                        new CausalMessage<String>("a", 1, 0, 1),
                        new CausalMessage<String>("c", 1, 1, 2)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .toList().blockingGet();

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c"});
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGap() {
        var l = new ArrayList<String>();
        final AtomicThrowable t = new AtomicThrowable();
        Flowable.just(
                        new CausalMessage<String>("c", 1, 1, 2),
                        new CausalMessage<String>("a", 1, 0, 1)
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
        var l = Flowable.just(
                        new CausalMessage<String>("c", 1, 1, 2),
                        new CausalMessage<String>("a", 1, 0, 1)
                )
                .lift(new FlowableCausalOperator<String>(2))
                .toList().blockingGet();
    }
}
