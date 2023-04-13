package causalop;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.FlowableOperator;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.*;

public class FlowableCausalOperator<T> implements FlowableOperator<T, CausalMessage<T>> {

    private final int n;

    public FlowableCausalOperator(int n) {
        this.n = n;
    }

    @Override
    public @NonNull Subscriber<? super CausalMessage<T>> apply(@NonNull Subscriber<? super T> down) throws Throwable {
        return new CausalSubscriber<T>(down, this.n);
    }

    private static class CausalSubscriber<T> implements Subscriber<CausalMessage<T>>, Subscription {

        private final Subscriber<? super T> down;
        private Subscription up;
        private long requested = 0;
        private boolean completed = false;

        private final int n;
        private final Set<CausalMessage<T>> buffer = new TreeSet<>(new CasualMessageComparator<T>());
        private int[] vv;

        public CausalSubscriber(Subscriber<? super T> down, int n) {
            this.down = down;
            this.n = n;
            this.vv = new int[n];
        }

        @Override
        public void onSubscribe(Subscription up) {
            this.up = up;
            this.down.onSubscribe(this);
        }

        @Override
        public void onNext(@NonNull CausalMessage<T> cm) {
            receive(cm);

            // We have already requested Long.MAX_VALUE, so we can't request any more.
            if(requested == Long.MAX_VALUE)
                return;

            //All the requested messages have been buffered,
            // so we need to request more to avoid a deadlock.
            if (requested > 0 && requested == buffer.size()) {
                requested++;
                System.out.println("requested upstream: " + 1);
                up.request(1);
            }
        }

        @Override
        public void onError(@NonNull Throwable e) {
            synchronized (this) {
                completed = true;
                buffer.clear();
            }

            down.onError(e);
        }

        @Override
        public void onComplete() {
            synchronized (this) {
                completed = true;

                System.out.println("End credits: " + requested);

                if (buffer.size() > 0)
                    down.onError(new IllegalArgumentException());
                else
                    down.onComplete();

            }
        }

        @Override
        public void request(long l) {
            System.out.println("downstream requested: " + l);

            if (l <= 0) {
                onError(new IllegalArgumentException("Non-positive request"));
                return;
            }

            synchronized (this) {
                //Already have max number of credits
                if (requested == Long.MAX_VALUE) return;

                //Avoid negative number because of overflow
                long newRequested = requested + l;
                if (newRequested < 0) newRequested = Long.MAX_VALUE;
                requested = newRequested;

                //Forward unbounded request
                if (newRequested == Long.MAX_VALUE) {
                    System.out.println("requested upstream: " + Long.MAX_VALUE);
                    up.request(Long.MAX_VALUE);
                    return;
                }

                //Try to deliver msgs if the buffer is not empty and
                // if there are credits available
                if (!buffer.isEmpty() && requested > 0)
                    tryDeliveringBufferedMsgs();

                if (completed)
                    down.onComplete();
                else if (requested > 0) {
                    System.out.println("requested upstream: " + 1);
                    up.request(1);
                }
            }
        }

        @Override
        public void cancel() {
            synchronized (this) {
                completed = true;
                buffer.clear();
            }

            up.cancel();
        }

        //Returns:
        //  -> 0 if the msg cannot be delivered yet
        //  -> 1 if the msg can be delivered
        //  -> -1 if the msg is a duplicate
        private int canItBeDelivered(CausalMessage<T> cm){
            var m_vv = cm.v;
            var node = cm.j;

            //First condition to be met for the msg to be delivered
            boolean b = vv[node] + 1 == m_vv[node];

            //if the first condition is false, it can be either because
            // the message is a duplicate, or because it is not yet time
            // to deliver the msg
            if(!b){
                //If the value present in the version vector is equal or higher than the
                // one present in the msg, than the msg is a duplicate
                if(vv[node] >= m_vv[node]) return -1;
                else return 0;
            }

            for(int k = 0 ; k < vv.length; k++)
                if(k != node && m_vv[k] > vv[k])
                    return 0;

            return 1;
        }

        private void receive(CausalMessage<T> cm){
            int canBeDelivered = canItBeDelivered(cm);
            if(canBeDelivered == 1)
                deliver(cm);
            else if(canBeDelivered == 0)
                buffer.add(cm);
            else //in case of a duplicate msg, we need to request upstream once again
                up.request(1);
        }

        private void tryDeliveringBufferedMsgs(){
            boolean stop = false;

            for(var it = buffer.iterator(); requested > 0 && !completed && !stop && it.hasNext() ; ){
                var cm = it.next();

                if(canItBeDelivered(cm) == 1) {
                    it.remove();
                    deliver(cm);
                    stop = true;
                }
                else break;
            }
        }

        private void deliver(CausalMessage<T> cm){
            requested--;
            vv[cm.j]++;
            down.onNext(cm.payload);
            tryDeliveringBufferedMsgs();
        }
    }
}

