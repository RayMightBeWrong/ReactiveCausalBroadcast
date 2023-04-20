package causalop;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.ObservableOperator;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.observers.DisposableObserver;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class CausalOperator<T> implements ObservableOperator<T, CausalMessage<T>> {
    
    private final int n;
    private final Set<CausalMessage<T>> buffer;
    private Map<Integer, Integer> vv;
    
    public CausalOperator(int n) {
        this.n = n;
        this.buffer = new TreeSet<>(new CausalMessageComparator<T>(n));
        this.vv = new HashMap<>();
        for (int i = 0; i < n; i++)
            this.vv.put(i, 0);
    }

    @Override
    public @NonNull Observer<? super CausalMessage<T>> apply(@NonNull Observer<? super T> down) throws Throwable {
        return new DisposableObserver<CausalMessage<T>>() {
            @Override
            public void onNext(@NonNull CausalMessage<T> m) {
                receive(m);
            }

            @Override
            public void onError(@NonNull Throwable e) {
                down.onError(e);
            }

            @Override
            public void onComplete() {
                if(buffer.size() > 0)
                    down.onError(new IllegalArgumentException());
                else
                    down.onComplete();
            }

            //Returns:
            //  -> 0 if the msg cannot be delivered yet
            //  -> 1 if the msg can be delivered
            //  -> -1 if the msg is a duplicate
            private int canItBeDelivered(CausalMessage<T> cm){
                var m_vv = cm.v;
                var node = cm.j;

                //First condition to be met for the msg to be delivered
                boolean b = vv.get(node) + 1 == m_vv.get(node);

                //if the first condition is false, it can be either because
                // the message is a duplicate, or because it is not yet time
                // to deliver the msg
                if(!b){
                    //If the value present in the version vector is equal or higher than the
                    // one present in the msg, than the msg is a duplicate
                    if(vv.get(node) >= m_vv.get(node)) return -1;
                    else return 0;
                }

                for (Map.Entry<Integer, Integer> entry: m_vv.entrySet()){
                    if (entry.getKey() != node && entry.getValue() > vv.get(entry.getKey())){
                        return 0;
                    }
                }

                return 1;
            }

            private void receive(CausalMessage<T> cm){
                int canBeDelivered = canItBeDelivered(cm);
                if(canBeDelivered == 1) {
                    deliver(cm);
                    tryDeliveringBufferedMsgs();
                }
                else if(canBeDelivered == 0)
                    buffer.add(cm);
                //else ignore the msg
            }

            private void tryDeliveringBufferedMsgs(){
                for(var it = buffer.iterator(); it.hasNext() ; ){
                    var cm = it.next();

                    if(canItBeDelivered(cm) == 1) {
                        it.remove();
                        deliver(cm);
                    }
                    else break;
                }
            }

            private void deliver(CausalMessage<T> cm){
                vv.put(cm.j, vv.get(cm.j) + 1);
                down.onNext(cm.payload);
            }
        };
    }
}
