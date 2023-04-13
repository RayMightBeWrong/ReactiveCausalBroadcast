package causalop;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.ObservableOperator;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.observers.DisposableObserver;

import java.util.Set;
import java.util.TreeSet;

public class CausalOperator<T> implements ObservableOperator<T, CausalMessage<T>> {
    
    private final int n;
    private final Set<CausalMessage<T>> buffer = new TreeSet<>(new CasualMessageComparator());
    private int[] vv;
    
    public CausalOperator(int n) {
        this.n = n;
        this.vv = new int[n];
    }

    @Override
    public @NonNull Observer<? super CausalMessage<T>> apply(@NonNull Observer<? super T> down) throws Throwable {
        return new DisposableObserver<CausalMessage<T>>() {
            @Override
            public void onNext(@NonNull CausalMessage<T> m) {
                
                boolean toBuffer = false;
                if (vv[m.j] + 1 == v[m.j])
                    toBuffer = true;


                //    if i != node_id and vv[i] < clock[i]:
                for (int i = 0; i < n; i++){
                    if (i )
                        toBuffer = true;
                }

                if (toBuffer)
                    buffer.add(m);

                vv[m.j] += 1;
                down.onNext(m.payload); // FIXME
            }

            @Override
            public void onError(@NonNull Throwable e) {
                down.onError(e); // FIXME
            }

            @Override
            public void onComplete() {
                down.onComplete(); // FIXME
            }
        };
    }
}
