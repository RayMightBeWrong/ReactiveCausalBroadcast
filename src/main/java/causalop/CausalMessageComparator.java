package causalop;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.DefaultSubscriber;

public class CausalMessageComparator<T> implements Comparator<CausalMessage<T>> {
    int n;

    public CausalMessageComparator(int n){
        this.n = n;
    }

    @Override
    public int compare(CausalMessage<T> cm1, CausalMessage<T> cm2){
        Map<Integer, Integer> vv1 = cm1.getV(); //version vector 1
        Map<Integer, Integer> vv2 = cm2.getV(); //version vector 2

        // messages sent from the same node
        if (cm1.j == cm2.j){
            Integer _v1 = vv1.get(cm1.j);
            Integer _v2 = vv2.get(cm2.j);

            if (_v1 > _v2)
                return 1;
            else if (_v1 > _v2)
                return -1;
            else
                return 0;
        }

        // messages sent from different nodes
        if (vv1.size() == 1 && vv2.size() == 1){
            return 1; // concurrent messages
        }
        else{
            if (vv1.containsKey(cm2.j) && vv2.containsKey(cm1.j)){
                if (vv1.get(cm1.j) > vv2.get(cm1.j) && vv1.get(cm2.j) >= vv2.get(cm2.j))         return -1;
                else if (vv1.get(cm1.j) <= vv2.get(cm1.j) && vv1.get(cm2.j) < vv2.get(cm2.j))    return 1;
                else if (vv1.get(cm1.j) > vv2.get(cm1.j) && vv1.get(cm2.j) < vv2.get(cm2.j))    return 1; // concorrentes
                else
                    return 0;
            }
            else if (vv1.containsKey(cm2.j)){
                // possible to compare
                if (vv1.get(cm2.j) >= vv2.get(cm2.j)){
                    return 1;
                }
            }
            else if (vv2.containsKey(cm1.j)){
                // possible to compare
                if (vv2.get(cm1.j) >= vv1.get(cm1.j)){
                    return -1;
                }
            }

            // concurrent messages
            return 1;
        }
    }
}
