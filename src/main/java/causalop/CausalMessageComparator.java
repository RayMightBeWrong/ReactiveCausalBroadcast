package causalop;

import java.util.Comparator;
import java.util.TreeSet;

public class CausalMessageComparator<T> implements Comparator<CausalMessage<T>> {

    @Override
    public int compare(CausalMessage<T> cm1, CausalMessage<T> cm2){
        int[] vv1 = cm1.getV(); //version vector 1
        int[] vv2 = cm2.getV();

        int counterHigher = 0, //number of clocks which value is higher in vv1
                counterLower = 0; //number of clocks which value is lower in vv1

        for(int i = 0; i < vv1.length; i++){
            int _v1 = vv1[i], _v2 = vv2[i];

            if (_v1 < _v2)
                counterLower++;
            else if(_v1 > _v2)
                counterHigher++;
        }

        if(counterLower > 0) {
            //if only the lower counter is higher than 0, then vv1 is less than vv2
            if(counterHigher == 0)
                return -1;
                //if both counters are higher than 0, then the messages are concurrent.
                // Any of them can appear first since there is no causal relationship between them,
                // but having in mind the order in which the objects are emitted,
                // the returned value will be 1, so that the concurrent message that was emitted
                // by the upstream first, will also be emitted first to the downstream
            else
                return 1;
        }
        else{
            //vv2 is less than vv1
            if(counterHigher > 0) return 1;
                //Version vectors are equal, so a duplicate message has arrived
            else return 0;
        }
    }
}