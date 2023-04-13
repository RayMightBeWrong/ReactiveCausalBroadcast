package causalop;

import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;

public class CasualMessageComparator<T> implements Comparator<CausalMessage<T>> {

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

        //if only the lower counter is higher than 0, then vv1 is less than vv2
        //if both counters are higher than 0, then the messages are concurrent
        //any of them can appear first since there is no causal relationship between them
        if(counterLower > 0)
            return -1;
        else if(counterLower == 0){
            //vv2 is less than vv1
            if(counterHigher > 0) return 1;
            //Version vectors are equal, so a duplicate message has arrived
            else return 0;
        }

         return 0;
    }


    public static void main(String[] args) {
        TreeSet<CausalMessage<String>> set = new TreeSet<>(new CasualMessageComparator<>());

        //testok
        //set.add(new CausalMessage<String>("a", 1, 0, 1));
        //set.add(new CausalMessage<String>("b", 0, 1, 0));
        //set.add(new CausalMessage<String>("c", 1, 1, 2));

        //testreorder
        //set.add(new CausalMessage<String>("c", 1, 1, 2));
        //set.add(new CausalMessage<String>("a", 1, 0, 1));
        //set.add(new CausalMessage<String>("b", 0, 1, 0));

        //testdupl
        set.add(new CausalMessage<String>("a", 1, 0, 1));
        set.add(new CausalMessage<String>("b", 0, 1, 0));
        set.add(new CausalMessage<String>("a", 1, 0, 1));
        set.add(new CausalMessage<String>("c", 1, 1, 2));

        //testgap
        //set.add(new CausalMessage<String>("c", 1, 1, 2));
        //set.add(new CausalMessage<String>("a", 1, 0, 1));

        int i = 0;
        for (var it = set.iterator(); it.hasNext();){
            var x = it.next();
            System.out.println(x);
            if(i == 0)
                it.remove();
            i++;
        }
        System.out.println(set);
    }
}
