package causalop;

import java.util.Comparator;
import java.util.TreeSet;

public class CausalMessageComparator<T> implements Comparator<CausalMessage<T>> {

    @Override
    public int compare(CausalMessage<T> cm1, CausalMessage<T> cm2){
        int[] vv1 = cm1.getV(); //version vector 1
        int[] vv2 = cm2.getV();

        boolean hasLower = false, //if there is a clock where the value is lower in vv1
                hasHigher = false; //if there is a clock where the value is higher in vv1

        for(int i = 0; i < vv1.length && (!hasHigher || !hasLower) ; i++){
            int _v1 = vv1[i], _v2 = vv2[i];

            if (_v1 < _v2)
                hasLower = true;
            else if(_v1 > _v2)
                hasHigher = true;
        }

        if(hasLower) {
            //if only the lower counter is higher than 0, then vv1 is less than vv2
            if(!hasHigher)
                return -1;
            else {
                //if both counters are higher than 0, then the messages are concurrent.
                // Any of them can appear first since there is no causal relationship between them,
                // but we will force an order to allow reproducibility when the order of the msgs
                // are different
                return cm1.getJ() < cm2.getJ() ? -1 : 1;
            }
        }
        else{
            //vv2 is less than vv1
            if(hasHigher) return 1;
                //Version vectors are equal, so a duplicate message has arrived
            else return 0;
        }
    }


    /*
        Durante a fase de testes, foi encontrado um caso em que duas mensagens duplicadas eram inseridas no TreeSet,
         algo que não deveria ser possível. Testamos várias vezes o nosso comparator e este parecia estar a devolver
         os resultados esperados, ou seja, no caso de duas mensagens serem iguais, este devolvia um 0.
        A solução que encontramos no momento foi sempre que considerávamos uma como entregue , percorremos o treeSet
        à procura de duplicados e, caso existam, removemos.
     */
    public static void main(String[] args) {
        var cm1 = new CausalMessage<>(6, 1, 4, 3);
        var cm2 = new CausalMessage<>(7, 0, 5, 2);
        var cm3 = new CausalMessage<>(8, 1, 5, 4);

        System.out.println(new CausalMessageComparator<Integer>().compare(cm1, cm1));

        var treeset = new TreeSet<CausalMessage<Integer>>(new CausalMessageComparator<Integer>());
        treeset.add(cm1);
        treeset.add(cm2);
        treeset.add(cm3);

        System.out.println(treeset);

        treeset.add(cm1);

        System.out.println(treeset);

    }
}