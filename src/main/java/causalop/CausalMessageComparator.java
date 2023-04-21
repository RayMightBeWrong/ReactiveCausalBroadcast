package causalop;

import java.util.Comparator;
import java.util.Map;

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
            return _v1.compareTo(_v2);
        }

        // messages sent from different nodes

        if (vv1.size() != 1 || vv2.size() != 1) {

            if (vv1.containsKey(cm2.j)){

                //if each vector contains a value for the sender of the other vector
                if(vv2.containsKey(cm1.j)){
                    Integer _vv1_cm1 = vv1.get(cm1.j), // _vv1_cm1 means version value of the sender of cm1 present in vv1
                            _vv1_cm2 = vv1.get(cm2.j),
                            _vv2_cm1 = vv2.get(cm1.j),
                            _vv2_cm2 = vv2.get(cm2.j);

                    if(_vv1_cm1 > _vv2_cm1) {
                        return 1;

                    }else if(_vv1_cm1 == _vv2_cm1){
                      if(_vv1_cm2 >= _vv2_cm2)
                        throw new IllegalArgumentException();
                          // considering {2, 2} -> a and {2, 1} -> b
                          // node a must receive b=2 to send a=2
                          // but node b when sending b=1, has already received a=2
                          // which makes no logical sense
                      if(_vv1_cm2 < _vv2_cm2)
                        return -1;

                    }else{ // if _vv1_cm1 < _vv2_cm1
                      if(_vv1_cm2 >= _vv2_cm2)
                        throw new IllegalArgumentException();
                          // considering {2, 2} -> a and {3, 1} -> b
                          // node a must receive b=2 to send a=2
                          // but node b when sending b=1, has already a=3
                          // which makes no logical sense
                      else
                        return -1;
                    }

                    /*
                        node1 | node2 | return
                        -----+-----+-------
                          >  |  >  |   1
                        -----+-----+-------
                          >  |  =  |   1
                        -----+-----+-------
                          >  |  <  |   1 (concurrent)
                        -----+-----+-------
                          =  |  >  | Impossible
                        -----+-----+-------
                          =  |  =  | Impossible
                        -----+-----+-------
                          =  |  <  |  -1
                        -----+-----+-------
                          <  |  >  | Impossible
                        -----+-----+-------
                          <  |  =  | Impossible
                        -----+-----+-------
                          <  |  <  |  -1
                        -----+-----+-------

                         When the messages are concurrent we want cm1 to appear after cm2, therefore the return value is 1
                    */

                }else { // if (!vv2.containsKey(cm1.j))

                    /*
                         cm2 | return
                        -----+-------
                          <  |   1  (concurrent)
                        -----+-------
                          =  |   1
                        -----+-------
                          >  |   1
                        -----+-------
                    */

                    return 1;
                }

            } else if (vv2.containsKey(cm1.j)) {
                /*
                         cm1 | return
                        -----+-------
                          <  |  -1  (concurrent)
                        -----+-------
                          =  |  -1
                        -----+-------
                          >  |   1 (concurrent or even if we had the full vv's we would reach the conclusion that vv2 < vv1)
                        -----+-------
                    */

                Integer _vv1_cm1 = vv1.get(cm1.j), // _vv1_cm1 means version value of the sender of cm1 present in vv1
                        _vv2_cm1 = vv2.get(cm1.j);

                if(_vv1_cm1 <= _vv2_cm1)
                    return -1;
                else
                    return 1;
            }
        }
        // if the size of both vectors equals to 1, then they are concurrent
        //  because the only value sent corresponds to the version of the node
        //  that sent the message, and since the senders are different,
        //  the messages are concurrent.
        //  The value 1 is returned because we want to maintain the order in which
        //  the messages arrived

        return 1;
    }
}
