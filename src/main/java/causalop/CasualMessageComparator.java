//package causalop;
public class CasualMessageComparator implements Comparator {
    @Override
    public int compare(CausalMessage<T> o1, CausalMessage<T> o2){
        int[] versionVector1 = o1.getV();
        int[] versionVector2 = o2.getV();

        int node1 = o1.getJ();
        int node2 = o2.getJ();

        /*
         * If causal messages are from the same node and have the same version vector the messages are the same
        */
        if(node1 == node2 && versionVector1[node1] == versionVector2[node1]){
            return 0;
        }        

        /* 
         * when the casual messages aren´t from the same node
         * - check both version vectors 
        */
        

         
    }
}
