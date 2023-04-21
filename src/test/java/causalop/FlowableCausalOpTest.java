package causalop;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.internal.util.AtomicThrowable;
import io.reactivex.rxjava3.subscribers.DefaultSubscriber;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

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
    public void testReorder2() {
        var l = new ArrayList<String>();
        Flowable.just(
                        new CausalMessage<String>("c", 0, 2, 1),
                        new CausalMessage<String>("d", 1, 1, 2),
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

        Assert.assertArrayEquals(l.toArray(), new String[]{"a","b","c","d"});
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

    private class RandomMessagesGenerator{

        private final int seed;
        private Random random;
        private final int n;
        private final int m;
        private int payload;
        private Map<Integer, int[]> vvs; //map from id of node to current version vector of that node

        private List<CausalMessage<Integer>> msgs;
        private int created; // number of messages created

        private Map<Integer,List<Integer>> notDelivered; //map from node id to list of messages to be delivered for that node

        public RandomMessagesGenerator(int n, int m, int seed){
            this.seed = seed;
            this.n = n;
            this.m = m;
        }

        private void initialize(){
            payload = 0;
            vvs = new HashMap<>();
            msgs = new ArrayList<>();
            created = 0;
            notDelivered = new HashMap<>();
            random = new Random(seed);

            for(int i = 0; i < n; i++) {
                vvs.put(i, new int[n]);
                notDelivered.put(i, new ArrayList<>());
            }
        }

        private int canItBeDelivered(int node, int msg_index){
            CausalMessage<Integer> cm = msgs.get(msg_index);
            int[] vv = vvs.get(node);
            var m_vv = cm.v;
            var m_node = cm.j;

            //First condition to be met for the msg to be delivered
            boolean b = vv[m_node] + 1 == m_vv[m_node];

            //if the first condition is false, it can be either because
            // the message is a duplicate, or because it is not yet time
            // to deliver the msg
            if(!b){
                //If the value present in the version vector is equal or higher than the
                // one present in the msg, than the msg is a duplicate
                if(vv[m_node] >= m_vv[m_node]) return -1;
                else return 0;
            }

            for(int k = 0 ; k < vv.length; k++)
                if(k != m_node && m_vv[k] > vv[k])
                    return 0;

            return 1;
        }

        /**
         *
         * @param node node where the msg is supposed to be delivered
         * @param msg_index index of the msg to be delivered
         */
        private void deliver(int node, int msg_index){
            CausalMessage<Integer> cm = msgs.get(msg_index);
            int[] node_vv = vvs.get(node);
            node_vv[cm.getJ()]++;
            notDelivered.get(node).remove((Object) msg_index);
        }

        public List<CausalMessage<Integer>> generate(){
            initialize();

            while(created < m){

                //first decide if a new message or a duplicate will be created
                int rand = random.nextInt(0, 100);

                //10% chance to be a duplicate
                if(rand >= 90 && msgs.size() > 0){
                    rand = random.nextInt(0,msgs.size());
                    var cm = msgs.get(rand);
                    msgs.add(cm);
                }else{
                    //choose which node will create a msg
                    int node = random.nextInt(0, n);
                    int vv[] = vvs.get(node);
                    vv[node]++;

                    msgs.add(new CausalMessage<>(payload, node, Arrays.copyOf(vv, n)));
                    payload++;

                    for(int i = 0; i < n; i++){
                        if(i != node){
                            notDelivered.get(i).add(created);
                        }
                    }
                }

                //deliver msgs
                for(int i = 0; i < n; i++){
                    var listOfUndelivered = notDelivered.get(i);
                    boolean delivered = true;

                    while(listOfUndelivered.size() > 0 && delivered){
                        rand = random.nextInt(0, 100);

                        //decide if should deliver a message
                        if(rand <= 65){

                            for(int j = 0 ; j < listOfUndelivered.size() ; j++){
                                int msg_index = listOfUndelivered.get(j);
                                int canBeDelivered = canItBeDelivered(i,msg_index);
                                if(canBeDelivered == 1){
                                    deliver(i,msg_index);
                                    break;
                                }else if(canBeDelivered == 0)
                                    delivered = false;
                                else { //if(canBeDelivered == -1)
                                    listOfUndelivered.remove(msg_index);
                                    break;
                                }
                            }

                        }else delivered = false;
                    }
                }

                created++;
            }

            return msgs;
        }

    }



    private class Tuple<T,R>{
        public T fst;
        public R snd;

        public Tuple(T fst, R snd) {
            this.fst = fst;
            this.snd = snd;
        }
    }

    /**
     * @param n number of nodes
     * @param m number of messages
     * @param seed integer that allows reproducing the same results
     * @return a tuple containing two lists of causal messages containing an integer as payload.
     * The first list is sorted by the first element generated to last element generated.
     * The second list has the same objects as the first list, but they are shuffled.
     * Only duplicate messages have the same payload.
     */
    private Tuple<List<CausalMessage<Integer>>,List<CausalMessage<Integer>>> randomMessagesGenerator(int n, int m, int seed){
        RandomMessagesGenerator rmg = new RandomMessagesGenerator(n,m,seed);
        var original = rmg.generate();
        var shuffled = new ArrayList<>(original);
        Collections.shuffle(shuffled);
        return new Tuple<>(original, shuffled);
    }


    private boolean assertCausalOrder(List<CausalMessage<Integer>> original, List<Integer> results){
        Map<Integer, CausalMessage<Integer>> map = new HashMap<>();
        for(var cm : original)
            map.put(cm.payload, cm);

        CausalMessage<Integer> cm1 = map.get(results.get(0));

        for(int i = 1; i < results.size(); i++){

            var cm2 = map.get(results.get(i));

            int[] vv1 = cm1.getV(); //version vector 1
            int[] vv2 = cm2.getV();

            int counterHigher = 0, //number of clocks which value is higher in vv1
                counterLower = 0; //number of clocks which value is lower in vv1

            for(int j = 0; j < vv1.length; j++){
                int _v1 = vv1[j], _v2 = vv2[j];

                if (_v1 < _v2)
                    counterLower++;
                else if(_v1 > _v2)
                    counterHigher++;
            }

            int r;

            if(counterLower > 0) {
                //if only the lower counter is higher than 0, then vv1 is less than vv2
                if(counterHigher == 0)
                    r = -1;
                    //if both counters are higher than 0, then the messages are concurrent.
                    // Any of them can appear first since there is no causal relationship between them,
                    // but having in mind the order in which the objects are emitted,
                    // the r =ed value will be 1, so that the concurrent message that was emitted
                    // by the upstream first, will also be emitted first to the downstream
                else
                    r = 2;
            }
            else{
                //vv2 is less than vv1
                if(counterHigher > 0) r = 1;
                    //Version vectors are equal, so a duplicate message has arrived
                else r = 0;
            }

            //Assert fails in case of duplicate or in case cm2 should appear first than cm1
            if(r == 1 || r == 0)
                return false;

            cm1 = cm2;
        }

        return map.size() == results.size();
    }

    @Test
    public void testRandom() {

        //Change the arguments of randomMessagesGenerator to
        // get another sequence of messages. The first argument
        // is the number of nodes, the second is the number of
        // messages that should be generated, and the last argument
        // is the seed.

        int nr_of_nodes = 2,
            nr_of_msgs = 10,
            seed = 123123;

        var tuple = randomMessagesGenerator(nr_of_nodes, nr_of_msgs, seed);
        var l = new ArrayList<Integer>();

        Flowable.fromIterable(tuple.snd)
                .lift(new FlowableCausalOperator<Integer>(nr_of_nodes))
                .blockingSubscribe(new DefaultSubscriber<>(){
                    @Override
                    protected void onStart() {
                        request(1);
                    }

                    @Override
                    public void onNext(Integer i) {
                        l.add(i);
                        request(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        throwable.printStackTrace();
                    }

                    @Override
                    public void onComplete() {

                    }
                });

        System.out.println("***************** Generated messages ********************\n");
        for (var cm : tuple.fst)
            System.out.println(cm);
        System.out.println("\n*********************************************************\n\n");
        System.out.println("Order in which the messages were emitted: " + tuple.snd.stream().map(cm -> cm.payload).toList());
        System.out.println("Result: " + l);

        Assert.assertTrue(assertCausalOrder(tuple.fst, l));
    }
}
