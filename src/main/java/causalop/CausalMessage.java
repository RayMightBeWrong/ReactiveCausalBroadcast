package causalop;

import java.util.Arrays;

public class CausalMessage<T> {
    int j, v[];
    public T payload;

    public CausalMessage(T payload, int j, int... v) {
        this.payload = payload;
        this.j = j;
        this.v = v;
    }


    public int[] getV() {
        return v;
    }

    public int getJ() {
        return j;
    }
}
