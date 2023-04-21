package causalop;

import java.util.Arrays;
import java.util.Map;

public class CausalMessage<T> {
    int j;
    int[] v;
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

    @Override
    public String toString() {
        return "CausalMessage{" +
                "\n\tj=" + j +
                ",\n\tv=" + Arrays.toString(v) +
                ",\n\tpayload=" + payload +
                "\n}";
    }
}
