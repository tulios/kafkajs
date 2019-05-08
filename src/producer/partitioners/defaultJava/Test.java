import java.util.Arrays;
import java.util.List;

/**
 * Based on:
 * https://github.com/apache/kafka/blob/3cdc78e6bb1f83973a14ce1550fe3874f7348b05/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java#L69
 * https://github.com/apache/kafka/blob/3cdc78e6bb1f83973a14ce1550fe3874f7348b05/clients/src/main/java/org/apache/kafka/common/utils/Utils.java
 * 
 * Used to validate the Javascript implementation of the Java-compatible murmur2
 * hashing function
 * 
 * javac Test.java && java Test
 */

public class Test {
    public static void main(String[] args) {
        List<String> keys = Arrays.asList("0", "1", "128", "2187", "16384", "78125", "279936", "823543", "2097152",
                "4782969", "10000000", "19487171", "35831808", "62748517", "105413504", "170859375");

        for (String key : keys) {
            byte[] keyBytes = key.getBytes();

            int murmur = murmur2(keyBytes);
            System.out.println("\"" + key + "\": " + murmur);
        }

    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * 
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    private static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16)
                    + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
        case 3:
            h ^= (data[(length & ~3) + 2] & 0xff) << 16;
        case 2:
            h ^= (data[(length & ~3) + 1] & 0xff) << 8;
        case 1:
            h ^= data[length & ~3] & 0xff;
            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }
}
