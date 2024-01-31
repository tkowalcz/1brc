package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.CalculateAverage_tkowalcz;
import dev.morling.onebrc.RawHashMapUnsafe;
import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Arrays;

public class JoinForkMicrobenchmark extends OneBrcMicrobenchmark {

    private static final Vector<Byte> ASCII_ZERO = ByteVector.SPECIES_256.broadcast('0');
    private static final Vector<Byte> DELIMITER_MINUS_ASCII_ZERO_VECTOR = ByteVector.SPECIES_256.broadcast(';' - '0');

    private static final VectorShuffle<Byte>[] STOI_SHUFFLE_1 = new VectorShuffle[]{
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    };

    private static final VectorShuffle<Byte>[] STOI_SHUFFLE_2 = new VectorShuffle[]{
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    };

    private static final VectorShuffle<Byte>[] STOI_SHUFFLE_3 = new VectorShuffle[]{
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    };

    private static final VectorShuffle<Byte>[] STOI_SHUFFLE_4 = new VectorShuffle[]{
            VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    };

    // Values used to multiply digits of temperature measurement to get proper int. E.g. -12.3 will be multiplied by
    // 10th element (0, -100, -10, 0, -1) giving '-' * 0 + '1' * -100 + '2' * -10 + '.' * 0 + '3' * -1 = -123.
    // There are four combinations of possible mask results from comparing (less than) vector containing temperature
    // measurement with ASCII_ZERO. Hence, only four entries are populated.
    private static final ShortVector[] STOI_MUL_LOOKUP = {
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0)
    };

    static final VectorMask<Byte>[][] CITY_LOOKUP_MASKS = createMasks10x10();

    private static final Vector<Byte> ZERO = ByteVector.zero(ByteVector.SPECIES_256);
    public static final int THRESHOLD = 10;

    public static VectorMask<Byte>[][] createMasks10x10() {
        VectorMask<Byte>[][] result = new VectorMask[11][];
        boolean[] source = new boolean[32];

        for (int i = 1; i <= THRESHOLD; i++) {
            result[i] = new VectorMask[11];
            fillLowerToN(source, i - 1);
            for (int j = 1; j <= THRESHOLD; j++) {
                fillUpperToN(source, j - 1);
                result[i][j] = VectorMask.fromArray(ByteVector.SPECIES_256, source, 0);
            }
        }

        return result;
    }

    private static void fillLowerToN(boolean[] array, int n) {
        Arrays.fill(array, 0, 16, false);
        Arrays.fill(array, 0, n, true);
    }

    private static void fillUpperToN(boolean[] array, int n) {
        Arrays.fill(array, 16, 32, false);
        Arrays.fill(array, 16, 16 + n, true);
    }

    private static final int[] STOI_SIZE_LOOKUP = {
            0, 0, 0, 0, 0, 4, 6, 0,
            0, 0, 5, 5, 0, 0, 0, 0
    };

    private Arena arena;
    private MemorySegment inputData;

    private MemorySegment output;

    private MemorySegment hashMapData;
    private int hashMapDataWriteIndex;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);

            hashMap = new RawHashMapUnsafe(arena);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // "London;14.6\nUpington;20.4\nPalerm"
    // London;-12.3\n
    // ......T.TT.T. -> .T.TT.T...... -> T.TT. -> 0x6
    // London;12.3\n
    // ......TTT.T. -> ..T.TTT...... -> .T.TT -> 0xB
    // London;-2.3\n
    // ......T.T.T. -> ..T.T.T...... -> .T.T. -> 0xA
    // London;2.3\n
    // ......TT.T. -> .T.TT...... -> .T.T -> 0x5
    // AAABBCCDDEE;1
    // ...........T.
    // Benchmark Mode Cnt Score Error Units
    // SmallVectorMicrobenchmark.justParseTemps avgt 2 5.167 s/op
    @Benchmark
    public long justParseTemps() {
        long stride = inputData.byteSize() / 2;

        long offset1 = 0;
        long end1 = stride - ByteVector.SPECIES_128.vectorByteSize();

        long offset2 = CalculateAverage_tkowalcz.findPastNewline(inputData, end1);
        long end2 = stride + stride - ByteVector.SPECIES_128.vectorByteSize();

        long outputOffset = 0;
        long collisions = 0;

        while (offset1 < end1 && offset2 < end2) {
            Vector<Byte> byteVector1 = ByteVector.SPECIES_128.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            Vector<Byte> byteVector2 = ByteVector.SPECIES_128.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());

            Vector<Byte> combinedVector = byteVector1
                    .castShape(ByteVector.SPECIES_256, 0)
                    .add(byteVector2.castShape(ByteVector.SPECIES_256, -1));

            Vector<Byte> byteVectorMinusZero = combinedVector.sub(ASCII_ZERO);
            VectorMask<Byte> compare1 = byteVectorMinusZero.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR);

            int merged = (int) compare1.toLong();
            short compareInt1 = (short) merged;
            short compareInt2 = (short) (merged >> 16);

            int delimiterPosition1 = Integer.numberOfTrailingZeros(compareInt1) + 1;
            int delimiterPosition2 = Integer.numberOfTrailingZeros(compareInt2) + 1;

            if (delimiterPosition1 > THRESHOLD) {
                offset1 = handleOver16(offset1);
                continue;
            }

            if (delimiterPosition2 > THRESHOLD) {
                offset2 = handleOver16(offset2);
                continue;
            }

            short lookupIndex1 = (short) (compareInt1 >> delimiterPosition1 & 0xF);
            short lookupIndex2 = (short) (compareInt2 >> delimiterPosition2 & 0xF);

            VectorMask<Byte> hashMask = CITY_LOOKUP_MASKS[delimiterPosition1][delimiterPosition2];
            Vector<Byte> hashInput = ZERO.blend(combinedVector, hashMask);

            Vector<Byte> hashInput1 = hashInput.castShape(ByteVector.SPECIES_128, 0);
            Vector<Byte> hashInput2 = hashInput.castShape(ByteVector.SPECIES_128, 1);

            LongVector asLongs = hashInput.reinterpretAsLongs();
            long l1 = asLongs.lane(0) + asLongs.lane(1);
            long l2 = asLongs.lane(2) + asLongs.lane(3);
            int index1 = (int) ((l1 + (l1 >> 32)) & TABLE_SIZE_MASK);
            int index2 = (int) ((l2 + (l2 >> 32)) & TABLE_SIZE_MASK);
            int cityNameOffset1 = (index1 << 5) + (index1 << 4);
            int cityNameOffset2 = (index2 << 5) + (index2 << 4);

            ByteVector cityVector1 = ByteVector.fromMemorySegment(ByteVector.SPECIES_128, hashMap.hashMapData, cityNameOffset1, ByteOrder.nativeOrder());
            if (!cityVector1.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                collisions++;
                hashMap.installNewCity(cityNameOffset1, delimiterPosition1, hashInput1);
            }

            ByteVector cityVector2 = ByteVector.fromMemorySegment(ByteVector.SPECIES_128, hashMap.hashMapData, cityNameOffset2, ByteOrder.nativeOrder());
            if (!cityVector2.compare(VectorOperators.EQ, hashInput2).allTrue()) {
                collisions++;
                hashMap.installNewCity(cityNameOffset2, delimiterPosition2, hashInput2);
            }

            offset1 += delimiterPosition1 + STOI_SIZE_LOOKUP[lookupIndex1];
            offset2 += delimiterPosition2 + STOI_SIZE_LOOKUP[lookupIndex2];
        }

        System.out.println(collisions);
        return offset1;
    }

    private RawHashMapUnsafe hashMap;

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x400000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = TABLE_SIZE - 1;

    private static final Vector<Byte> NEWLINE_VECTOR_256 = ByteVector.SPECIES_256.broadcast('\n');

    private long handleOver16(long offset1) {
        while (inputData.get(ValueLayout.JAVA_BYTE, offset1) != '\n') {
            offset1++;
        }

        return offset1 + 1;
    }

    public static void main(String[] args) throws RunnerException {
        // runWithPerfAsm(JoinForkMicrobenchmark.class.getSimpleName());
        runWithPerfNorm(JoinForkMicrobenchmark.class.getSimpleName());
        // run(JoinForkMicrobenchmark.class.getSimpleName());
        // runWithJFR(JoinForkMicrobenchmark.class.getSimpleName());
    }
}
