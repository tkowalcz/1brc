package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.CalculateAverage_tkowalcz;
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

public class SingleLaneMicrobenchmark extends OneBrcMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');
    private static final Vector<Byte> DELIMITER_MINUS_ASCII_ZERO_VECTOR = SPECIES.broadcast(';' - '0');

    private static final VectorShuffle<Byte>[] STOI_SHUFFLE_1 = new VectorShuffle[]{
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            VectorShuffle.fromValues(SPECIES, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
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
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
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

    static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

    public static VectorMask<Byte>[] createMasks32() {
        VectorMask<Byte>[] result = new VectorMask[34];
        result[0] = SPECIES.maskAll(false);
        result[1] = SPECIES.maskAll(false);

        int maskSource = 0x1;
        for (int i = 2; i < 34; i++) {
            result[i] = VectorMask.fromLong(SPECIES, maskSource);
            maskSource <<= 1;
            maskSource += 1;
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

            hashMap = new int[TABLE_SIZE];
            hashMapData = arena.allocate(10_000 * (4 + 100 + 28));
            output = arena.allocate(6 * 1024 * 1024 * 1024L, 1);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup(Level.Iteration)
    public void setupClean() {
        Arrays.fill(hashMap, 0);
        hashMapDataWriteIndex = 0;
        sequence = 0;

        installNewCity(0, 16, ByteVector.SPECIES_256.zero());
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
        long offset1 = 0;
        long end1 = inputData.byteSize() - ByteVector.SPECIES_256.vectorByteSize();

        long outputOffset = 0;
        long collisions = 0;

        while (offset1 < end1) {
            Vector<Byte> byteVector = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());

            Vector<Byte> byteVectorMinusZero1 = byteVector.sub(ASCII_ZERO);
            VectorMask<Byte> compare1 = byteVectorMinusZero1.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR);

            int compareInt1 = (int) compare1.toLong();
            int delimiterPosition1 = Integer.numberOfTrailingZeros(compareInt1) + 1;
            if (delimiterPosition1 < 12) {
                short lookupIndex1 = (short) (compareInt1 >> delimiterPosition1 & 0xF);

                int value1 = (int) byteVectorMinusZero1
                        // byteVectorMinusZero1
                        .rearrange(STOI_SHUFFLE_1[lookupIndex1])
                        .castShape(ShortVector.SPECIES_256, 0)
                        // .mul(STOI_MUL_LOOKUP[lookupIndex1])
                        .reduceLanesToLong(VectorOperators.ADD);
                output.set(ValueLayout.JAVA_INT_UNALIGNED, outputOffset, value1);
                // .intoMemorySegment(output, outputOffset, ByteOrder.nativeOrder());
                outputOffset += 4;
                offset1 += delimiterPosition1 + STOI_SIZE_LOOKUP[lookupIndex1];

                VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[delimiterPosition1];
                Vector<Byte> hashInput1 = ZERO.blend(byteVector, hashMask1);

                int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int index1 = perfectHash32_1 & TABLE_SIZE_MASK;
                int cityOffset1 = hashMap[index1];
                short cityId1 = hashMapData.get(ValueLayout.JAVA_SHORT, cityOffset1);
                int cityNameOffset = cityOffset1 + 4;

                ByteVector cityVector = ByteVector.fromMemorySegment(SPECIES, hashMapData, cityNameOffset, ByteOrder.nativeOrder());
                if (!cityVector.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                    collisions++;
                    cityId1 = hashMiss(cityOffset1, index1, hashInput1, delimiterPosition1);
                }

                output.set(ValueLayout.JAVA_SHORT, outputOffset, cityId1);
                outputOffset += 2;
            }
            else {
                offset1 = handleOver16(offset1);
            }
        }

        System.out.println(collisions);
        return offset1;
    }

    private short hashMiss(int cityOffset, int index, Vector<Byte> hashInput, int delimiterPosition) {
        if (cityOffset == 0) {
            return installNewCity(index, delimiterPosition, hashInput);
        }
        else {
            index = (index + 1) & TABLE_SIZE_MASK;

        }

        return 0;
    }

    private short installNewCity(int index, int delimiterPosition, Vector<Byte> hashInput) {
        short cityId1 = installEntry(index, delimiterPosition);

        hashInput.intoMemorySegment(hashMapData, hashMapDataWriteIndex, ByteOrder.nativeOrder());
        hashMapDataWriteIndex += ByteVector.SPECIES_128.vectorByteSize();
        return cityId1;
    }

    private int[] hashMap;

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x400000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = TABLE_SIZE - 1;

    private short sequence = 0;

    private short installEntry(int mapIndex, int cityNameSize) {
        short cityId = sequence++;

        hashMap[mapIndex] = hashMapDataWriteIndex;
        hashMapData.set(ValueLayout.JAVA_SHORT, hashMapDataWriteIndex, cityId);
        hashMapDataWriteIndex += 2;
        hashMapData.set(ValueLayout.JAVA_SHORT, hashMapDataWriteIndex, (short) cityNameSize);
        hashMapDataWriteIndex += 2;

        return cityId;
    }

    private static final Vector<Byte> NEWLINE_VECTOR_256 = ByteVector.SPECIES_256.broadcast('\n');

    private long handleOver16(long offset1) {
        VectorMask<Byte> newlines1;
        Vector<Byte> byteVector1;
        byteVector1 = ByteVector.SPECIES_256.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
        newlines1 = byteVector1.compare(VectorOperators.EQ, NEWLINE_VECTOR_256);

        int newline = newlines1.firstTrue();
        if (newline == 32) {
            while (inputData.get(ValueLayout.JAVA_BYTE, offset1) != '\n') {
                offset1++;
            }

            offset1++;
        }
        else {
            offset1 += newline + 1;
        }
        return offset1;
    }

    public static void main(String[] args) throws RunnerException {
        runWithPerfAsm(JoinForkMicrobenchmark.class.getSimpleName());
        // runWithPerfNorm(SingleLaneMicrobenchmark.class.getSimpleName());
        // run(JoinForkMicrobenchmark.class.getSimpleName());
        // runWithJFR(JoinForkMicrobenchmark.class.getSimpleName());
    }
}
