package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.IoUtil;
import dev.morling.onebrc.RawHashMapUnsafe;
import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class JustCitiesInASmallVectorMicrobenchmark extends OneBrcMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');

    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');

    private static final Vector<Byte> NEWLINE_VECTOR_256 = ByteVector.SPECIES_256.broadcast('\n');

    // Values used to multiply digits of temperature measurement to get proper int. E.g. -12.3 will be multiplied by
    // 10th element (0, -100, -10, 0, -1) giving '-' * 0 + '1' * -100 + '2' * -10 + '.' * 0 + '3' * -1 = -123.
    // There are four combinations of possible mask results from comparing (less than) vector containing temperature
    // measurement with ASCII_ZERO. Hence, only four entries are populated.
    private static final short[][] STOI_MUL_LOOKUP_TEMPLATE = {
            new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
            new short[]{ 0, 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
            new short[]{ 0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
            new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
            new short[]{ 0, 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
            new short[]{ 0, 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
    };

    private static final short[] HASH_MUL_LOOKUP_TEMPLATE = {
            157, 149, 137, 127, 109, 103, 97, 83,
            73, 67, 59, 47, 41, 31, 23, 17,
    };
    private static final ShortVector[][] STOI_MUL_LOOKUP = createMulVariations(STOI_MUL_LOOKUP_TEMPLATE);

    private static ShortVector[][] createMulVariations(short[][] template) {
        ShortVector[][] result = new ShortVector[12][];

        for (int i = 0; i < result.length; i++) {
            result[i] = new ShortVector[6];
            for (int j = 0; j < template.length; j++) {
                result[i][j] = ShortVector.fromArray(
                        ShortVector.SPECIES_256,
                        shiftRightBy(template[j], i),
                        0);
            }
        }

        return result;
    }

    private static short[] shiftRightBy(short[] template, int amount) {
        short[] clone = template.clone();
        for (int i = 0; i < amount; i++) {
            shiftRight(clone, HASH_MUL_LOOKUP_TEMPLATE[i]);
        }

        return clone;
    }

    private static void shiftRight(short[] template, short fillValue) {
        for (int i = template.length - 1; i > 0; i--) {
            template[i] = template[i - 1];
        }

        template[0] = fillValue;
    }

    // We also need to know the size of temperature measurement in characters, lookup table works the same way as STOI_MUL_LOOKUP.
    private static final int[] STOI_SIZE_LOOKUP = { 0, 6, 4, 0, 5, 5 };

    private static final String FILE = "measurements.txt";

    private Arena arena;
    private MemorySegment inputData;

    private RawHashMapUnsafe hashMap;
    private MemorySegment hashMapData;
    private int hashMapDataWriteIndex;

    private short sequence = 0;

    // private MemoryMappedFile dataPointer;
    private MemorySegment output;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);
            output = arena.allocate(5 * 1024 * 1024 * 1024L, 1);

            hashMap = new RawHashMapUnsafe(arena);
            // hashMapData = arena.allocate(10_000 * (4 + 100 + 28));
            // hashMapDataWriteIndex = 4;
            // sequence = 0;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @TearDown
    public void tearDown() {
        arena.close();
    }

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x4000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = TABLE_SIZE - 1;

    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

    static final VectorMask<Byte>[] CITY_LOOKUP_MASK_BYTE = createMasksByte32();

    // private static final VectorShuffle<Short>[][] TEMP_SHUFFLE = new VectorShuffle[][]{
    // null,
    // new VectorShuffle[]{
    // null,
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // null,
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // }
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 3, 4, 5, 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 4, 5, 6, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 5, 6, 7, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 6, 7, 8, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 7, 8, 9, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 8, 9, 10, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 9, 10, 11, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 10, 11, 12, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 11, 12, 13, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // VectorShuffle.fromValues(ShortVector.SPECIES_256, 12, 13, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    // };

    // 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 | -12.3
    // 0, 1, 2, 4
    // 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 | 2.3
    // 0, 1, 2, 4
    // 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 | 12.3
    // 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 | -2.3

    // public static VectorMask<Short>[] createMasks32() {
    // VectorMask<Short>[] result = new VectorMask[33];
    // result[0] = ShortVector.SPECIES_256.maskAll(false);
    //
    // int maskSource = 0x1;
    // for (int i = 1; i < 33; i++) {
    // result[i] = VectorMask.fromLong(ShortVector.SPECIES_256, maskSource);
    // maskSource <<= 1;
    // maskSource += 1;
    // }
    //
    // return result;
    // }

    public static VectorMask<Byte>[] createMasks32() {
        VectorMask<Byte>[] result = new VectorMask[33];
        result[0] = SPECIES.maskAll(false);

        int maskSource = 0x1;
        for (int i = 1; i < 33; i++) {
            result[i] = VectorMask.fromLong(SPECIES, maskSource);
            maskSource <<= 1;
            maskSource += 1;
        }

        return result;
    }

    public static VectorMask<Byte>[] createMasksByte32() {
        VectorMask<Byte>[] result = new VectorMask[33];
        result[0] = ByteVector.SPECIES_128.maskAll(false);

        int maskSource = 0x1;
        for (int i = 1; i < 33; i++) {
            result[i] = VectorMask.fromLong(ByteVector.SPECIES_128, maskSource);
            maskSource <<= 1;
            maskSource += 1;
        }

        return result;
    }

    // "London;14.6\nUpington;20.4\nPalerm"
    @Benchmark
    public long justParseTemps() {
        long stride = inputData.byteSize() / 2;

        long offset1 = 0;
        long end1 = stride - SPECIES.vectorByteSize();

        long offset2 = stride;
        long end2 = stride + stride - SPECIES.vectorByteSize();

        long offset3 = stride + stride;
        long end3 = stride + stride + stride - SPECIES.vectorByteSize();

        long offset4 = stride + stride + stride;
        long end4 = stride + stride + stride + stride - SPECIES.vectorByteSize();

        long[] outputOffset = new long[1];

        return process(offset1, end1, offset2, end2, offset3, end3, offset4, end4, outputOffset);
    }

    private long process(long offset1, long end1, long offset2, long end2, long offset3, long end3, long offset4, long end4, long[] outputOffset) {
        /*
         * // VectorMask<Byte> newlineMask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
         * // long mask1 = newlineMask1.toLong();
         * // int delimiterPosition1 = Long.numberOfLeadingZeros(mask1);
         * // int lineEnd1 = (mask1 >> delimiterPosition1);
         *
         */
        long collisions = 0;
        while (offset1 < end1 && offset2 < end2 && offset3 < end3 && offset4 < end4) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            VectorMask<Byte> newlineMask1 = byteVector1.compare(VectorOperators.EQ, NEWLINE_VECTOR);
            VectorMask<Byte> delimiterPositions1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR);
            int lineEnd1 = newlineMask1.firstTrue();
            int delimiterPosition1 = delimiterPositions1.firstTrue();

            if (lineEnd1 == 16) {
                offset1 = handleOver16(offset1);
                continue;
            }

            offset1 += lineEnd1 + 1;
            VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[delimiterPosition1];
            Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);

            int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index1 = perfectHash32_1 & TABLE_SIZE_MASK;
            int cityOffset1 = (index1 << 5) + (index1 << 4);

            ByteVector cityVector1 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityOffset1, ByteOrder.nativeOrder());
            if (!cityVector1.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                collisions++;
                hashMap.installNewCity(index1, delimiterPosition1, hashInput1);
            }

            output.set(ValueLayout.JAVA_INT, outputOffset[0], index1);
            outputOffset[0] += 4;

            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            VectorMask<Byte> newlineMask2 = byteVector2.compare(VectorOperators.EQ, NEWLINE_VECTOR);
            VectorMask<Byte> delimiterPositions2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR);
            int lineEnd2 = newlineMask2.firstTrue();
            int delimiterPosition2 = delimiterPositions2.firstTrue();

            if (lineEnd2 == 16) {
                offset2 = handleOver16(offset2);
                continue;
            }

            offset2 += lineEnd2 + 1;
            VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[delimiterPosition2];
            Vector<Byte> hashInput2 = ZERO.blend(byteVector2, hashMask2);

            int perfectHash32_2 = hashInput2.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index2 = perfectHash32_2 & TABLE_SIZE_MASK;
            int cityOffset2 = (index2 << 5) + (index2 << 4);

            ByteVector cityVector2 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityOffset2, ByteOrder.nativeOrder());
            if (!cityVector2.compare(VectorOperators.EQ, hashInput2).allTrue()) {
                collisions++;
                hashMap.installNewCity(index2, delimiterPosition2, hashInput2);
            }

            output.set(ValueLayout.JAVA_INT, outputOffset[0], index2);
            outputOffset[0] += 4;
        }

        return collisions;
    }

    private long processVector(long offset2, long[] outputOffset) {
        Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
        VectorMask<Byte> newlineMask2 = byteVector2.compare(VectorOperators.EQ, NEWLINE_VECTOR);
        VectorMask<Byte> delimiterPositions2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR);
        int lineEnd2 = newlineMask2.firstTrue();
        int delimiterPosition2 = delimiterPositions2.firstTrue();

        if (lineEnd2 == 16) {
            return handleOver16(offset2);
        }

        offset2 += lineEnd2 + 1;
        VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[delimiterPosition2];
        Vector<Byte> hashInput2 = ZERO.blend(byteVector2, hashMask2);

        int perfectHash32_2 = hashInput2.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
        int index2 = perfectHash32_2 & TABLE_SIZE_MASK;
        int cityOffset2 = (index2 << 5) + (index2 << 4);

        ByteVector cityVector2 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityOffset2, ByteOrder.nativeOrder());
        if (!cityVector2.compare(VectorOperators.EQ, hashInput2).allTrue()) {
            hashMap.installNewCity(index2, delimiterPosition2, hashInput2);
        }

        output.set(ValueLayout.JAVA_INT, outputOffset[0], index2);
        outputOffset[0] += 4;
        return offset2;
    }

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
        // runWithJFR(JustCitiesInASmallVectorMicrobenchmark.class.getSimpleName());
        // run(JustCitiesInASmallVectorMicrobenchmark.class.getSimpleName());
        runWithPerfAsm(JustCitiesInASmallVectorMicrobenchmark.class.getSimpleName());
        // runWithPerfNorm(JustCitiesInASmallVectorMicrobenchmark.class.getSimpleName());
    }
}
