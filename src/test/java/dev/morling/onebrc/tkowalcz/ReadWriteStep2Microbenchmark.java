package dev.morling.onebrc.tkowalcz;

import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * 4 way writing out one way (128MB)
 * Benchmark                         Mode  Cnt  Score   Error  Units
 * ReadWriteMicrobenchmark.test      avgt    2  5.557           s/op
 *
 * 4 way writing out four way (128MB)
 * Benchmark                         Mode  Cnt  Score   Error  Units
 * ReadWriteMicrobenchmark.test      avgt    2  3.266           s/op
 *
 * 4 way writing eight way (128MB)
 * ReadWriteMicrobenchmark.test      avgt    2  3.312           s/op
 *
 * 4 way writing 16 way (128MB)
 * Benchmark                         Mode  Cnt  Score   Error  Units
 * ReadWriteMicrobenchmark.test      avgt    2  3.711           s/op
 *
 * 4 way writing 32 way (128MB)
 * Benchmark                         Mode  Cnt  Score   Error  Units
 * ReadWriteMicrobenchmark.test      avgt    2  4.234           s/op
 *
 * 4 way writing 64 way (32MB)
 * Benchmark                         Mode  Cnt  Score   Error  Units
 * ReadWriteMicrobenchmark.test      avgt    2  5.611           s/op
 *
 * 4 way writing 128 way (32MB)
 * Benchmark                         Mode  Cnt  Score   Error  Units
 * ReadWriteMicrobenchmark.test      avgt    2  5.745           s/op
 *
 * 4 way writing 128 way (16MB)
 * Benchmark                         Mode  Cnt  Score   Error  Units
 * ReadWriteMicrobenchmark.test      avgt    2  5.770           s/op
 */
public class ReadWriteStep2Microbenchmark extends OneBrcMicrobenchmark {

    private static final String FILE = "measurements.txt";
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');
    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> DELIMITER_MINUS_ASCII_ZERO_VECTOR = SPECIES.broadcast(';' - '0');
    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');

    private static final ShortVector[] STOI_MUL_LOOKUP = {
            null,
            null,
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            null,
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
    };

    private static final VectorShuffle<Byte>[] TEMP_SHUFFLE = new VectorShuffle[]{
            null,
            null,
            VectorShuffle.fromValues(ByteVector.SPECIES_256, 0, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), // - 1 2 3 == -12.3
            VectorShuffle.fromValues(ByteVector.SPECIES_256, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), // 2 3 == 2.3
            null,
            VectorShuffle.fromValues(ByteVector.SPECIES_256, 10, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), // 1 2 3 == 12.3
            VectorShuffle.fromValues(ByteVector.SPECIES_256, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) // - 2 3 == -2.3
    };

    private static final int OUTPUT_SIZE = 64 * 1024 * 1024;
    private static final int OUTPUT_SIZE_MASK = OUTPUT_SIZE - 1;
    private static final int OUTPUT_COUNT = 1;
    private static final int OUTPUT_MASK = OUTPUT_COUNT - 1;

    private Arena arena;
    private MemorySegment inputData;

    private MemorySegment[] outputDataArray;
    private long[] outputArrayOffset;

    private MemorySegment outputData;
    private long outputOffset;

    @Setup
    public void setup() throws IOException {
        arena = Arena.ofShared();
        inputData = mmapDataFile(FILE, arena);

        outputData = arena.allocate(OUTPUT_SIZE);
        // outputData = new MemorySegment[OUTPUT_COUNT];
        // outputOffset = new long[OUTPUT_COUNT];
        // for (int i = 0; i < outputData.length; i++) {
        // outputData[i] = arena.allocate(OUTPUT_SIZE);
        // outputOffset[i] = 0;
        // }
    }

    // Toliara;-12.3\n
    // .T.TT.T....... -> .T.TT. -> TT. -> 0x6
    // Toliara;12.3\n
    // .T.TTT....... -> .T.TT -> .TT -> 0x3
    // Toliara;-2.3\n
    // .T.T.T....... -> .T.T. -> .T. -> 0x2
    // Toliara;2.3\n
    // .T.TT....... -> .T.T -> T.T -> 0x5
    @Benchmark
    public long test() {
        MemorySegment inputData = this.inputData;
        // MemorySegment[] outputData = this.outputData;
        MemorySegment outputData = this.outputData;

        long stride = inputData.byteSize() / 4;

        long offset1 = 0;
        long end1 = stride - SPECIES.vectorByteSize();

        long offset2 = stride;
        long end2 = stride + stride - SPECIES.vectorByteSize();

        long offset3 = stride + stride;
        long end3 = stride + stride + stride - SPECIES.vectorByteSize();

        long offset4 = stride + stride + stride;
        long end4 = stride + stride + stride + stride - SPECIES.vectorByteSize();

        while (offset1 < end1 && offset2 < end2 && offset3 < end3 && offset4 < end4) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            VectorMask<Byte> newlines1 = byteVector1.compare(VectorOperators.EQ, NEWLINE_VECTOR);

            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            VectorMask<Byte> newlines2 = byteVector2.compare(VectorOperators.EQ, NEWLINE_VECTOR);

            Vector<Byte> byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            VectorMask<Byte> newlines3 = byteVector3.compare(VectorOperators.EQ, NEWLINE_VECTOR);

            Vector<Byte> byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
            VectorMask<Byte> newlines4 = byteVector4.compare(VectorOperators.EQ, NEWLINE_VECTOR);

            if (newlines1.trueCount() != 2) {
                offset1 = handleExceptionalCase(offset1, inputData);
                continue;
            }
            offset1 += newlines1.lastTrue() + 1;

            if (newlines2.trueCount() != 2) {
                offset2 = handleExceptionalCase(offset2, inputData);
                continue;
            }
            offset2 += newlines2.lastTrue() + 1;

            if (newlines3.trueCount() != 2) {
                offset3 = handleExceptionalCase(offset3, inputData);
                continue;
            }
            offset3 += newlines3.lastTrue() + 1;

            if (newlines4.trueCount() != 2) {
                offset4 = handleExceptionalCase(offset4, inputData);
                continue;
            }
            offset4 += newlines4.lastTrue() + 1;

            {
                Vector<Byte> sub = byteVector1.sub(ASCII_ZERO);
                int compareInt = (int) sub.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR).toLong();

                int firstTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (firstTrue + 1);
                byte shuffle1 = (byte) (compareInt & 0x7);
                compareInt = compareInt >> 5;

                int lastTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (lastTrue + 1);
                byte shuffle2 = (byte) (compareInt & 0x7);

                // Vector<Byte> rearrange1 = byteVector1.rearrange(TEMP_SHUFFLE[shuffle1]);
                Vector<Byte> rearrange2 = byteVector1.rearrange(TEMP_SHUFFLE[shuffle2]);

                // int perfectHash = byteVector1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                // int outputSelect = perfectHash & OUTPUT_MASK;

                // rearrange1.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset[outputSelect] += SPECIES.vectorByteSize();
                // rearrange2.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset[outputSelect] += SPECIES.vectorByteSize();

                byteVector1.intoMemorySegment(outputData, outputOffset & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputOffset += SPECIES.vectorByteSize();
                rearrange2.intoMemorySegment(outputData, outputOffset & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputOffset += SPECIES.vectorByteSize();
            }

            {
                Vector<Byte> sub = byteVector2.sub(ASCII_ZERO);
                int compareInt = (int) sub.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR).toLong();

                int firstTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (firstTrue + 1);
                byte shuffle1 = (byte) (compareInt & 0x7);
                compareInt = compareInt >> 5;

                int lastTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (lastTrue + 1);
                byte shuffle2 = (byte) (compareInt & 0x7);

                // Vector<Byte> rearrange1 = byteVector2.rearrange(TEMP_SHUFFLE[shuffle1]);
                Vector<Byte> rearrange2 = byteVector2.rearrange(TEMP_SHUFFLE[shuffle2]);

                // int perfectHash = byteVector2.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                // int outputSelect = perfectHash & OUTPUT_MASK;

                // rearrange1.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset[outputSelect] += SPECIES.vectorByteSize();
                // rearrange2.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset[outputSelect] += SPECIES.vectorByteSize();
                byteVector2.intoMemorySegment(outputData, outputOffset & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputOffset += SPECIES.vectorByteSize();
                rearrange2.intoMemorySegment(outputData, outputOffset & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputOffset += SPECIES.vectorByteSize();
            }

            {
                Vector<Byte> sub = byteVector3.sub(ASCII_ZERO);
                int compareInt = (int) sub.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR).toLong();

                int firstTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (firstTrue + 1);
                byte shuffle1 = (byte) (compareInt & 0x7);
                compareInt = compareInt >> 5;

                int lastTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (lastTrue + 1);
                byte shuffle2 = (byte) (compareInt & 0x7);

                // Vector<Byte> rearrange1 = byteVector3.rearrange(TEMP_SHUFFLE[shuffle1]);
                Vector<Byte> rearrange2 = byteVector3.rearrange(TEMP_SHUFFLE[shuffle2]);

                // int perfectHash = byteVector3.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                // int outputSelect = perfectHash & OUTPUT_MASK;

                // rearrange1.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset[outputSelect] += SPECIES.vectorByteSize();
                // rearrange2.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset[outputSelect] += SPECIES.vectorByteSize();
                byteVector3.intoMemorySegment(outputData, outputOffset & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputOffset += SPECIES.vectorByteSize();
                rearrange2.intoMemorySegment(outputData, outputOffset & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputOffset += SPECIES.vectorByteSize();
            }

            {
                Vector<Byte> sub = byteVector4.sub(ASCII_ZERO);
                int compareInt = (int) sub.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR).toLong();

                int firstTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (firstTrue + 1);
                byte shuffle1 = (byte) (compareInt & 0x7);
                compareInt = compareInt >> 5;

                int lastTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (lastTrue + 1);
                byte shuffle2 = (byte) (compareInt & 0x7);

                // Vector<Byte> rearrange1 = byteVector4.rearrange(TEMP_SHUFFLE[shuffle1]);
                Vector<Byte> rearrange2 = byteVector4.rearrange(TEMP_SHUFFLE[shuffle2]);

                // int perfectHash = byteVector4.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                // int outputSelect = perfectHash & OUTPUT_MASK;

                // rearrange1.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset[outputSelect] += SPECIES.vectorByteSize();
                // rearrange2.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset[outputSelect] += SPECIES.vectorByteSize();
                byteVector4.intoMemorySegment(outputData, outputOffset & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputOffset += SPECIES.vectorByteSize();
                rearrange2.intoMemorySegment(outputData, outputOffset & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputOffset += SPECIES.vectorByteSize();
            }
        }

        return offset1;
    }

    // @Benchmark
    public long testSingle() {
        MemorySegment inputData = this.inputData;
        MemorySegment[] outputData = this.outputDataArray;

        long stride = inputData.byteSize();

        long offset1 = 0;
        long end1 = stride - SPECIES.vectorByteSize();

        while (offset1 < end1) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            VectorMask<Byte> newlines1 = byteVector1.compare(VectorOperators.EQ, NEWLINE_VECTOR);

            if (newlines1.trueCount() != 2) {
                offset1 = handleExceptionalCase(offset1, inputData);
                continue;
            }
            offset1 += newlines1.lastTrue() + 1;

            {
                Vector<Byte> sub = byteVector1.sub(ASCII_ZERO);
                int compareInt = (int) sub.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR).toLong();

                int firstTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (firstTrue + 1);
                byte shuffle1 = (byte) (compareInt & 0x7);
                compareInt = compareInt >> 5;

                int lastTrue = Integer.numberOfTrailingZeros(compareInt);
                compareInt = compareInt >> (lastTrue + 1);
                byte shuffle2 = (byte) (compareInt & 0x7);

                Vector<Byte> rearrange1 = byteVector1.rearrange(TEMP_SHUFFLE[shuffle1]);
                Vector<Byte> rearrange2 = byteVector1.rearrange(TEMP_SHUFFLE[shuffle2]);

                int perfectHash = byteVector1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int outputSelect = perfectHash & OUTPUT_MASK;

                rearrange1.intoMemorySegment(outputData[outputSelect], outputArrayOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputArrayOffset[outputSelect] += SPECIES.vectorByteSize();
                rearrange2.intoMemorySegment(outputData[outputSelect], outputArrayOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                outputArrayOffset[outputSelect] += SPECIES.vectorByteSize();
            }
        }

        return offset1;
    }

    /*
     * while (offset1 < end1) {
     * Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
     * // System.out.println(toString(byteVector1).replaceAll("\n", " "));
     * VectorMask<Byte> newlines = byteVector1.compare(VectorOperators.EQ, NEWLINE_VECTOR);
     * 
     * if (newlines.trueCount() != 2) {
     * offset1 = handleExceptionalCase(offset1, inputData);
     * continue;
     * }
     * offset1 += newlines.lastTrue() + 1;
     * 
     * // Vector<Byte> sub = byteVector1.sub(ASCII_ZERO);
     * // int compareInt = (int) sub.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR).toLong();
     * //// compareInt = compareInt; Dikson;-11.7 Tucson;21.5 Accra;3
     * //
     * // int firstTrue = Integer.numberOfTrailingZeros(compareInt);
     * // compareInt = compareInt >> (firstTrue + 1);
     * // byte shuffle1 = (byte) (compareInt & 0x7);
     * // compareInt = compareInt >> 5;
     * //
     * // int lastTrue = Integer.numberOfTrailingZeros(compareInt);
     * // compareInt = compareInt >> (lastTrue + 1);
     * // byte shuffle2 = (byte) (compareInt & 0x7);
     * //// System.out.println("shuffle1 = " + shuffle1);
     * //// System.out.println("shuffle2 = " + shuffle2);
     * //
     * // Vector<Byte> rearrange1 = byteVector1.rearrange(TEMP_SHUFFLE[shuffle1]);
     * // Vector<Byte> rearrange2 = byteVector1.rearrange(TEMP_SHUFFLE[shuffle2]);
     * 
     * {
     * int perfectHash = byteVector1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
     * int outputSelect = perfectHash & 0x3F;
     * 
     * byteVector1.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
     * outputOffset[outputSelect] += SPECIES.vectorByteSize();
     * byteVector1.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
     * outputOffset[outputSelect] += SPECIES.vectorByteSize();
     * }
     * }
     */

    private long handleExceptionalCase(long offset, MemorySegment inputData) {
        while (inputData.get(ValueLayout.JAVA_BYTE, offset) != '\n') {
            offset++;
        }

        offset++;
        return offset + 1;
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        // Class<? extends Profiler> profilerClass = AsyncProfiler.class;
        runWith(
                ReadWriteStep2Microbenchmark.class.getSimpleName(),
                profilerClass
        // ,
        // "libPath=/root/libasyncProfiler.so;interval=1000000;output=flamegraph;dir=profile-results"
        );
    }
}
