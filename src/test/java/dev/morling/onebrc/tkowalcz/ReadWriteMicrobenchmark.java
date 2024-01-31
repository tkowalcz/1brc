package dev.morling.onebrc.tkowalcz;

import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
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
public class ReadWriteMicrobenchmark extends OneBrcMicrobenchmark {

    private static final String FILE = "measurements.txt";
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);
    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');

    private static final int OUTPUT_SIZE = 64 * 1024 * 1024;
    private static final int OUTPUT_SIZE_MASK = OUTPUT_SIZE - 1;
    private static final int OUTPUT_COUNT = 128;

    private Arena arena;
    private MemorySegment inputData;
    private MemorySegment[] outputData;
    private long[] outputOffset;
    // private long outputOffset;

    @Setup
    public void setup() throws IOException {
        arena = Arena.ofShared();
        inputData = mmapDataFile(FILE, arena);

        outputData = new MemorySegment[OUTPUT_COUNT];
        outputOffset = new long[OUTPUT_COUNT];
        for (int i = 0; i < outputData.length; i++) {
            outputData[i] = arena.allocate(OUTPUT_SIZE);
            outputOffset[i] = 0;
        }
    }

    @Benchmark
    public long test() {
        MemorySegment inputData = this.inputData;
        MemorySegment[] outputData = this.outputData;

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
            offset1 += SPECIES.vectorByteSize();
            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            offset2 += SPECIES.vectorByteSize();
            Vector<Byte> byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            offset3 += SPECIES.vectorByteSize();
            Vector<Byte> byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
            offset4 += SPECIES.vectorByteSize();

            // VectorMask<Byte> newlineMask1 = byteVector1.compare(VectorOperators.EQ, NEWLINE_VECTOR);
            // VectorMask<Byte> delimiterPositions1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR);

            // VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[delimiterPosition1];
            // Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);
            {
                int perfectHash = byteVector1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int outputSelect = perfectHash & 0x7F;

                byteVector1.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset += SPECIES.vectorByteSize();
                outputOffset[outputSelect] += SPECIES.vectorByteSize();
            }

            {
                int perfectHash = byteVector2.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int outputSelect = perfectHash & 0x7F;
                byteVector2.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset += SPECIES.vectorByteSize();
                outputOffset[outputSelect] += SPECIES.vectorByteSize();
            }

            {
                int perfectHash = byteVector3.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int outputSelect = perfectHash & 0x7F;
                byteVector3.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset += SPECIES.vectorByteSize();
                outputOffset[outputSelect] += SPECIES.vectorByteSize();
            }

            {
                int perfectHash = byteVector4.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int outputSelect = perfectHash & 0x7F;
                byteVector4.intoMemorySegment(outputData[outputSelect], outputOffset[outputSelect] & OUTPUT_SIZE_MASK, ByteOrder.nativeOrder());
                // outputOffset += SPECIES.vectorByteSize();
                outputOffset[outputSelect] += SPECIES.vectorByteSize();
            }
        }

        return offset1;
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = AsyncProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        runWith(ReadWriteMicrobenchmark.class.getSimpleName(), profilerClass);
    }
}
