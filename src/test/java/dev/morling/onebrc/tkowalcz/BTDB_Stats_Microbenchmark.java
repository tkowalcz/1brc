/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.CalculateAverage_tkowalcz;
import dev.morling.onebrc.IoUtil;
import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
        "-Xmx1g",
        "-Xms1g"
})
@Threads(1)
public class BTDB_Stats_Microbenchmark {

    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    // Used to identify positions where vector containing temperature measurement has '-', '.' and '\n' characters.
    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');

    private static final Vector<Byte> SEMICOLON_VECTOR = SPECIES.broadcast(';');

    static final VectorMask<Byte>[] CITY_LOOKUP_MASK = CalculateAverage_tkowalcz.createMasks32();

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x400000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = 0x400000 - 1;

    // Values used to multiply digits of temperature measurement to get proper int. E.g. -12.3 will be multiplied by
    // 10th element (0, -100, -10, 0, -1) giving '-' * 0 + '1' * -100 + '2' * -10 + '.' * 0 + '3' * -1 = -123.
    // There are four combinations of possible mask results from comparing (less than) vector containing temperature
    // measurement with ASCII_ZERO. Hence, only four entries are populated.
    private static final ShortVector[] STOI_MUL_LOOKUP = {
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0)
    };

    // We also need to know the size of temperature measurement in characters, lookup table works the same way as STOI_MUL_LOOKUP.
    private static final int[] STOI_SIZE_LOOKUP = { 0, 0, 0, 0, 5, 5, 0, 0, 0, 6, 4 };

    private Arena arena;
    private MemorySegment inputData;
    private byte[] citiesMap;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);

            citiesMap = new byte[64 * TABLE_SIZE];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // histogram = [0, 34737800, 213091910, 2136860, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    // Jayapura;-5.3\Detroit;7.9\Las Ve
    // ........T............T.......... == ;
    // ........TTTTTT.......TTTTT...... <= ;
    // .........TTTTT........TTTT...... < ;
    // .............T...........T...... == \n
    // compress
    // -5.3\7.9\
    //
    // Jayapura;-5.3\Detroit;7.9\Las Ve
    // .........TTTTT........TTTT...... < ;
    // .............T...........T...... == \n
    @Benchmark
    public float backToTheDrawingBoard_aligned() {
        long offset = 0;
        long end = inputData.byteSize() - 2 * SPECIES.length();

        int[] histogram = new int[32];

        float trueCountSum = 0;
        float loopIterations = 0;
        while (offset < end) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset, ByteOrder.nativeOrder());
            VectorMask<Byte> vectorMask = byteVector1.compare(VectorOperators.LT, SEMICOLON_VECTOR);
            int firstDelimiter1 = vectorMask.firstTrue();

            VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[firstDelimiter1];
            Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);

            int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index1 = perfectHash32_1 & TABLE_SIZE_MASK;
//            citiesMap[index1 << 5];

            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex1 = (int) ((mask1.toLong() >> firstDelimiter1) & 0x0F);

            long value = byteVector1
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1])
                    .reduceLanesToLong(VectorOperators.ADD);

//            offset1 += STOI_SIZE_LOOKUP[lookupIndex1];

            loopIterations++;
            int trueCount = vectorMask.trueCount();
            histogram[trueCount]++;

            if (trueCount == 0) {
                offset += SPECIES.length();
            } else {
                trueCountSum += trueCount;
                offset += vectorMask.lastTrue() + 1;
            }

//            System.out.println("offset = " + offset);
//            offset += SPECIES.length();
        }

        System.out.println("avg newlines count = " + (trueCountSum / loopIterations));
        System.out.println("histogram = " + Arrays.toString(histogram));
        return trueCountSum;
    }

    private static MemorySegment mmapDataFile(String fileName, Arena arena) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
             FileChannel channel = file.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
    }

    private static MemoryMappedFile mmapDataFile(String fileName) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
             FileChannel channel = file.getChannel()) {
            long pointer = IoUtil.map(channel, FileChannel.MapMode.READ_ONLY, 0, channel.size());
            return new MemoryMappedFile(
                    pointer,
                    channel.size());
        }
    }

    record MemoryMappedFile(long pointer, long size) {
    }

    public static void main(String[] args) throws RunnerException {
        Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
//        Class<? extends Profiler> profilerClass = AsyncProfiler.class;
//        Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(BTDB_Stats_Microbenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)
                // .addProfiler("async:libPath=/root/libasyncProfiler.so;output=flamegraph;dir=profile-results")
                .build();

        new Runner(opt).run();
    }
}
