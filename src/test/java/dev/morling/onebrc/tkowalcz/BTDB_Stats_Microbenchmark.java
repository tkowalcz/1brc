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
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
    };

    // We also need to know the size of temperature measurement in characters, lookup table works the same way as STOI_MUL_LOOKUP.
    private static final int[] STOI_SIZE_LOOKUP = { 0, 6, 4, 0, 5, 5 };

    private Arena arena;
    private MemorySegment inputData;
    private byte[] citiesMap;
    private CalculateAverage_tkowalcz.StatisticsAggregate[] dataTable;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);

            dataTable = new CalculateAverage_tkowalcz.StatisticsAggregate[TABLE_SIZE];
            citiesMap = new byte[64 * TABLE_SIZE];
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // -12.3 12.3 -2.3 2.3
    public static final VectorShuffle<Short> ATOI_SHUFFLE = VectorShuffle.fromValues(
            ShortVector.SPECIES_256,
            0, 3, 0, 0, 0, 7, 0, 0, 0, 11, 0, 0, 0, 15, 0, 0);

    static CalculateAverage_tkowalcz.StatisticsAggregate findCityInChain(
                                                                         CalculateAverage_tkowalcz.StatisticsAggregate startingNode,
                                                                         Vector<Byte> hashInput,
                                                                         VectorMask<Byte> hashMask,
                                                                         AtomicInteger sequence,
                                                                         List<CalculateAverage_tkowalcz.StatisticsAggregate> realAggregates) {
        CalculateAverage_tkowalcz.StatisticsAggregate node = startingNode.getNext();
        while (node != null) {
            ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, node.getCity(), 0);
            if (!cityVector.compare(VectorOperators.EQ, hashInput).allTrue()) {
                node = node.getNext();
            }
            else {
                return node;
            }
        }

        byte[] city = new byte[SPECIES.length()];
        hashInput.reinterpretAsBytes().intoArray(city, 0, hashMask);
        CalculateAverage_tkowalcz.StatisticsAggregate result = new CalculateAverage_tkowalcz.StatisticsAggregate(
                city,
                hashMask.trueCount(),
                (short) sequence.getAndIncrement());
        realAggregates.add(result);
        return startingNode.attachLast(result);
    }

    private static String toString(Vector<Byte> data) {
        byte[] array = data.reinterpretAsBytes().toArray();
        return new String(array, StandardCharsets.UTF_8);
    }

    // histogram = [0, 34737800, 213091910, 2136860, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

    // London;14.6\Upington;20.4\Palerm
    // ......T.............T........... == ;
    // ......TTTTTT........TTTTTT...... <= ;
    // .......TTTTT.........TTTTT...... < ;
    // ...........T.............T...... == \n
    //
    // London;14.6\Upington;20.4\Palerm
    // .......TTTTT.........TTTTT...... < ;
    // ...........T.............T...... == \n
    @Benchmark
    public float backToTheDrawingBoard_aligned() {
        AtomicInteger sequence = new AtomicInteger();
        List<CalculateAverage_tkowalcz.StatisticsAggregate> realAggregates = new ArrayList<>();

        long offset = 0;
        long end = inputData.byteSize() - 2 * SPECIES.length();

        int[] histogram = new int[32];

        float trueCountSum = 0;
        float loopIterations = 0;
        while (offset < end) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset, ByteOrder.nativeOrder());
            VectorMask<Byte> vectorMask = byteVector1.compare(VectorOperators.EQ, SEMICOLON_VECTOR);
            int firstDelimiter1 = vectorMask.firstTrue() + 1;

            VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[firstDelimiter1];
            Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);

            int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index1 = perfectHash32_1 & TABLE_SIZE_MASK;
            CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate_1 = dataTable[index1];
            if (statisticsAggregate_1 == null) {
                byte[] city = new byte[SPECIES.length()];
                hashInput1.reinterpretAsBytes().intoArray(city, 0, hashMask1);

                statisticsAggregate_1 = new CalculateAverage_tkowalcz.StatisticsAggregate(
                        city,
                        hashMask1.trueCount(),
                        (short) sequence.getAndIncrement());

                realAggregates.add(statisticsAggregate_1);
                dataTable[index1] = statisticsAggregate_1;
            }
            else {
                ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_1.getCity(), 0);
                if (!cityVector.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                    // Very slow path: linked list of collisions
                    statisticsAggregate_1 = findCityInChain(statisticsAggregate_1, hashInput1, hashMask1, sequence, realAggregates);
                }
            }

            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            long l = mask1.toLong() >> firstDelimiter1;
            int lookupIndex1 = (int) (l & 0x07);

            long value = byteVector1
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1])
                    .reduceLanesToLong(VectorOperators.ADD);

            offset += firstDelimiter1 + STOI_SIZE_LOOKUP[lookupIndex1];

            //
            // loopIterations++;
            // int trueCount = vectorMask.trueCount();
            // histogram[trueCount]++;
            //
            // if (trueCount == 0) {
            // offset += SPECIES.length();
            // } else {
            // trueCountSum += trueCount;
            // offset += vectorMask.lastTrue() + 1;
            // }

            // System.out.println("offset = " + offset);
            // offset += SPECIES.length();
        }

        // System.out.println(realAggregates
        // .stream()
        // .map(statisticsAggregate -> statisticsAggregate.cityAsString() + ": " + statisticsAggregate.getCityId())
        // .collect(Collectors.joining("\n"))
        // );
        //
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
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = AsyncProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

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
