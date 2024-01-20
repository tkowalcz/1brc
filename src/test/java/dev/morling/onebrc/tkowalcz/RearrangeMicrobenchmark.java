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
import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfProfiler;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+DebugNonSafepoints",
        "-XX:+EnableVectorSupport",
        "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
        "-Xmx8g",
        "-Xmn8g"
})
@Threads(1)
public class RearrangeMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');

    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');
    private static final Vector<Byte> ASCII_NINE = SPECIES.broadcast('9');

    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    public static final long[] A = { 1, 2, 3, 4 };
    public static final long[] B = { 11, 12, 13, 14 };
    public static final VectorShuffle<Long> SHUFFLE = VectorShuffle.fromValues(LongVector.SPECIES_256, 1, 3, 5, 7);
    public static final long[] A1 = new long[4];
    private static final VectorShuffle<Byte> CITIES_SHUFFLE_1 = VectorShuffle.fromValues(
            ByteVector.SPECIES_256,
            0, 1, 2, 3, 4, 5, 6, 6,
            8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31);

    private static final VectorMask<Byte> CITIES_MASK_1 = ByteVector.SPECIES_256.loadMask(
            new boolean[]{
                    true, true, true, true, true, true, false, false,
                    false, false, false, false, false, false, false, false,
                    false, false, false, false, false, false, false, false,
                    false, false, false, false, false, false, false, false
            },
            0);

    private static final VectorShuffle<Byte> CITIES_SHUFFLE_2 = VectorShuffle.fromValues(
            ByteVector.SPECIES_256,
            12, 13, 14, 15, 16, 17, 18, 19,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0);

    private static final VectorShuffle<Byte>[] CITIES_SHUFFLE_V2 = createShuffles();

    private static final VectorMask<Byte> CITIES_MASK_2 = ByteVector.SPECIES_256.loadMask(
            new boolean[]{
                    true, true, true, true, true, true, true, true,
                    false, false, false, false, false, false, false, false,
                    false, false, false, false, false, false, false, false,
                    false, false, false, false, false, false, false, false
            },
            0);

    private static final VectorShuffle<Byte> TEMP_SHUFFLE = VectorShuffle.fromValues(
            ByteVector.SPECIES_256,
            0, 0, 0, 7, 0, 8, 0, 10,
            0, 0, 0, 21, 0, 22, 0, 24,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0);

    private static final VectorMask<Byte> TEMP_MASK = ByteVector.SPECIES_256.loadMask(
            new boolean[]{
                    false, false, false, true, false, true, false, true,
                    false, false, false, true, false, true, false, true,
                    false, false, false, false, false, false, false, false,
                    false, false, false, false, false, false, false, false
            },
            0);

    private byte[] data1;
    private byte[] data2;
    private byte[] citiesOutput1 = new byte[32];
    private byte[] citiesOutput2 = new byte[32];
    private CalculateAverage_tkowalcz.StatisticsAggregate[] dataTable;

    @Setup
    public void setUp() {
        data1 = "London;14.6\nUpington;20.4\nPalerm".getBytes(StandardCharsets.UTF_8);
        data2 = "Palermo;18.2\nBrussels;3.2\nYaoundé;27.4".getBytes(StandardCharsets.UTF_8);

        dataTable = new CalculateAverage_tkowalcz.StatisticsAggregate[TABLE_SIZE];
    }

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x400000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = 0x400000 - 1;

    static final VectorMask<Byte>[] CITY_LOOKUP_MASK = CalculateAverage_tkowalcz.createMasks32();

    // London;14.6\Upington;20.4\Palerm
    // ......T....T........T....T......
    @Benchmark
    public void rearrange() {
        ByteVector byteVector = ByteVector.fromArray(ByteVector.SPECIES_256, data1, 0);
        // printString(byteVector);
        VectorMask<Byte> delimiterPositions = byteVector.compare(VectorOperators.EQ, DELIMITER_VECTOR);
        VectorMask<Byte> newlinePositions = byteVector.compare(VectorOperators.EQ, NEWLINE_VECTOR);

        long numberOfLinesInAVector = newlinePositions.trueCount();
        if (numberOfLinesInAVector == 0) {

        }

        int delimiterPosition = delimiterPositions.firstTrue();
        VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[delimiterPosition];
        Vector<Byte> cities1 = ZERO.blend(byteVector, hashMask1);
        // printString(cities1);
        int index1 = getHash(cities1);
        CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate_1 = dataTable[index1];
        if (statisticsAggregate_1 == null) {
            statisticsAggregate_1 = createNewEntry(cities1, hashMask1, delimiterPosition, index1);
        }
        statisticsAggregate_1.accept(1);

        if (numberOfLinesInAVector == 2) {
            int delimiter2Position = delimiterPositions.lastTrue();
            int newlinePosition = newlinePositions.firstTrue();
            int cityNameSize = delimiter2Position - newlinePosition - 1;

            VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[cityNameSize];
            VectorShuffle<Byte> shuffle2 = CITIES_SHUFFLE_V2[newlinePosition + 1];
            ByteVector cities2 = byteVector.rearrange(shuffle2, hashMask2);
            // printString(cities2);
            int index2 = getHash(cities2);

            CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate_2 = dataTable[index2];
            if (statisticsAggregate_2 == null) {
                statisticsAggregate_2 = createNewEntry(cities2, hashMask2, cityNameSize, index2);
            }
            statisticsAggregate_2.accept(2);
        }
        else {

        }

        // ByteVector temperatures = byteVector.rearrange(TEMP_SHUFFLE, TEMP_MASK);
        // temperatures.intoArray(data2, 0);
    }

    private CalculateAverage_tkowalcz.StatisticsAggregate createNewEntry(Vector<Byte> dataVector, VectorMask<Byte> cityNameMask, int cityNameSize, int mapIndex) {
        CalculateAverage_tkowalcz.StatisticsAggregate statisticsAggregate;
        byte[] city = new byte[SPECIES.length()];
        dataVector.reinterpretAsBytes().intoArray(city, 0, cityNameMask);

        statisticsAggregate = new CalculateAverage_tkowalcz.StatisticsAggregate(city, cityNameSize);
        dataTable[mapIndex] = statisticsAggregate;

        return statisticsAggregate;
    }

    private static int getHash(Vector<Byte> city) {
        int perfectHash32 = city.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
        return perfectHash32 & TABLE_SIZE_MASK;
    }

    private static final VectorShuffle<Byte> SHUFFLE2 = VectorShuffle.fromValues(
            ByteVector.SPECIES_256,
            0, 0, 0, 7, 0, 8, 0, 10,
            0, 0, 0, 21, 0, 22, 0, 24,
            0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0);

    // London;14.6\Upington;20.4\Palerm
    // .......TT.T..........TT.T.......
    // 01234567891111111111222222222233
    // 0123456789012345678901
    // 0000000780100000000002202
    // 0 12 3
    // 781222
    // 0123
    // Palermo;18.2\Brussels;3.2\Yaoundé;27.4
    //

    private static final Vector<Byte> INDEX = ByteVector.SPECIES_256.iotaShuffle(0, 1, false).toVector();

    private static void printString(Vector<Byte> data) {
        byte[] array = data.reinterpretAsBytes().toArray();
        String string = new String(array, StandardCharsets.UTF_8);

        System.out.println(string);
    }

    // @Benchmark
    public void compress() {
        ByteVector byteVector1 = ByteVector.fromArray(ByteVector.SPECIES_256, data1, 0);
        VectorMask<Byte> ge0 = byteVector1.compare(VectorOperators.GE, ASCII_ZERO);
        VectorMask<Byte> le9 = byteVector1.compare(VectorOperators.LE, ASCII_NINE);

        byteVector1 = byteVector1.sub(ASCII_ZERO);
        // System.out.println(toString(byteVector1));
        // VectorMask<Byte> newlineMask1 = byteVector1.compare(VectorOperators.EQ, NEWLINE_VECTOR);
        // VectorMask<Byte> semicolonMask1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR);

        VectorMask<Byte> mask = ge0.and(le9);
        Vector<Byte> compress = INDEX.compress(mask);
        ByteVector rearrange = byteVector1.rearrange(compress.toShuffle());

        // -12.3 12.3 -2.3 2.3
        // 0124 013 013 02
        // ByteVector byteVector2 = ByteVector.fromArray(ByteVector.SPECIES_256, data2, 0);
        // VectorMask<Byte> newlineMask2 = byteVector2.compare(VectorOperators.EQ, NEWLINE_VECTOR);
        // VectorMask<Byte> semicolonMask2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR);
        // VectorMask<Byte> mask2 = byteVector2.compare(VectorOperators.LT, ASCII_ZERO);
        //
        // ByteVector rearrange = byteVector1.rearrange(SHUFFLE2, byteVector2);
        rearrange.intoArray(data2, 0);
    }

    public static VectorShuffle<Byte>[] createShuffles() {
        VectorShuffle<Byte>[] result = new VectorShuffle[33];

        VectorShuffle<Byte> iota = VectorShuffle.iota(SPECIES, 0, 1, false);
        for (int i = 1; i < result.length; i++) {
            result[i] = iota.toVector().add(SPECIES.broadcast(i)).toShuffle();
        }

        return result;
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = GCProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = AsyncProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(RearrangeMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                // .addProfiler(profilerClass)
                // .addProfiler("async:libPath=/root/libasyncProfiler.so;output=flamegraph;dir=profile-results")
                .build();

        new Runner(opt).run();
    }
}
