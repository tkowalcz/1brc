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

import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
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
        "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        // "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
})
@Threads(1)
public class ParseMicrobenchmark {

    public static final VectorMask<Short> AH = VectorMask.fromLong(ShortVector.SPECIES_256, 0xFF_FF_00_00L);
    public static final VectorMask<Short> AL = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_FF_FFL);
    private static final Vector<Byte> ASCII_ZERO = ByteVector.SPECIES_256.broadcast('0');

    private static final ShortVector[] STOI_MUL_LOOKUP = {
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
    };

    private static final ShortVector[] STOI_MUL_LOOKUP_2 = {
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
    };

    private static final LongVector[] STOI_MUL_LOOKUP_3 = {
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 0, 0, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 0, 0, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 0, 0, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 0, 0, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 100, 10, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ -10, 0, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 0, 0, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 0, 0, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 0, 0, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ -100, -10, 0, 0 }, 0),
            LongVector.fromArray(LongVector.SPECIES_256, new long[]{ 10, 0, 0, 0 }, 0)
    };
    public static final VectorShuffle<Short> ATOI_SHUFFLE_1 = VectorShuffle.fromValues(ShortVector.SPECIES_256, 0, 3, 0, 0, 0, 7, 0, 0, 0, 11, 0, 0, 0, 15, 0, 0);
    public static final VectorShuffle<Short> ATOI_SHUFFLE_2 = VectorShuffle.fromValues(ShortVector.SPECIES_256, 0, 2, 0, 0, 0, 6, 0, 0, 0, 10, 0, 0, 0, 14, 0, 0);

    private byte[] value;
    private int position;

    private long[] tmpArr;
    private int[] indexMap = new int[]{ 1, 4, 8, 16 };
    private short[] shortData1;
    private short[] shortData2;

    @Setup
    public void setup() {
        value = "Warszawa;-12.3\nKraków;0.4\nSuwałki;-4.3\nGdańsk;9.8\nKłaj;-1.4".getBytes(StandardCharsets.UTF_8);
        position = 9;
        tmpArr = new long[64];

        shortData1 = new short[]{ 0, -1, -2, -3, 0, 1, 2, 3, 0, 0, 2, 3, 0, 0, -2, -2 };
        shortData2 = new short[]{ 0, 1, 2, 3, 0, -1, -2, -3, 0, 0, 2, 3, 0, 0, -2, -2 };
    }

    // @Benchmark
    public float parseFloat() {
        int integerPart = 0;
        int fractionalPart = 0;
        float negative = 1.0f;

        int i = position;
        if (value[i] == '-') {
            negative = -1.0f;
            i++;
        }

        for (; i < value.length; i++) {
            if (value[i] == '.') {
                break;
            }

            integerPart *= 10;
            integerPart += value[i] - '0';
        }

        i++;
        fractionalPart = value[i] - '0';
        return negative / fractionalPart + integerPart;
    }
    //
    // private static final int[] ITOA_LOOKUP;
    //
    // static {
    // for (int i = -999; i < 999; i++) {
    // ITOA_LOOKUP[i] = i / 10;
    // }
    // }

    // @Benchmark
    // public long parseSWAR() {
    // long measurement = Unsafe.getUnsafe().getLong(value, ARRAY_BASE_OFFSET + 9);
    //
    // (measurement + 0x30_30_30_30_30_30_30_30L) &
    // VectorMask < Byte > mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
    // int lookupIndex = (int) (mask.toLong() & 0x0F);
    //
    // return vector
    // .sub(ASCII_ZERO)
    // .castShape(ShortVector.SPECIES_256, 0)
    // .mul(STOI_MUL_LOOKUP[lookupIndex])
    // .reduceLanesToLong(VectorOperators.ADD);
    // }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public long parseVectorised() {
        ByteVector vector = ByteVector.fromArray(ByteVector.SPECIES_256, value, 9);

        return vector.lane(0) + vector.lane(1) + vector.lane(2) + vector.lane(3) + vector.lane(4);
    }

    // @Benchmark
    public long parseTwoVectorised() {
        Vector<Byte> vector1 = ByteVector.fromArray(
                ByteVector.SPECIES_256, value, 9);

        Vector<Byte> vector2 = ByteVector.fromArray(
                ByteVector.SPECIES_256, value, 23);

        VectorMask<Byte> mask1 = vector1.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex1 = (int) (mask1.toLong() & 0x0F);

        VectorMask<Byte> mask2 = vector2.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex2 = (int) (mask2.toLong() & 0x0F);

        long v1 = vector1
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex1])
                .reduceLanesToLong(VectorOperators.ADD);

        long v2 = vector2
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex2])
                .reduceLanesToLong(VectorOperators.ADD);

        return v1 + v2;
    }

    // @Benchmark
    public long parseTwoInOneVectorised() {
        byte[] charData = "-12.3\n          0.4\n            ".getBytes(StandardCharsets.UTF_8);
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256, charData, 0);

        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        long longMask = mask.toLong();
        int lookupIndex = (int) ((longMask & 0x07L | (longMask >> 29)) & 0x03F);

        ShortVector lookup = STOI_MUL_LOOKUP_2[lookupIndex];

        Vector<Short> shortMul = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(lookup);

        long v1 = shortMul.reduceLanesToLong(VectorOperators.ADD, AH);
        long v2 = shortMul.reduceLanesToLong(VectorOperators.ADD, AL);

        return v1 + v2;
    }

    // @Benchmark
    public long parseFourInTwoInOneVectorised() {
        byte[] charData1 = "-12.3\n          0.4\n            ".getBytes(StandardCharsets.UTF_8);
        byte[] charData2 = "2.8  \n          34.1\n           ".getBytes(StandardCharsets.UTF_8);

        Vector<Byte> vector1 = ByteVector.fromArray(
                ByteVector.SPECIES_256, charData1, 0);

        Vector<Byte> vector2 = ByteVector.fromArray(
                ByteVector.SPECIES_256, charData2, 0);

        VectorMask<Byte> mask1 = vector1.compare(VectorOperators.LT, ASCII_ZERO);
        VectorMask<Byte> mask2 = vector2.compare(VectorOperators.LT, ASCII_ZERO);

        long longMask1 = mask1.toLong();
        int lookupIndex1 = (int) ((longMask1 & 0x07L | (longMask1 >> 29)) & 0x03F);

        long longMask2 = mask2.toLong();
        int lookupIndex2 = (int) ((longMask2 & 0x07L | (longMask2 >> 29)) & 0x03F);

        ShortVector lookup1 = STOI_MUL_LOOKUP_2[lookupIndex1];
        Vector<Short> shortMul1 = vector1
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(lookup1);

        ShortVector lookup2 = STOI_MUL_LOOKUP_2[lookupIndex2];
        Vector<Short> shortMul2 = vector2
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(lookup2);

        long v1 = shortMul1.reduceLanesToLong(VectorOperators.ADD, AH);
        long v2 = shortMul1.reduceLanesToLong(VectorOperators.ADD, AL);
        long v3 = shortMul2.reduceLanesToLong(VectorOperators.ADD, AH);
        long v4 = shortMul2.reduceLanesToLong(VectorOperators.ADD, AL);

        return v1 + v2 + v3 + v4;
    }

    public static final ShortVector ATOI_MUL = ShortVector.fromArray(
            ShortVector.SPECIES_256,
            new short[]{ 0, 100, 10, 1, 0, 100, 10, 1, 0, 100, 10, 1, 0, 100, 10, 1, 0 },
            0);

    public static final IntVector ATOI_MUL_INT = IntVector.fromArray(
            IntVector.SPECIES_256,
            new int[]{ 0, 100, 10, 1, 0, 100, 10, 1 },
            0);

    public static final VectorMask<Short> ATOI_REDUCE_1 = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_00_00_00_00_00_0FL);
    public static final VectorMask<Short> ATOI_REDUCE_2 = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_00_00_00_00_00_F0L);
    public static final VectorMask<Short> ATOI_REDUCE_3 = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_00_00_00_00_0F_00L);
    public static final VectorMask<Short> ATOI_REDUCE_4 = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_00_00_00_00_F0_00L);

    // -12.3 12.3 2.3 -2.3
    // 00 -01 -02 -03 | 00 01 02 03 | 00 00 02 03 | 00 00 -02 -03
    // 0 100 10 1
    //
    // @Benchmark
    public long parseBinaryCodedDecimal() {
        ShortVector shortVector = ShortVector.fromArray(ShortVector.SPECIES_256, shortData1, 0)
                .mul(ATOI_MUL);

        // ShortVector s1 = shortVector.rearrange(ATOI_SHUFFLE_1);
        // ShortVector s2 = shortVector.rearrange(ATOI_SHUFFLE_2);

        // ShortVector result = shortVector.add(s1).add(s2);
        // return result.lane(1) | result.lane(5) | result.lane(9) | result.lane(13);
        short temp1 = (short) shortVector.reduceLanesToLong(VectorOperators.ADD, ATOI_REDUCE_1);
        short temp2 = (short) shortVector.reduceLanesToLong(VectorOperators.ADD, ATOI_REDUCE_2);
        short temp3 = (short) shortVector.reduceLanesToLong(VectorOperators.ADD, ATOI_REDUCE_3);
        short temp4 = (short) shortVector.reduceLanesToLong(VectorOperators.ADD, ATOI_REDUCE_4);

        return temp1 | temp2 | temp3 - temp4;
    }

    // @Benchmark
    public long parseTwoBinaryCodedDecimals() {
        ShortVector shortVector1 = ShortVector.fromArray(ShortVector.SPECIES_256, shortData1, 0)
                .mul(ATOI_MUL);

        ShortVector shortVector2 = ShortVector.fromArray(ShortVector.SPECIES_256, shortData2, 0)
                .mul(ATOI_MUL);

        ShortVector s1 = shortVector1.rearrange(ATOI_SHUFFLE_1);
        ShortVector s2 = shortVector1.rearrange(ATOI_SHUFFLE_2);

        ShortVector result1 = shortVector1.add(s1).add(s2);

        s1 = shortVector2.rearrange(ATOI_SHUFFLE_1);
        s2 = shortVector2.rearrange(ATOI_SHUFFLE_2);

        ShortVector result2 = shortVector2.add(s1).add(s2);

        long r1 = result1.lane(1) | result1.lane(5) | result1.lane(9) | result1.lane(13);
        long r2 = result2.lane(1) | result2.lane(5) | result2.lane(9) | result2.lane(13);

        // short temp1 = (short) shortVector.reduceLanesToLong(VectorOperators.ADD, ATOI_REDUCE_1);
        // short temp2 = (short) shortVector.reduceLanesToLong(VectorOperators.ADD, ATOI_REDUCE_2);
        // short temp3 = (short) shortVector.reduceLanesToLong(VectorOperators.ADD, ATOI_REDUCE_3);
        // short temp4 = (short) shortVector.reduceLanesToLong(VectorOperators.ADD, ATOI_REDUCE_4);

        return r1 | r2;
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = JavaFlightRecorderProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(ParseMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)
                .build();

        new Runner(opt).run();
    }
}
