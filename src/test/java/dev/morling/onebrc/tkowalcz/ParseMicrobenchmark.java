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
import sun.misc.Unsafe;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        // "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
})
@Threads(1)
public class ParseMicrobenchmark {

    public static final long ARRAY_BASE_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(byte[].class);

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

    private byte[] value;
    private int position;

    private long[] tmpArr;
    private int[] indexMap = new int[]{ 1, 4, 8, 16 };

    @Setup
    public void setup() {
        value = "Warszawa;-12.3\nKraków;0.4\nSuwałki;-4.3\nGdańsk;9.8\nKłaj;-1.4".getBytes(StandardCharsets.UTF_8);
        position = 9;
        tmpArr = new long[64];
    }

    @Benchmark
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
    public long parseVectorised() {
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256, value, 9);

        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x0F);

        return vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);
    }

    @Benchmark
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

    @Benchmark
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

    @Benchmark
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

    // @Benchmark
    public void parseFourInOneVectorised() {
        byte[] charData = "-12.3\n  0.4\n    14.5    -2.1    ".getBytes(StandardCharsets.UTF_8);
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256, charData, 0);

        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex1 = (int) (mask.toLong() & 0x00_00_00_00_00_00_00_0FL);
        int lookupIndex2 = (int) (mask.toLong() & 0x00_00_00_00_00_00_00_0FL);
        int lookupIndex3 = (int) (mask.toLong() & 0x00_00_00_00_00_00_00_0FL);
        int lookupIndex4 = (int) (mask.toLong() >> 32);

        LongVector lookup1 = STOI_MUL_LOOKUP_3[lookupIndex1];
        // ShortVector lookup2 = STOI_MUL_LOOKUP_2[lookupIndex2];

        // ShortVector lookup = lookup1.add(lookup2);

        LongVector mul = (LongVector) vector
                .sub(ASCII_ZERO)
                .convert(VectorOperators.B2L, 0)
                .mul(lookup1);

        mul.intoArray(tmpArr, 0, indexMap, 0);
        // return mul.lane(0) + mul.lane(1) + mul.lane(2) + mul.lane(3);
    }

    public static void main(String[] args) throws RunnerException {
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
