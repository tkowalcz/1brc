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

import dev.morling.onebrc.IoUtil;
import dev.morling.onebrc.UnsafeAccess;
import jdk.incubator.vector.*;
import jdk.incubator.vector.Vector;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.LinuxPerfNormProfiler;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import sun.misc.Unsafe;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Benchmark                               (intermediateArraySize)  Mode  Cnt  Score   Error  Units
 * DistributeMicrobenchmark.aggregate                           32  avgt    2  2.095           s/op
 * DistributeMicrobenchmark.aggregate:asm                       32  avgt         NaN            ---
 * DistributeMicrobenchmark.aggregate                           64  avgt    2  1.976           s/op
 * DistributeMicrobenchmark.aggregate:asm                       64  avgt         NaN            ---
 * DistributeMicrobenchmark.aggregate                          128  avgt    2  2.000           s/op
 * DistributeMicrobenchmark.aggregate:asm                      128  avgt         NaN            ---
 * DistributeMicrobenchmark.aggregate                          256  avgt    2  2.139           s/op
 * DistributeMicrobenchmark.aggregate:asm                      256  avgt         NaN            ---
 * DistributeMicrobenchmark.aggregate                          512  avgt    2  2.262           s/op
 * DistributeMicrobenchmark.aggregate:asm                      512  avgt         NaN            ---
 * DistributeMicrobenchmark.aggregate                         1024  avgt    2  2.319           s/op
 * DistributeMicrobenchmark.aggregate:asm                     1024  avgt         NaN            ---
 * DistributeMicrobenchmark.aggregate                         2048  avgt    2  2.397           s/op
 * DistributeMicrobenchmark.aggregate:asm                     2048  avgt         NaN            ---
 * DistributeMicrobenchmark.aggregate                         4096  avgt    2  2.588           s/op
 * DistributeMicrobenchmark.aggregate:asm                     4096  avgt         NaN            ---
 * <p>
 * Benchmark                                                   Mode  Cnt           Score   Error      Units
 * DistributeMicrobenchmark.aggregate                          avgt    2           1.319               s/op
 * DistributeMicrobenchmark.aggregate:CPI                      avgt                0.386          clks/insn
 * DistributeMicrobenchmark.aggregate:IPC                      avgt                2.593          insns/clk
 * DistributeMicrobenchmark.aggregate:L1-dcache-load-misses    avgt        767015016.977               #/op
 * DistributeMicrobenchmark.aggregate:L1-dcache-loads          avgt       3487240021.618               #/op
 * DistributeMicrobenchmark.aggregate:L1-icache-load-misses    avgt           224357.158               #/op
 * DistributeMicrobenchmark.aggregate:L1-icache-loads          avgt         28979258.073               #/op
 * DistributeMicrobenchmark.aggregate:branch-misses            avgt          3116633.860               #/op
 * DistributeMicrobenchmark.aggregate:branches                 avgt       1753805429.343               #/op
 * DistributeMicrobenchmark.aggregate:cycles                   avgt       3463095367.488               #/op
 * DistributeMicrobenchmark.aggregate:dTLB-load-misses         avgt           616402.135               #/op
 * DistributeMicrobenchmark.aggregate:dTLB-loads               avgt         68273004.606               #/op
 * DistributeMicrobenchmark.aggregate:iTLB-load-misses         avgt              760.539               #/op
 * DistributeMicrobenchmark.aggregate:iTLB-loads               avgt             1089.497               #/op
 * DistributeMicrobenchmark.aggregate:instructions             avgt       8978476796.115               #/op
 * DistributeMicrobenchmark.aggregate:stalled-cycles-backend   avgt       2715924059.190               #/op
 * DistributeMicrobenchmark.aggregate:stalled-cycles-frontend  avgt          4516201.467               #/op
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+AlwaysPreTouch",
        "-XX:+DebugNonSafepoints",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
//        "-XX:MaxDirectMemorySize=8589934592",
        "-Xmx1g",
        "-Xms1g"
})
@Threads(1)
public class DistributeMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast('\n');

    private static final String FILE = "measurements.txt";

    public static final int TABLE_SIZE = 0x400000;
    public static final long MEM_SIZE = 6 * 1000_000_000L;
//    public static final ShortVector ATOI_MUL = ShortVector.fromArray(
//            ShortVector.SPECIES_256,
//            new short[]{0, 100, 10, 1, 0, 100, 10, 1, 0, 100, 10, 1, 0, 100, 10, 1},
//            0
//    );

    public static final ShortVector ATOI_MUL = ShortVector.fromArray(
            ShortVector.SPECIES_256,
            new short[]{1, 10, 100, 0, 1, 10, 100, 0, 1, 10, 100, 0, 1, 10, 100, 0},
            0
    );
    public static final VectorMask<Short> ATOI_REDUCE_1 = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_00_00_00_00_00_0FL);
    public static final VectorMask<Short> ATOI_REDUCE_2 = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_00_00_00_00_00_F0L);
    public static final VectorMask<Short> ATOI_REDUCE_3 = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_00_00_00_00_0F_00L);
    public static final VectorMask<Short> ATOI_REDUCE_4 = VectorMask.fromLong(ShortVector.SPECIES_256, 0x00_00_00_00_00_00_F0_00L);

    //    @Param({"32", "64", "128", "256", "512", "1024", "2048", "4096"})
//    private int intermediateArraySize;
    private int[][] data;

    private MemoryMappedFile memoryMappedFile;

    record TmpAggregate(short cityId, long temperature) {
    }

    @Setup
    public void setUp() throws IOException {
        Map<String, Integer> hmm = new HashMap<>();
        AtomicInteger sequence = new AtomicInteger();

        Unsafe unsafe = UnsafeAccess.UNSAFE;
//        recreateData(hmm, sequence);

        memoryMappedFile = mmapDataFile("row-data.dat");
        int maxId = Integer.MIN_VALUE;
        for (long i = memoryMappedFile.pointer(); i < memoryMappedFile.pointer() + memoryMappedFile.size(); ) {
            i += Long.BYTES * 4;
            long cities = unsafe.getLong(i);
            i += Long.BYTES;

            short city1 = (short) (cities >> 48);
            short city2 = (short) (cities >> 32);
            short city3 = (short) (cities >> 16);
            short city4 = (short) cities;

            maxId = (short) Math.max(maxId, city1);
            maxId = (short) Math.max(maxId, city2);
            maxId = (short) Math.max(maxId, city3);
            maxId = (short) Math.max(maxId, city4);
        }

        System.out.println("maxId = " + maxId);

        data = new int[maxId + 1][];
        for (int i = 0; i < data.length; i++) {
            data[i] = new int[153];
            data[i][0] = 25;
        }
    }

    private static void recreateData(Map<String, Integer> hmm, AtomicInteger sequence) throws IOException {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(Path.of("row-data.dat"))))) {
            List<TmpAggregate> tmpAggregate = new ArrayList<>();

            Iterator<String> iterator = Files.lines(Path.of(FILE)).iterator();
            while (iterator.hasNext()) {
                String line = iterator.next();
                String[] cityAndTemperature = line.split(";");

                byte[] cityBytes = cityAndTemperature[0].getBytes(StandardCharsets.UTF_8);
                String city = new String(cityBytes, StandardCharsets.UTF_8);

                long temperature = getTemperature(cityAndTemperature);

                short cityId = hmm.computeIfAbsent(city, _ -> sequence.getAndIncrement()).shortValue();
                tmpAggregate.add(new TmpAggregate(cityId, temperature));
                if (tmpAggregate.size() == 4) {
                    TmpAggregate tmpAggregate0 = tmpAggregate.get(0);
                    TmpAggregate tmpAggregate1 = tmpAggregate.get(1);
                    TmpAggregate tmpAggregate2 = tmpAggregate.get(2);
                    TmpAggregate tmpAggregate3 = tmpAggregate.get(3);

                    long temperature0 = tmpAggregate0.temperature();
                    long temperature1 = tmpAggregate1.temperature();
                    long temperature2 = tmpAggregate2.temperature();
                    long temperature3 = tmpAggregate3.temperature();

                    long cityId0 = tmpAggregate0.cityId();
                    long cityId1 = tmpAggregate1.cityId();
                    long cityId2 = tmpAggregate2.cityId();
                    long cityId3 = tmpAggregate3.cityId();

                    long cities = cityId0 | (cityId1 << 16) | (cityId2 << 32) | (cityId3 << 48);

                    tmpAggregate.clear();

                    try {
                        output.writeLong(Long.reverseBytes(temperature0));
                        output.writeLong(Long.reverseBytes(temperature1));
                        output.writeLong(Long.reverseBytes(temperature2));
                        output.writeLong(Long.reverseBytes(temperature3));
                        output.writeLong(Long.reverseBytes(cities));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    private static long getTemperature(String[] cityAndTemperature) {
        byte[] temperatureBytes = cityAndTemperature[1].getBytes(StandardCharsets.UTF_8);
        long temperature = 0;
        if (temperatureBytes[0] == '-') {
            if (temperatureBytes.length == 5) {
                byte temp1 = (byte) (temperatureBytes[1] - '0');
                byte temp2 = (byte) (temperatureBytes[2] - '0');
                byte temp3 = (byte) (temperatureBytes[4] - '0');

                short tmp1 = (short) -temp1;
                short tmp2 = (short) -temp2;
                short tmp3 = (short) -temp3;

                long t1 = tmp1 & 0xFF_FF;
                long t2 = tmp2 & 0xFF_FF;
                long t3 = tmp3 & 0xFF_FF;

                temperature = t3 | (t2 << 16) | (t1 << 32);
            } else if (temperatureBytes.length == 4) {
                byte temp1 = (byte) (temperatureBytes[1] - '0');
                byte temp2 = (byte) (temperatureBytes[3] - '0');

                short tmp1 = (short) -temp1;
                short tmp2 = (short) -temp2;

                long t1 = tmp1 & 0xFF_FF;
                long t2 = tmp2 & 0xFF_FF;

                temperature = t2 | (t1 << 16);
            }
        } else {
            if (temperatureBytes.length == 4) {
                long temp1 = (byte) (temperatureBytes[0] - '0');
                long temp2 = (byte) (temperatureBytes[1] - '0');
                long temp3 = (byte) (temperatureBytes[3] - '0');

                temperature = temp3 | (temp2 << 16) | (temp1 << 32);
            } else if (temperatureBytes.length == 3) {
                long temp1 = (byte) (temperatureBytes[0] - '0');
                long temp2 = (byte) (temperatureBytes[2] - '0');

                temperature = temp2 | (temp1 << 16);
            }
        }
        return temperature;
    }

    @TearDown(Level.Iteration)
    public void clear() {
        for (int i = 0; i < data.length; i++) {
            data[i][0] = 25;
        }
    }

    record MinMaxSum(int min, int max, int sum) {
    }


    //    @Benchmark
    public void aggregate() {
        Unsafe unsafe = UnsafeAccess.UNSAFE;

        for (long i = memoryMappedFile.pointer(); i < memoryMappedFile.pointer() + memoryMappedFile.size() - 4 * Long.BYTES; ) {
            long tmpValue1 = unsafe.getLong(i);
            i += Long.BYTES;
            long tmpValue2 = unsafe.getLong(i);
            i += Long.BYTES;

            handleCity((int) (tmpValue1 >> 32));
            handleCity((int) tmpValue1);
            handleCity((int) (tmpValue2 >> 32));
            handleCity((int) tmpValue2);
        }
    }

    @Benchmark
    public void aggregate2() {
        Unsafe unsafe = UnsafeAccess.UNSAFE;

        MemorySegment memorySegment = MemorySegment
                .ofAddress(memoryMappedFile.pointer())
                .reinterpret(memoryMappedFile.size());

        // -12.3 12.3 2.3 -2.3
        // 00 -01 -02 -03 | 00 01 02 03 | 00 00 02 03 | 00 00 -02 -03
        // 0 100 10 1
        //
//        for (long i = memoryMappedFile.pointer(); i < memoryMappedFile.pointer() + memoryMappedFile.size() - 4 * Long.BYTES; ) {
        for (long i = 0; i < memoryMappedFile.size() - 4 * Long.BYTES; ) {
            ShortVector shortVector = ShortVector.fromMemorySegment(ShortVector.SPECIES_256, memorySegment, i, ByteOrder.nativeOrder());
            shortVector = shortVector.mul(ATOI_MUL);

            short temp1 = shortVector.reduceLanes(VectorOperators.ADD, ATOI_REDUCE_1);
            short temp2 = shortVector.reduceLanes(VectorOperators.ADD, ATOI_REDUCE_2);
            short temp3 = shortVector.reduceLanes(VectorOperators.ADD, ATOI_REDUCE_3);
            short temp4 = shortVector.reduceLanes(VectorOperators.ADD, ATOI_REDUCE_4);

            i += LongVector.SPECIES_256.vectorByteSize();
            long cities = memorySegment.get(ValueLayout.JAVA_LONG, i);
            i += Long.BYTES;

            short city1 = (short) (cities >> 48);
            short city2 = (short) (cities >> 32);
            short city3 = (short) (cities >> 16);
            short city4 = (short) cities;

            handleCity(city1, temp1);
            handleCity(city2, temp2);
            handleCity(city3, temp3);
            handleCity(city4, temp4);
        }
    }

    private void handleCity(short cityId, short temperature) {
        int[] cityData = data[cityId];
        int index = cityData[0];
        cityData[index++] = temperature;

        if (index == 153) {
            aggregateMinMaxSum(cityData);
            index = 25;
        }

        cityData[0] = index;
    }

    private void handleCity(int tmpValue) {
        short cityId = (short) tmpValue;
        int temperature = tmpValue >> 16;

        int[] cityData = data[cityId];
        int index = cityData[0];
        cityData[index++] = temperature;

        if (index == 153) {
            aggregateMinMaxSum(cityData);
            index = 25;
        }

        cityData[0] = index;
    }

    private void handleCity(long tmpValue) {
        short cityId = (short) tmpValue;
        int temperature = (int) (tmpValue >> 32);

        int[] cityData = data[cityId];
        int index = cityData[0];
        cityData[index++] = temperature;

        if (index == 153) {
            aggregateMinMaxSum(cityData);
            index = 25;
        }

        cityData[0] = index;
    }

    private void aggregateMinMaxSum(int[] data) {
        IntVector minVector = IntVector.fromArray(IntVector.SPECIES_256, data, 1);
        IntVector maxVector = IntVector.fromArray(IntVector.SPECIES_256, data, 9);
        IntVector sumVector = IntVector.fromArray(IntVector.SPECIES_256, data, 17);

        IntVector intVector1 = IntVector.fromArray(IntVector.SPECIES_256, data, 25);
        IntVector intVector2 = IntVector.fromArray(IntVector.SPECIES_256, data, 57);
        IntVector intVector3 = IntVector.fromArray(IntVector.SPECIES_256, data, 89);
        IntVector intVector4 = IntVector.fromArray(IntVector.SPECIES_256, data, 121);

        minVector = minVector.min(intVector1);
        minVector = minVector.min(intVector2);
        minVector = minVector.min(intVector3);
        minVector = minVector.min(intVector4);

        maxVector = maxVector.max(intVector1);
        maxVector = maxVector.max(intVector2);
        maxVector = maxVector.max(intVector3);
        maxVector = maxVector.max(intVector4);

        sumVector = sumVector.add(intVector1);
        sumVector = sumVector.add(intVector2);
        sumVector = sumVector.add(intVector3);
        sumVector = sumVector.add(intVector4);

        minVector.intoArray(data, 1);
        maxVector.intoArray(data, 9);
        sumVector.intoArray(data, 17);
    }


    //    @Benchmark
    public List<MinMaxSum> sumMinMax() {
        VectorSpecies<Integer> species256 = IntVector.SPECIES_256;
        List<MinMaxSum> result = new ArrayList<>(data.length);

        for (int i = 0; i < data.length; i++) {
            IntVector minVector = IntVector.broadcast(species256, Integer.MAX_VALUE);
            IntVector maxVector = IntVector.broadcast(species256, Integer.MIN_VALUE);
            IntVector sumVector = IntVector.broadcast(species256, 0);

            for (int j = 0; j < data[i].length; j += species256.length()) {
                IntVector intVector = IntVector.fromArray(species256, data[i], j);
                minVector = minVector.min(intVector);
                maxVector = maxVector.max(intVector);
                sumVector = sumVector.add(intVector);
            }

            int min = minVector.reduceLanes(VectorOperators.MIN);
            int max = maxVector.reduceLanes(VectorOperators.MAX);
            int sum = sumVector.reduceLanes(VectorOperators.ADD);
            result.add(new MinMaxSum(min, max, sum));
        }

        return result;
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
//        Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
//        Class<? extends Profiler> profilerClass = AsyncProfiler.class;
//        Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(DistributeMicrobenchmark.class.getSimpleName())
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
