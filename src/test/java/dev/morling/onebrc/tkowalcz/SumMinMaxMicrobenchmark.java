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
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
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
//        "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
        "-Xmx8g",
        "-Xmn8g"
})
@Threads(1)
public class SumMinMaxMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast('\n');

    private static final String FILE = "measurements.txt";

    public static final int TABLE_SIZE = 0x400000;

    private int[][] data;

    @Setup
    public void setUp() {
        data = new int[400][];
        for (int i = 0; i < data.length; i++) {
            data[i] = new int[2_500_000];
        }
    }

    record MinMaxSum(int min, int max, int sum) {
    }

    @Benchmark
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
        Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
//        Class<? extends Profiler> profilerClass = AsyncProfiler.class;
//        Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(SumMinMaxMicrobenchmark.class.getSimpleName())
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
