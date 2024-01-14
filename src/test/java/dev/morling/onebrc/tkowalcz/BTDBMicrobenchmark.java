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
import dev.morling.onebrc.UnsafeAccess;
import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
})
@Threads(1)
public class BTDBMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast('\n');

    private static final String FILE = "measurements.txt";

    public static final int TABLE_SIZE = 0x400000;

    private Arena arena;
    private MemorySegment inputData;
    private CalculateAverage_tkowalcz.StatisticsAggregate[] dataTable;
    private MemoryMappedFile dataPointer;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);
            dataPointer = mmapDataFile(FILE);
            dataTable = new CalculateAverage_tkowalcz.StatisticsAggregate[TABLE_SIZE];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public long backToTheDrawingBoard_unaligned() {
        int vectorByteSize = SPECIES.vectorByteSize();

        long offset = 0;
        long vectorOffset = 0;
        long end = inputData.byteSize() - 2L * SPECIES.length();

        long result = 0;
        long loopIterations = 0;
        while (offset < end) {
            loopIterations++;
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, vectorOffset, ByteOrder.nativeOrder());
//            vectorOffset += vectorByteSize;
//            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, vectorOffset, ByteOrder.nativeOrder());

            int firstDelimiter = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            byteVector1.convertShape(VectorOperators.B2S, ShortVector.SPECIES_256, 0);
            offset += firstDelimiter + 1;
            vectorOffset = offset & 0xFF_FF_FF_FF_FF_FF_FF_E0L;
        }

//        System.out.println("loopIterations = " + loopIterations);
        return result;
    }

    @Benchmark
    public long backToTheDrawingBoard_aligned() {
        int vectorByteSize = SPECIES.vectorByteSize();

        long offset = 0;
        long end = inputData.byteSize() - 2L * SPECIES.length();

        long result = 0;
        long loopIterations = 0;
        while (offset < end) {
            loopIterations++;

            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset, ByteOrder.nativeOrder());
            int firstDelimiter = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            result += firstDelimiter;
            offset += vectorByteSize;
        }

        System.out.println("loopIterations = " + loopIterations);
        return result;
    }

    @Benchmark
    public long iterative() {
        Unsafe unsafe = UnsafeAccess.UNSAFE;

        long offset = dataPointer.pointer();
        long end = offset + dataPointer.size() - 2L * SPECIES.length();

        long result = 0;
        while (offset < end) {
            long value = unsafe.getLong(offset);
            result += value & 0x30_30_30_30_30_30_30_30L;
            offset += Long.BYTES;
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
                .include(BTDBMicrobenchmark.class.getSimpleName())
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
