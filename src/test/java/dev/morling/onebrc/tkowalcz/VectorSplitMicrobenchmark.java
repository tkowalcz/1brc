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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(value = 0, jvmArgsPrepend = {
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
public class VectorSplitMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> SEMICOLON_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> MINUS_VECTOR = SPECIES.broadcast('-');

    private byte[] line;

    @Setup
    public void setup() {
        line = "Jayapura;-5.3\nDetroit;7.9\nLas Ve".getBytes(StandardCharsets.UTF_8);
    }

    // Jayapura;-5.3\Detroit;7.9\Las Ve
    // ........T............T.......... == ;
    // ........TTTTTT.......TTTTT...T.. <= ;
    // .........TTTTT........TTTT...... < ;
    // .............T...........T...... == \n
    // compress
    // -5.3\7.9\
    //
    // Jayapura;-5.3\Detroit;7.9\Las Ve
    // .........TTTTT........TTTT...... < ;
    // .............T...........T...... == \n
//    @Benchmark
//    public float backToTheDrawingBoard_aligned() {
//        Vector<Byte> byteVector = ByteVector.fromArray(SPECIES, line, 0);
//        VectorMask<Byte> vectorMask1 = byteVector.compare(VectorOperators.LT, SEMICOLON_VECTOR);
//        VectorMask<Byte> vectorMask2 = byteVector.compare(VectorOperators.GE, MINUS_VECTOR);
//
//        vectorMask1.and(vectorMask2);
//
//        Vector<Byte> compress = byteVector.compress(vectorMask);
//
//        return compress.reduceLanesToLong(VectorOperators.ADD);
//    }

    public static void main(String[] args) throws RunnerException {
        Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
//        Class<? extends Profiler> profilerClass = AsyncProfiler.class;
//        Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(VectorSplitMicrobenchmark.class.getSimpleName())
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
