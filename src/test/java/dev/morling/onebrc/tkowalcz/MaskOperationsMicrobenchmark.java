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

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.profile.Profiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/*
 * A lot of mov/and/shl/shr/or-s and and-s.
 *
 * Benchmark                                  Mode  Cnt  Score   Error  Units
 * ReverseBitsMicrobenchmark.reverseBits      avgt    2  1.838          ns/op
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+DebugNonSafepoints",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+UseEpsilonGC",
        "-XX:PrintAssemblyOptions=intel"
})
@Threads(1)
public class MaskOperationsMicrobenchmark {

    private int value;

    private ByteVector byteVector;
    private ByteVector comparisonVector;

    @Setup
    public void setup() {
        value = ThreadLocalRandom.current().nextInt();

        byte[] data = "London;14.6\nUpington;20.4\nPalerm".getBytes(StandardCharsets.UTF_8);

        byteVector = ByteVector.fromArray(ByteVector.SPECIES_256, data, 0);
        comparisonVector = ByteVector.broadcast(ByteVector.SPECIES_256, '0');
    }

    // uses tzcnt, there is also lzcnt
    @Benchmark
    public int trailingZeros() {
        return Integer.numberOfTrailingZeros(value);
    }

    // @Benchmark
    public int reverseBits() {
        return Integer.reverse(value);
    }

    // @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public int maskOperationsOnVectors() {
        VectorMask<Byte> byteVectorMask = byteVector.compare(VectorOperators.GT, comparisonVector);
        return byteVectorMask.firstTrue();
    }

    // @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public int maskOperationsOnLong() {
        VectorMask<Byte> byteVectorMask = byteVector.compare(VectorOperators.GT, comparisonVector);
        long value = byteVectorMask.toLong();
        return Long.numberOfTrailingZeros(value);
    }

    // @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public int maskOperationsOnVectorsWithReverseBits() {
        VectorMask<Byte> byteVectorMask = byteVector.compare(VectorOperators.GT, comparisonVector);
        int value = (int) byteVectorMask.toLong();
        return Integer.reverse(value);
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(MaskOperationsMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .addProfiler(profilerClass)
                .build();

        new Runner(opt).run();
    }
}
