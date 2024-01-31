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
import jdk.incubator.vector.Vector;
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/*
Benchmark                                                  Mode  Cnt  Score   Error  Units
UnsignedComparisonMicrobenchmark.comparisonGE              avgt    2  1.451          ns/op
UnsignedComparisonMicrobenchmark.comparisonGE:asm          avgt         NaN            ---
UnsignedComparisonMicrobenchmark.comparisonLE              avgt    2  1.441          ns/op
UnsignedComparisonMicrobenchmark.comparisonLE:asm          avgt         NaN            ---
UnsignedComparisonMicrobenchmark.comparisonUnsignedGE      avgt    2  1.421          ns/op
UnsignedComparisonMicrobenchmark.comparisonUnsignedGE:asm  avgt         NaN            ---
UnsignedComparisonMicrobenchmark.comparisonUnsignedLE      avgt    2  1.485          ns/op
UnsignedComparisonMicrobenchmark.comparisonUnsignedLE:asm  avgt         NaN            ---
 */
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
        "-XX:+UseEpsilonGC",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
})
@Threads(1)
public class UnsignedComparisonMicrobenchmark {

    private static final Vector<Byte> DELIMITER_VECTOR = ByteVector.SPECIES_256.broadcast(';');
    private ByteVector byteVector;

    @Setup
    public void setup() {
        byteVector = ByteVector.fromArray(ByteVector.SPECIES_256, new byte[]{
                0, 1, 2, 3, 4, 5, 6, 7,
                0, 1, 2, 3, 4, 5, 6, 7,
                0, 1, 2, 3, 4, 5, 6, 7,
                0, 1, 2, 3, 4, 5, 6, 7,
        },
                0);
    }

    @Benchmark
    public int comparisonEq() {
        VectorMask<Byte> mask = byteVector.compare(VectorOperators.EQ, DELIMITER_VECTOR);
        return (int) mask.toLong();
    }

    /*
     * 0.04% │ 0x00007fd8044c58c6: vmovdqu ymm0,YMMWORD PTR [rdi+0x10]
     * 0.30% │ 0x00007fd8044c58db: vmovdqu ymm1,YMMWORD PTR [r12+r10*8+0x10]
     * 
     * 0.07% │ 0x00007fd8044c58e6: vpcmpgtb ymm4,ymm1,ymm0
     * 8.02% │ 0x00007fd8044c58ea: vpcmpeqd ymm3,ymm3,ymm3 ; setting ymm3 to all ones
     * 1.09% │ 0x00007fd8044c58ee: vpxor ymm4,ymm3,ymm4
     * 10.82% │ 0x00007fd8044c58f2: vpmovmskb r10d,ymm4
     */

    @Benchmark
    public int comparisonLE() {
        VectorMask<Byte> mask = byteVector.compare(VectorOperators.LE, DELIMITER_VECTOR);
        return (int) mask.toLong();
    }

    /*
     * 0.14% │ 0x00007ff0e04c526f: vpxor ymm2,ymm3,ymm1
     * 16.96% │ 0x00007ff0e04c5273: vpxor ymm3,ymm3,ymm0
     * 8.54% │ 0x00007ff0e04c5277: vpcmpgtb ymm2,ymm2,ymm3
     * 5.75% │ 0x00007ff0e04c527b: vpcmpeqd ymm3,ymm3,ymm3
     * 0.02% │ 0x00007ff0e04c527f: vpxor ymm2,ymm3,ymm2
     * 15.42% │ 0x00007ff0e04c5283: vpmovmskb r10d,ymm2
     */
    @Benchmark
    public int comparisonUnsignedLE() {
        VectorMask<Byte> mask = byteVector.compare(VectorOperators.UNSIGNED_LE, DELIMITER_VECTOR);
        return (int) mask.toLong();
    }

    /*
     * 7.46% │ 0x00007ff4444c661e: vmovdqu ymm0,YMMWORD PTR [rdi+0x10]
     * 0.31% │ 0x00007ff4444c662b: vmovdqu ymm1,YMMWORD PTR [r12+r10*8+0x10]
     * 
     * 0.09% │ 0x00007ff4444c6636: vpcmpgtb ymm4,ymm0,ymm1
     * 7.35% │ 0x00007ff4444c663a: vpcmpeqd ymm2,ymm2,ymm2
     * 1.09% │ 0x00007ff4444c663e: vpxor ymm4,ymm2,ymm4
     * 10.80% │ 0x00007ff4444c6642: vpmovmskb r10d,ymm4
     */
    @Benchmark
    public int comparisonGE() {
        VectorMask<Byte> mask = byteVector.compare(VectorOperators.GE, DELIMITER_VECTOR);
        return (int) mask.toLong();
    }

    /*
     * 14.19% ││ 0x00007f32c41f781e: vmovdqu ymm0,YMMWORD PTR [rdi+0x10]
     * 0.58% ││ 0x00007f32c41f782b: vmovdqu ymm1,YMMWORD PTR [r12+r10*8+0x10]
     * 
     * 0.14% ││ 0x00007f32c41f7836: vpcmpgtb ymm4,ymm0,ymm1
     * 15.35% ││ 0x00007f32c41f783a: vpcmpeqd ymm2,ymm2,ymm2
     * 2.23% ││ 0x00007f32c41f783e: vpxor ymm4,ymm2,ymm4
     * 21.71% ││ 0x00007f32c41f7842: vpmovmskb r10d,ymm4
     */
    @Benchmark
    public int comparisonUnsignedGE() {
        VectorMask<Byte> mask = byteVector.compare(VectorOperators.GE, DELIMITER_VECTOR);
        return (int) mask.toLong();
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(UnsignedComparisonMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)
                .build();

        new Runner(opt).run();
    }
}
