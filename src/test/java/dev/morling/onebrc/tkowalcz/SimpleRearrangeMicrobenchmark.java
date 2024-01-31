package dev.morling.onebrc.tkowalcz;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorShuffle;
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
        "-XX:+EnableVectorSupport",
        "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+UseEpsilonGC",
        // "-XX:+PrintFlagsFinal",
        "-XX:-UseCompressedOops",
        "-XX:-UseCompressedClassPointers",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
        "-Xmx8g",
        "-Xmn8g"
})
@Threads(1)
public class SimpleRearrangeMicrobenchmark {

    private ByteVector byteVector1;
    private ByteVector byteVector2;
    private VectorShuffle<Byte> byteShuffle;
    byte[] byteOutput;

    private IntVector intVector1;
    private IntVector intVector2;
    private VectorShuffle<Integer> intShuffle;
    int[] intOutput;

    @Setup
    public void setUp() {
        byte[] data1 = "London;14.6\nUpington;20.4\nPalerm".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "Palermo;18.2\nBrussels;3.2\nYaoundé;27.4".getBytes(StandardCharsets.UTF_8);

        byteVector1 = ByteVector.fromArray(ByteVector.SPECIES_256, data1, 0);
        byteVector2 = ByteVector.fromArray(ByteVector.SPECIES_256, data2, 0);

        byteShuffle = VectorShuffle.fromValues(ByteVector.SPECIES_256,
                0, 1, 2, 3, 4, 5, 6, 7, 8,
                9, 10, 11, 12, 13, 14, 15,
                16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, 26, 27, 28, 29, 30, 31);
        byteOutput = new byte[32];

        intVector1 = byteVector1.reinterpretAsInts();
        intVector2 = byteVector2.reinterpretAsInts();

        intShuffle = VectorShuffle.fromValues(IntVector.SPECIES_256, 0, 1, 2, 3, 4, 5, 6, 7);
        intOutput = new int[32];
    }

    // @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void rearrangeBytes() {
        ByteVector rearranged = byteVector1.rearrange(byteShuffle, byteVector2);
        rearranged.intoArray(byteOutput, 0);
    }

    /*
     * 0.03% │ 0x00007f46d44bfc09: vmovdqu xmm0,XMMWORD PTR [r11+0x18]
     * 0.41% │ 0x00007f46d44bfc0f: vmovdqu xmm1,XMMWORD PTR [r10+0x18]
     * 3.17% │ 0x00007f46d44bfc15: vpshufb xmm0,xmm0,xmm1
     * 2.89% │ 0x00007f46d44bfc1a: vmovdqu XMMWORD PTR [r8+0x18],xmm0
     *
     */
    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void rearrangeBytesSimple() {
        ByteVector rearranged = byteVector1.rearrange(byteShuffle);
        rearranged.intoArray(byteOutput, 0);
    }

    // @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void rearrangeIntegers() {
        IntVector rearranged = intVector1.rearrange(intShuffle, intVector2);
        rearranged.intoArray(intOutput, 0);
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void rearrangeIntegersSimple() {
        IntVector rearranged = intVector1.rearrange(intShuffle);
        rearranged.intoArray(intOutput, 0);
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = GCProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = AsyncProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;

        Options opt = new OptionsBuilder()
                .include(SimpleRearrangeMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)
                .build();

        new Runner(opt).run();
    }
}
