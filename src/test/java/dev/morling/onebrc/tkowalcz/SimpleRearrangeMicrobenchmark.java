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

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void rearrangeBytes() {
        ByteVector rearranged = byteVector1.rearrange(byteShuffle, byteVector2);
        rearranged.intoArray(byteOutput, 0);
    }

    /*
     * On Zen2:
     * vmovdqu xmm0,XMMWORD PTR [r11+0x18]
     * vmovdqu xmm1,XMMWORD PTR [r10+0x18]
     * vpshufb xmm0,xmm0,xmm1
     * vmovdqu XMMWORD PTR [r8+0x18],xmm0
     *
     * On Milan (??!!):
     * cpu family      : 25
     * model           : 1
     * model name      : AMD EPYC-Milan Processor
     * stepping        : 1
     * microcode       : 0x1000065
     * flags           : fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm rep_good nopl cpuid extd_apicid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm cmp_legacy cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw topoext perfctr_core invpcid_single ssbd ibrs ibpb stibp vmmcall fsgsbase bmi1 avx2 smep bmi2 erms invpcid rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec xgetbv1 xsaves clzero xsaveerptr wbnoinvd arat umip pku ospke rdpid fsrm
     * bugs            : sysret_ss_attrs null_seg spectre_v1 spectre_v2 spec_store_bypass srso
     *
     * vmovdqu ymm0,YMMWORD PTR [r11+0x18]
     * vmovdqu ymm1,YMMWORD PTR [r10+0x18]
     * vperm2i128 ymm3,ymm0,ymm0,0x1
     * vpshufb ymm3,ymm3,ymm1
     * vpshufb ymm2,ymm0,ymm1
     * vpaddb ymm4,ymm1,YMMWORD PTR [rip+0xffffffffffa8350b]
     * vpblendvb ymm2,ymm2,ymm3,ymm4
     * vmovdqu YMMWORD PTR [r8+0x18],ymm2
     *
     *
     *
     * vmovdqu ymm0,YMMWORD PTR [r11+0x18]
     * vmovdqu ymm1,YMMWORD PTR [r10+0x18]
     * vpermb ymm0,ymm1,ymm0
     * vmovdqu YMMWORD PTR [r8+0x18],ymm0
     *
     */
    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void rearrangeBytesSimple() {
        ByteVector rearranged = byteVector1.rearrange(byteShuffle);
        rearranged.intoArray(byteOutput, 0);
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void rearrangeIntegers() {
        IntVector rearranged = intVector1.rearrange(intShuffle, intVector2);
        rearranged.intoArray(intOutput, 0);
    }

    /*
     * Intel Sapphire Rapids (AWS):
     *
     * cpu family	: 6
     * model		: 143
     * model name	: Intel(R) Xeon(R) Platinum 8488C
     * stepping	: 8
     * microcode	: 0x2b000571
     *
     * vmovdqu ymm0,YMMWORD PTR [r11+0x18]
     * vmovq  xmm1,QWORD PTR [r10+0x18]
     * vpmovzxbd ymm1,xmm1
     * vpermd ymm0,ymm1,ymm0
     * vmovdqu YMMWORD PTR [r8+0x18],ymm0
     *
     */
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
