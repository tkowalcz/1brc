package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.CalculateAverage_tkowalcz;
import dev.morling.onebrc.IoUtil;
import jdk.incubator.vector.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
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
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+DebugNonSafepoints",
        "-XX:+UnlockExperimentalVMOptions",
        "-server",
        "-XX:-TieredCompilation",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+TrustFinalNonStaticFields",
        // "-XX:+UseEpsilonGC",
        "-XX:CompileCommand=inline dev.morling.onebrc.tkowalczLocalPermuteMicrobenchmark::computeIfAbsent",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
        "-Xmx8g",
        "-Xms8g"
})
@Threads(1)
public class ShuffleObjectVsValueMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');

    private VectorShuffle<Byte>[] shuffles;
    private int[][] byteShuffles;
    private byte[] data;

    @Setup
    public void setup() {
        shuffles = new VectorShuffle[32];

        for (int i = 1; i < shuffles.length; i++) {
            shuffles[i] = VectorShuffle.iota(SPECIES, 0, 1, false);
        }

        byteShuffles = new int[32][];
        for (int i = 1; i < shuffles.length; i++) {
            byteShuffles[i] = new int[32];
        }

        data = "London;14.6\nUpington;20.4\nPalerm".getBytes(StandardCharsets.UTF_8);
    }

    @Benchmark
    public long object() {
        ByteVector byteVector = ByteVector.fromArray(SPECIES, data, 0);
        VectorMask<Byte> mask = byteVector.compare(VectorOperators.EQ, DELIMITER_VECTOR);

        VectorShuffle<Byte> shuffle = shuffles[mask.firstTrue()];
        ByteVector shuffledVector = byteVector.rearrange(shuffle);

        return shuffledVector.lane(1);
    }

    @Benchmark
    public long array() {
        ByteVector byteVector = ByteVector.fromArray(SPECIES, data, 0);
        VectorMask<Byte> mask = byteVector.compare(VectorOperators.EQ, DELIMITER_VECTOR);
        VectorShuffle<Byte> shuffle = VectorShuffle.fromArray(SPECIES, byteShuffles[mask.firstTrue()], 0);
        ByteVector shuffledVector = byteVector.rearrange(shuffle);

        return shuffledVector.lane(1);
    }

    public static void main(String[] args) throws RunnerException {
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = AsyncProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;
        // Class<? extends Profiler> profilerClass = JavaFlightRecorderProfiler.class;

        Options opt = new OptionsBuilder()
                .include(ShuffleObjectVsValueMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)// , "libPath=/root/libasyncProfiler.so;output=flamegraph;dir=profile-results")
                .build();

        new Runner(opt).run();
    }
}
