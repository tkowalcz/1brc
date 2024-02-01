package dev.morling.onebrc.tkowalcz;

import dev.morling.onebrc.IoUtil;
import jdk.incubator.vector.Vector;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+AlwaysPreTouch",
        "-XX:+DebugNonSafepoints",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        // "-XX:+UseEpsilonGC",
        "-XX:-UseCompressedOops",
        // "-XX:+PrintInlining",
        // "-XX:-UseCompressedClassPointers",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
        // "-XX:+HeapDumpOnOutOfMemoryError",
        // "-XX:MaxDirectMemorySize=12884901888",
        // "-XX:CompileCommand=dontinline,dev.morling.onebrc.tkowalcz.CalculateAverage_tkowalcz2::hashMiss",
        // "-XX:CompileCommand=dontinline,dev.morling.onebrc.tkowalcz.CalculateAverage_tkowalcz2.RawHashMap::aggregateMinMaxSum",
        // "-XX:CompileCommand=inline,dev.morling.onebrc.tkowalcz.CalculateAverage_tkowalcz2::getCityId",
        // "-XX:CompileCommand=dontinline,dev.morling.onebrc.tkowalcz.DivideAndConquerMicrobenchmark::process",
        // "-XX:CompileCommand=dontinline,dev.morling.onebrc.tkowalcz.JustTempsInaSmallVectorMicrobenchmark::process",
        // "-XX:CompileCommand=dontinline,dev.morling.onebrc.tkowalcz.JustCitiesInASmallVectorMicrobenchmark::process",
         "-XX:CompileCommand=inline,dev.morling.onebrc.tkowalcz.CalculateAverage2Unsafe::addMeasurement",
        "-Xmx8g",
        "-Xms8g",
        "-XX:-AlwaysPreTouch",
        "-XX:+UseTransparentHugePages",
        "-XX:-TieredCompilation",
        "-XX:CompileThreshold=2048",
        "-XX:-UseCountedLoopSafepoints",
        "-XX:+TrustFinalNonStaticFields"
})
@Threads(1)
public class OneBrcMicrobenchmark {

    static final String FILE = "measurements.txt";

    public static String toString(Vector<Byte> data) {
        byte[] array = data.reinterpretAsBytes().toArray();
        return new String(array, StandardCharsets.UTF_8).replace('\n', '\\');
    }

    static MemorySegment mmapDataFile(String fileName, Arena arena) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
                FileChannel channel = file.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
    }

    static MemoryMappedFile mmapDataFile(String fileName) throws IOException {
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

    private static int notInDebugMode() {
        boolean isDebug = ManagementFactory.getRuntimeMXBean()
                .getInputArguments()
                .toString()
                .contains("-agentlib:jdwp");

        return isDebug ? 0 : 1;
    }

    public static void runWithPerfAsm(String benchmarkName) throws RunnerException {
        runWith(benchmarkName, LinuxPerfAsmProfiler.class, "tooBigThreshold=10000");
    }

    public static void runWithPerfNorm(String benchmarkName) throws RunnerException {
        runWith(benchmarkName, LinuxPerfNormProfiler.class, "");
    }

    public static void runWithJFR(String benchmarkName) throws RunnerException {
        runWith(benchmarkName, JavaFlightRecorderProfiler.class, "");
    }

    public static void runWithAsync(String benchmarkName) throws RunnerException {
        runWith(benchmarkName,
                AsyncProfiler.class,
                "libPath=/root/libasyncProfiler.so;output=flamegraph;dir=profile-results");
    }

    public static void run(String benchmarkName) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(benchmarkName)
                .forks(notInDebugMode())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .build();

        new Runner(opt).run();
    }

    public static void runWith(String benchmarkName, Class<? extends Profiler> profilerClass) throws RunnerException {
        runWith(benchmarkName, profilerClass, "");
    }

    public static void runWith(String benchmarkName, Class<? extends Profiler> profilerClass, String initLine) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(benchmarkName)
                .forks(notInDebugMode())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass, initLine)
                .build();

        new Runner(opt).run();
    }
}
