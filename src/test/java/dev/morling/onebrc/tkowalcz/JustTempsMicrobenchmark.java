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
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Fork(value = 1, jvmArgsPrepend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:PrintAssemblyOptions=intel",
        "-XX:+DebugNonSafepoints",
        "-XX:+UnlockExperimentalVMOptions",
        "-server",
        "-XX:-TieredCompilation",
        // "-XX:+AlwaysPreTouch",
        "-XX:+EnableVectorReboxing",
        "-XX:+EnableVectorAggressiveReboxing",
        "-XX:+TrustFinalNonStaticFields",
        // "-XX:InlineSmallCode=10000",
        // "-XX:CompileThreshold=2",
        // "-XX:+PrintCompilation",
        // "-XX:+PrintInlining",
        "-XX:+UseEpsilonGC",
        "-XX:CompileCommand=inline dev.morling.onebrc.tkowalczLocalPermuteMicrobenchmark::computeIfAbsent",
        "-Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0",
        "-Xmx8g",
        "-Xms8g"
})
@Threads(1)
public class JustTempsMicrobenchmark {

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');
    private static final Vector<Byte> NEWLINE_VECTOR = SPECIES.broadcast('\n');

    // Values used to multiply digits of temperature measurement to get proper int. E.g. -12.3 will be multiplied by
    // 10th element (0, -100, -10, 0, -1) giving '-' * 0 + '1' * -100 + '2' * -10 + '.' * 0 + '3' * -1 = -123.
    // There are four combinations of possible mask results from comparing (less than) vector containing temperature
    // measurement with ASCII_ZERO. Hence, only four entries are populated.
    private static final ShortVector[] STOI_MUL_LOOKUP = {
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
    };

    private static final VectorShuffle<Byte>[] TEMP_SHUFFLE = new VectorShuffle[]{
            null,
            VectorShuffle.fromValues(ByteVector.SPECIES_256, 1, 2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), // - 1 2 3 == -12.3
            VectorShuffle.fromValues(ByteVector.SPECIES_256, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), // 2 3 == 2.3
            null,
            VectorShuffle.fromValues(ByteVector.SPECIES_256, 10, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), // 1 2 3 == 12.3
            VectorShuffle.fromValues(ByteVector.SPECIES_256, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) // - 2 3 == -2.3
    };

    // We also need to know the size of temperature measurement in characters, lookup table works the same way as STOI_MUL_LOOKUP.
    private static final int[] STOI_SIZE_LOOKUP = { 0, 6, 4, 0, 5, 5 };

    private static final String FILE = "measurements.txt";

    private Arena arena;
    private MemorySegment inputData;

    private int[] hashMap;
    private MemorySegment hashMapData;
    private int hashMapDataWriteIndex;

    private short sequence = 0;

    // private MemoryMappedFile dataPointer;
    private MemorySegment output;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);
            // dataPointer = mmapDataFile(FILE);
            output = arena.allocate(5 * 1024 * 1024 * 1024L, 1);

            hashMap = new int[TABLE_SIZE];
            hashMapData = arena.allocate(10_000 * (4 + 100 + 28));
            hashMapDataWriteIndex = 4;
            sequence = 0;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @TearDown
    public void tearDown() {
        arena.close();
    }

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x4000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = 0x4000 - 1;

    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');
    static final VectorMask<Byte>[] CITY_LOOKUP_MASK = CalculateAverage_tkowalcz.createMasks32();

    // "London;14.6\nUpington;20.4\nPalerm"
    @Benchmark
    public long justParseTemps() {
        long stride = inputData.byteSize() / 2;

        long offset1 = 0;
        long end1 = stride - SPECIES.vectorByteSize();

        long offset2 = stride;
        long end2 = stride + stride - SPECIES.vectorByteSize();

        // long offset3 = stride + stride;
        // long end3 = stride + stride + stride - SPECIES.vectorByteSize();
        //
        // long offset4 = stride + stride + stride;
        // long end4 = stride + stride + stride + stride - SPECIES.vectorByteSize();

        offset1 = fastFroward(offset1);
        offset2 = fastFroward(offset2);
        // offset3 = fastFroward(offset3);
        // offset4 = fastFroward(offset4);

        long result = 0;
        while (offset1 < end1 && offset2 < end2 /* && offset3 < end3 && offset4 < end4 */) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            int firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter1 == 0) {
                offset1 = fastFroward(offset1);
                byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
                firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            }

            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            int firstDelimiter2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter2 == 0) {
                offset2 = fastFroward(offset2);
                byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
                firstDelimiter2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            }

            // Vector<Byte> byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            // int firstDelimiter3 = byteVector3.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            // if (firstDelimiter3 == 0) {
            // offset3 = fastFroward(offset3);
            // byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            // firstDelimiter3 = byteVector3.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            // }
            //
            // Vector<Byte> byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
            // int firstDelimiter4 = byteVector4.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            // if (firstDelimiter4 == 0) {
            // offset4 = fastFroward(offset4);
            // byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
            // firstDelimiter4 = byteVector4.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            // }

            // VectorShuffle<Byte> shuffle = VectorShuffle.fromValues(ByteVector.SPECIES_256, );

            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex1 = (int) mask1.toLong() & 0x07;
            long value1 = byteVector1
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1])
                    .reduceLanesToLong(VectorOperators.ADD);

            VectorMask<Byte> mask2 = byteVector2.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex2 = (int) mask2.toLong() & 0x07;
            long value2 = byteVector2
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex2])
                    .reduceLanesToLong(VectorOperators.ADD);

            // VectorMask<Byte> mask3 = byteVector3.compare(VectorOperators.LT, ASCII_ZERO);
            // int lookupIndex3 = (int) mask3.toLong() & 0x07;
            // long value3 = byteVector3
            // .sub(ASCII_ZERO)
            // .castShape(ShortVector.SPECIES_256, 0)
            // .mul(STOI_MUL_LOOKUP[lookupIndex3])
            // .reduceLanesToLong(VectorOperators.ADD);
            //
            // VectorMask<Byte> mask4 = byteVector4.compare(VectorOperators.LT, ASCII_ZERO);
            // int lookupIndex4 = (int) mask4.toLong() & 0x07;
            // long value4 = byteVector4
            // .sub(ASCII_ZERO)
            // .castShape(ShortVector.SPECIES_256, 0)
            // .mul(STOI_MUL_LOOKUP[lookupIndex4])
            // .reduceLanesToLong(VectorOperators.ADD);

            result += value1 + value2 /* + value3 + value4 */;
            offset1 += firstDelimiter1 + 1;
            offset2 += firstDelimiter2 + 1;
            // offset3 += firstDelimiter3 + 1;
            // offset4 += firstDelimiter4 + 1;
        }

        return result;
    }

    @Benchmark
    public long aggregateAndParseTemps() {
        long stride = inputData.byteSize() / 4;

        long offset1 = 0;
        long end1 = stride - SPECIES.vectorByteSize();

        long offset2 = stride;
        long end2 = stride + stride - SPECIES.vectorByteSize();

        long offset3 = stride + stride;
        long end3 = stride + stride + stride - SPECIES.vectorByteSize();

        long offset4 = stride + stride + stride;
        long end4 = stride + stride + stride + stride - SPECIES.vectorByteSize();

        offset1 = fastFroward(offset1);
        offset2 = fastFroward(offset2);
        offset3 = fastFroward(offset3);
        offset4 = fastFroward(offset4);

        long result = 0;
        while (offset1 < end1 && offset2 < end2 /* && offset3 < end3 && offset4 < end4 */) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            int firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter1 == 0) {
                offset1 = fastFroward(offset1);
                byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
                firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            }

            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            int firstDelimiter2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter2 == 0) {
                offset2 = fastFroward(offset2);
                byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
                firstDelimiter2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            }

            Vector<Byte> byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            int firstDelimiter3 = byteVector3.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter3 == 0) {
                offset3 = fastFroward(offset3);
                byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
                firstDelimiter3 = byteVector3.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            }

            Vector<Byte> byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
            int firstDelimiter4 = byteVector4.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter4 == 0) {
                offset4 = fastFroward(offset4);
                byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
                firstDelimiter4 = byteVector4.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            }

            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex1 = (int) mask1.toLong() & 0x07;

            VectorMask<Byte> mask2 = byteVector2.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex2 = (int) mask2.toLong() & 0x07;

            VectorMask<Byte> mask3 = byteVector3.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex3 = (int) mask3.toLong() & 0x07;

            VectorMask<Byte> mask4 = byteVector4.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex4 = (int) mask4.toLong() & 0x07;

            Vector<Byte> accumulator = ByteVector.SPECIES_256.zero();
            byteVector1.blend(accumulator, CITY_LOOKUP_MASK[lookupIndex1]);
            byteVector2.blend(accumulator, CITY_LOOKUP_MASK[lookupIndex2]);
            byteVector3.blend(accumulator, CITY_LOOKUP_MASK[lookupIndex3]);
            byteVector4.blend(accumulator, CITY_LOOKUP_MASK[lookupIndex4]);

            accumulator
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1]);

            // result += value1 + value2 + value3 + value4;
            offset1 += firstDelimiter1 + 1;
            offset2 += firstDelimiter2 + 1;
            offset3 += firstDelimiter3 + 1;
            offset4 += firstDelimiter4 + 1;
        }

        return result;
    }

    private long fastFroward(long offset1) {
        while (inputData.get(ValueLayout.JAVA_BYTE, offset1) != ';') {
            offset1++;
        }

        offset1++;
        return offset1;
    }

    private static String toString(Vector<Byte> data) {
        byte[] array = data.reinterpretAsBytes().toArray();
        return new String(array, StandardCharsets.UTF_8);
    }

    // private CityRecord computeIfAbsent(Vector<Byte> dataVector, VectorMask<Byte> cityNameMask,
    // int cityNameSize, int mapIndex) {
    // CityRecord cityRecord = hashMap[mapIndex];
    // if (cityRecord == null) {
    // byte[] cityName = new byte[SPECIES.length()];
    // dataVector.reinterpretAsBytes().intoArray(cityName, 0, cityNameMask);
    //
    // cityRecord = new CityRecord(sequence++, cityName, cityNameSize);
    // hashMap[mapIndex] = cityRecord;
    // }
    //
    // return cityRecord;
    // }

    private static int getHash(Vector<Byte> city) {
        int perfectHash32 = city.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
        return perfectHash32 & TABLE_SIZE_MASK;
    }

    public static VectorShuffle<Byte>[] createShuffles() {
        VectorShuffle<Byte>[] result = new VectorShuffle[33];

        VectorShuffle<Byte> iota = VectorShuffle.iota(SPECIES, 0, 1, false);
        for (int i = 1; i < result.length; i++) {
            result[i] = iota.toVector().add(SPECIES.broadcast(i)).toShuffle();
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
        // Class<? extends Profiler> profilerClass = LinuxPerfProfiler.class;
        // Class<? extends Profiler> profilerClass = AsyncProfiler.class;
        // Class<? extends Profiler> profilerClass = LinuxPerfNormProfiler.class;
        Class<? extends Profiler> profilerClass = LinuxPerfAsmProfiler.class;
        // Class<? extends Profiler> profilerClass = JavaFlightRecorderProfiler.class;

        Options opt = new OptionsBuilder()
                .include(JustTempsMicrobenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(2)
                .resultFormat(ResultFormatType.CSV)
                .jvmArgsAppend("--add-modules", "jdk.incubator.vector")
                .addProfiler(profilerClass)// , "libPath=/root/libasyncProfiler.so;output=flamegraph;dir=profile-results")
                .build();

        new Runner(opt).run();
    }
}
