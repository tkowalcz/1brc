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
package dev.morling.onebrc;

import jdk.incubator.vector.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

/**
 * This solution has two (conscious) assumptions about the input data:
 * <ol>
 * <li>The measurements can be numbers in one of four forms: -XX.X, XX.X, -X.X, X.X</li>
 * <li>The city name fits into vector register (has at most 32 bytes). This is a soft requirement: I check for this
 * condition and will execute fallback if that is not the case, but I did not code the fallback.
 * </li>
 * </ol>
 * <p>
 * For top speed we <b>hope</b> that the "hash" function has no collisions. If that is not the case we hit a very slow
 * fallback path that ensures correctness.
 * <p>
 *  I would prefer to split this class but don't want to pollute source tree and vectorisation breaks when split into methods.
 */
public class CalculateAverage_tkowalcz2Unsafe {

    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');

    // Used to identify positions where vector containing temperature measurement has '-', '.' and '\n' characters.
    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');

    private static final FloatVector MINUS_ASCII_ZERO = FloatVector.broadcast(FloatVector.SPECIES_256, -'0');

    private static final Vector<Byte> DELIMITER_MINUS_ASCII_ZERO_VECTOR = ByteVector.SPECIES_256.broadcast(';' - '0');

    static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

    // Values used to multiply digits of temperature measurement to get proper int. E.g. -12.3 will be multiplied by
    // 10th element (0, -100, -10, 0, -1) giving '-' * 0 + '1' * -100 + '2' * -10 + '.' * 0 + '3' * -1 = -123.
    // There are four combinations of possible mask results from comparing (less than) vector containing temperature
    // measurement with ASCII_ZERO. Hence, only four entries are populated.
//    private static final ShortVector[] STOI_MUL_LOOKUP = shiftedLookups();
    private static final ShortVector[] STOI_MUL_LOOKUP = {
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0)
    };

    private static final FloatVector[] STOI_MUL_LOOKUP_F = {
            FloatVector.fromArray(FloatVector.SPECIES_256, new float[]{0, 0, 0, 0, 0, 0, 0, 0}, 0),
            FloatVector.fromArray(FloatVector.SPECIES_256, new float[]{0, -100, -10, 0, -1, 0, 0, 0}, 0),
            FloatVector.fromArray(FloatVector.SPECIES_256, new float[]{10, 0, 1, 0, 0, 0, 0, 0}, 0),
            FloatVector.fromArray(FloatVector.SPECIES_256, new float[]{0, 0, 0, 0, 0, 0, 0, 0}, 0),
            FloatVector.fromArray(FloatVector.SPECIES_256, new float[]{100, 10, 0, 1, 0, 0, 0, 0}, 0),
            FloatVector.fromArray(FloatVector.SPECIES_256, new float[]{0, -10, 0, -1, 0, 0, 0, 0}, 0)
    };

    private static ShortVector[] shiftedLookups() {
        short[][] base = {
                new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new short[]{0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new short[]{10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new short[]{100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                new short[]{0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
        };

        ShortVector[] result = new ShortVector[32 * 6];
        for (int i = 0; i < 32; i++) {
            result[i * 6 + 0] = ShortVector.fromArray(ShortVector.SPECIES_256, shiftRight(base[0], i), 0);
            result[i * 6 + 1] = ShortVector.fromArray(ShortVector.SPECIES_256, shiftRight(base[1], i), 0);
            result[i * 6 + 2] = ShortVector.fromArray(ShortVector.SPECIES_256, shiftRight(base[2], i), 0);
            result[i * 6 + 3] = ShortVector.fromArray(ShortVector.SPECIES_256, shiftRight(base[3], i), 0);
            result[i * 6 + 4] = ShortVector.fromArray(ShortVector.SPECIES_256, shiftRight(base[4], i), 0);
            result[i * 6 + 5] = ShortVector.fromArray(ShortVector.SPECIES_256, shiftRight(base[5], i), 0);
        }

        return result;
    }

    private static short[] shiftRight(short[] values, int amount) {
        short[] result = new short[values.length];
        for (int i = result.length; i > amount; i--) {
            result[i - 1] = values[i - amount - 1];
        }

        return result;
    }

    // We also need to know the size of temperature measurement in characters, lookup table works the same way as STOI_MUL_LOOKUP.
    private static final int[] STOI_SIZE_LOOKUP = {0, 6, 4, 0, 5, 5};

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x400000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = TABLE_SIZE - 1;

    //    public static final int TABLE_ENTRY_SIZE = 228;
    public static final int TABLE_ENTRY_SIZE = 48;

    // public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
    //// try (Arena arena = Arena.ofShared()) {
    // long start = System.nanoTime();
    // Arena arena = Arena.ofShared();
    // int availableProcessors = 8;// Runtime.getRuntime().availableProcessors();
    //
    // MemorySegment inputData = mmapDataFile(FILE, arena);
    // List<MemorySegment> memorySegments = divideAlongNewlines(inputData, availableProcessors);
    //
    // CompletionService<List<StatisticsAggregate>> completionService = new ExecutorCompletionService<>(
    // Executors.newFixedThreadPool(
    // availableProcessors,
    // new DaemonThreadFactory()));
    // memorySegments.forEach(slice -> completionService.submit(() -> execute(arena, slice)));
    //
    // TreeMap<String, StatisticsAggregate> results = new TreeMap<>();
    // for (int i = 0; i < memorySegments.size(); i++) {
    // List<StatisticsAggregate> result = completionService.take().get();
    // for (StatisticsAggregate statisticsAggregate : result) {
    // StatisticsAggregate node = statisticsAggregate;
    // do {
    // results.merge(node.cityAsString(), node, StatisticsAggregate::merge);
    // node = node.getNext();
    // } while (node != null);
    // }
    // }
    //
    // System.out.println(results);
    // long runtime = System.nanoTime() - start;
    // System.out.println(STR."Runtime: \{TimeUnit.NANOSECONDS.toMillis(runtime)}ms");
    // }

    static List<MemorySegment> divideAlongNewlines(MemorySegment inputData, int numberOfParts) {
        List<MemorySegment> result = new ArrayList<>();

        long startingOffset = 0;
        long sliceSize = inputData.byteSize() / numberOfParts;
        do {
            long endingOffset = findPastNewline(inputData, startingOffset + sliceSize - 1);
            result.add(inputData.asSlice(startingOffset, endingOffset - startingOffset));

            startingOffset = endingOffset;
        } while (startingOffset < inputData.byteSize() - sliceSize);

        if (inputData.byteSize() - startingOffset > 0) {
            result.add(inputData.asSlice(startingOffset, inputData.byteSize() - startingOffset));
        }

        return result;
    }

    record FooHashMap(int[] hashMap, MemorySegment hashMapData, long hashMapDataWriteIndex, int sequence) {

    }

    // public static List<StatisticsAggregate> execute(Arena arena, MemorySegment inputData) {
    // int[] hashMap = new int[TABLE_SIZE];
    // MemorySegment hashMapData = arena.allocate(10_000 * (4 + 100 + 28));
    //
    // long dataSize = inputData.byteSize();
    //
    // long offset1 = 0;
    // long offset2 = findPastNewline(inputData, dataSize / 2);
    //
    // long end1 = offset2;
    // long end2 = dataSize;
    //
    // DoubleCursor doubleCursor = executeDoublePumped(inputData, dataTable, offset1, offset2, end1, end2);
    // offset1 = executeSinglePumped(inputData, dataTable, doubleCursor.offset1(), end1);
    // offset2 = executeSinglePumped(inputData, dataTable, doubleCursor.offset2(), end2);
    // Map<String, StatisticsAggregate> tailResults1 = executeScalar(inputData, offset1, end1);
    // Map<String, StatisticsAggregate> tailResults2 = executeScalar(inputData, offset2, end2);
    //
    // List<StatisticsAggregate> result = filterEmptyEntries(dataTable);
    // result.addAll(tailResults1.values());
    // result.addAll(tailResults2.values());
    //
    // return result;
    // }

    // I'm really tired at this point
    // static Map<String, StatisticsAggregate> executeScalar(MemorySegment inputData, long offset, long end) {
    // // Why getting byte data from a memory segment is so hard?
    // byte[] inputDataArray = new byte[(int) (end - offset)];
    // for (int i = 0; i < inputDataArray.length; i++) {
    // inputDataArray[i] = inputData.get(ValueLayout.JAVA_BYTE, offset + i);
    // }
    //
    // String inputString = new String(inputDataArray, StandardCharsets.UTF_8);
    // if (inputString.isEmpty()) {
    // return Collections.emptyMap();
    // }
    //
    // String[] lines = inputString.split("\n");
    // return stream(lines)
    // .map(line -> {
    // String[] cityAndTemperature = line.split(";");
    //
    // byte[] cityBytes = cityAndTemperature[0].getBytes(StandardCharsets.UTF_8);
    // StatisticsAggregate aggregate = new StatisticsAggregate(cityBytes, cityBytes.length);
    //
    // long temperature = (long) (Float.parseFloat(cityAndTemperature[1]) * 10);
    // return aggregate.accept(temperature);
    // })
    // .collect(Collectors.toMap(StatisticsAggregate::cityAsString, Function.identity(), StatisticsAggregate::merge));
    // }

    public static Cursor executeDoublePumped(
            MemorySegment inputData,
            RawHashMapUnsafe hashMap,
            long offset1,
            long end1,
            long offset2,
            long end2,
            long offset3,
            long end3) {

        while (offset1 < end1 && offset2 < end2 && offset3 < end3) {
            Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            int firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter1 == 32) {
                offset1 = findPastNewline(inputData, offset1);
                continue;
            }
            offset1 += firstDelimiter1 + 1;

            Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            int firstDelimiter2 = byteVector2.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter2 == 32) {
                offset2 = findPastNewline(inputData, offset2);
                continue;
            }
            offset2 += firstDelimiter2 + 1;

            Vector<Byte> byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            int firstDelimiter3 = byteVector3.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
            if (firstDelimiter3 == 32) {
                offset3 = findPastNewline(inputData, offset3);
                continue;
            }
            offset3 += firstDelimiter3 + 1;

            VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[firstDelimiter1];
            Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);
            VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[firstDelimiter2];
            Vector<Byte> hashInput2 = ZERO.blend(byteVector2, hashMask2);
            VectorMask<Byte> hashMask3 = CITY_LOOKUP_MASK[firstDelimiter3];
            Vector<Byte> hashInput3 = ZERO.blend(byteVector3, hashMask3);

            int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index1 = perfectHash32_1 & TABLE_SIZE_MASK;
            int cityNameOffset1 = (index1 << 5) + (index1 << 4);

            ByteVector cityVector1 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityNameOffset1, ByteOrder.nativeOrder());
            if (!cityVector1.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                cityNameOffset1 = hashMiss(hashMap, cityNameOffset1, index1, hashInput1, firstDelimiter2);
            }

            int perfectHash32_2 = hashInput2.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index2 = perfectHash32_2 & TABLE_SIZE_MASK;
            int cityNameOffset2 = (index2 << 5) + (index2 << 4);

            ByteVector cityVector2 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityNameOffset2, ByteOrder.nativeOrder());
            if (!cityVector2.compare(VectorOperators.EQ, hashInput2).allTrue()) {
                cityNameOffset2 = hashMiss(hashMap, cityNameOffset2, index2, hashInput2, firstDelimiter2);
            }

            int perfectHash32_3 = hashInput3.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
            int index3 = perfectHash32_3 & TABLE_SIZE_MASK;
            int cityNameOffset3 = (index3 << 5) + (index3 << 4);

            ByteVector cityVector3 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityNameOffset3, ByteOrder.nativeOrder());
            if (!cityVector3.compare(VectorOperators.EQ, hashInput3).allTrue()) {
                cityNameOffset3 = hashMiss(hashMap, cityNameOffset3, index3, hashInput3, firstDelimiter3);
            }

            byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex1 = (int) (mask1.toLong() & 0x07);
            int value = (int) byteVector1
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1])
                    .reduceLanesToLong(VectorOperators.ADD);
            hashMap.addMeasurement(cityNameOffset1, value);
            offset1 += STOI_SIZE_LOOKUP[lookupIndex1];

            byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
            VectorMask<Byte> mask2 = byteVector2.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex2 = (int) (mask2.toLong() & 0x07);
            value = (int) byteVector2
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex2])
                    .reduceLanesToLong(VectorOperators.ADD);
            hashMap.addMeasurement(cityNameOffset2, value);
            offset2 += STOI_SIZE_LOOKUP[lookupIndex2];

            byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
            VectorMask<Byte> mask3 = byteVector3.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex3 = (int) (mask3.toLong() & 0x07);
            value = (int) byteVector3
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex3])
                    .reduceLanesToLong(VectorOperators.ADD);
            hashMap.addMeasurement(cityNameOffset3, value);
            offset3 += STOI_SIZE_LOOKUP[lookupIndex3];
        }

        return new Cursor(offset1, offset2, offset3);
    }

    private static int hashMiss(RawHashMapUnsafe hashMap, int cityNameOffset, int index, Vector<Byte> hashInput, int delimiterPosition) {
//        if (cityNameOffset == 0) {
        hashMap.installNewCity(cityNameOffset, delimiterPosition, hashInput);
//        } else {
//            index = (index + 1) & TABLE_SIZE_MASK;
//
//        }
//
        return cityNameOffset;
    }

    public record Cursor(long offset1, long offset2, long offset3) {
    }

    public static long findPastNewline(MemorySegment inputData, long position) {
        while (inputData.get(ValueLayout.JAVA_BYTE, position) != '\n') {
            position++;

            if (position == inputData.byteSize()) {
                return position;
            }
        }

        return position + 1;
    }

    private static String toString(Vector<Byte> data) {
        byte[] array = data.reinterpretAsBytes().toArray();
        return new String(array, StandardCharsets.UTF_8).replace('\n', ' ');
    }

    private static MemorySegment mmapDataFile(String fileName, Arena arena) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
             FileChannel channel = file.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
    }

    public static VectorMask<Byte>[] createMasks32() {
        VectorMask<Byte>[] result = new VectorMask[33];
        result[0] = SPECIES.maskAll(false);

        int maskSource = 0x1;
        for (int i = 1; i < 33; i++) {
            result[i] = VectorMask.fromLong(SPECIES, maskSource);
            maskSource <<= 1;
            maskSource += 1;
        }

        return result;
    }

    static class DaemonThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread result = new Thread(r);
            result.setDaemon(true);
            return result;
        }
    }

}
