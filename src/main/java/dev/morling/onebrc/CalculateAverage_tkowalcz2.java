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

import jdk.incubator.vector.Vector;
import jdk.incubator.vector.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static java.util.Arrays.stream;

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
public class CalculateAverage_tkowalcz2 {

    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');

    // Used to identify positions where vector containing temperature measurement has '-', '.' and '\n' characters.
    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');

    static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();

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

    // We also need to know the size of temperature measurement in characters, lookup table works the same way as STOI_MUL_LOOKUP.
    private static final int[] STOI_SIZE_LOOKUP = { 0, 6, 4, 0, 5, 5 };

    // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
    // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
    public static final int TABLE_SIZE = 0x400000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = TABLE_SIZE - 1;

    // public static final int TABLE_ENTRY_SIZE = 228;
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

    public static DoubleCursor executeDoublePumped(
                                                   MemorySegment inputData,
                                                   RawHashMap2 hashMap,
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
            // int cityNameOffset1 = (index1 << 8) + (index1 << 5); // 2715559 - 130346832
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

            VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex1 = (int) ((mask1.toLong() >> (firstDelimiter1 + 1)) & 0x07);
            long value = byteVector1
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex1])
                    .reduceLanesToLong(VectorOperators.ADD);

            hashMap.addMeasurement(cityNameOffset1, (int) value);
            offset1 += STOI_SIZE_LOOKUP[lookupIndex1];

            VectorMask<Byte> mask2 = byteVector2.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex2 = (int) ((mask2.toLong() >> (firstDelimiter2 + 1)) & 0x07);
            value = byteVector2
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex2])
                    .reduceLanesToLong(VectorOperators.ADD);
            hashMap.addMeasurement(cityNameOffset2, (int) value);
            offset2 += STOI_SIZE_LOOKUP[lookupIndex2];

            VectorMask<Byte> mask3 = byteVector3.compare(VectorOperators.LT, ASCII_ZERO);
            int lookupIndex3 = (int) ((mask3.toLong() >> (firstDelimiter3 + 1)) & 0x07);
            value = byteVector3
                    .sub(ASCII_ZERO)
                    .castShape(ShortVector.SPECIES_256, 0)
                    .mul(STOI_MUL_LOOKUP[lookupIndex3])
                    .reduceLanesToLong(VectorOperators.ADD);
            hashMap.addMeasurement(cityNameOffset3, (int) value);
            offset3 += STOI_SIZE_LOOKUP[lookupIndex3];
        }

        return new DoubleCursor(offset1, offset2);
    }

    private static int hashMiss(RawHashMap2 hashMap, int cityNameOffset, int index, Vector<Byte> hashInput, int delimiterPosition) {
        // if (cityNameOffset == 0) {
        hashMap.installNewCity(cityNameOffset, delimiterPosition, hashInput);
        // } else {
        // index = (index + 1) & TABLE_SIZE_MASK;
        //
        // }
        //
        return cityNameOffset;
    }

    public record DoubleCursor(long offset1, long offset2) {
    }

    // public static long executeSinglePumped(MemorySegment inputData, StatisticsAggregate[] dataTable, long offset1, long end1) {
    // end1 -= 2 * SPECIES.length();
    // while (offset1 < end1) {
    // Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
    // int firstDelimiter1 = byteVector1.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
    // offset1 += firstDelimiter1 + 1;
    //
    // if (firstDelimiter1 == 32) {
    // // Slow path - need to reimplement all logic in a scalar way, but it does not have to exact -
    // // this string will always hit this path
    // // System.out.println("Unsupported city name exceeding \{SPECIES.length()} bytes. Starts with \{toString(byteVector1)}");
    // }
    //
    // VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[firstDelimiter1];
    // Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);
    //
    // int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
    // int index1 = perfectHash32_1 & TABLE_SIZE_MASK;
    //
    // StatisticsAggregate statisticsAggregate_1 = dataTable[index1];
    // if (statisticsAggregate_1 == null) {
    // byte[] city = new byte[SPECIES.length()];
    // hashInput1.reinterpretAsBytes().intoArray(city, 0, hashMask1);
    //
    // statisticsAggregate_1 = new StatisticsAggregate(city, hashMask1.trueCount());
    // dataTable[index1] = statisticsAggregate_1;
    // } else {
    // ByteVector cityVector = ByteVector.fromArray(ByteVector.SPECIES_256, statisticsAggregate_1.getCity(), 0);
    // if (!cityVector.compare(VectorOperators.EQ, hashInput1).allTrue()) {
    // // Very slow path: linked list of collisions
    // // statisticsAggregate_1 = findCityInChain(statisticsAggregate_1, hashInput1, hashMask1);
    // }
    // }
    //
    // // byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
    // VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
    // int lookupIndex1 = (int) ((mask1.toLong() >> (firstDelimiter1 + 1)) & 0x07);
    //
    // long value = byteVector1
    // .sub(ASCII_ZERO)
    // .castShape(ShortVector.SPECIES_256, 0)
    // // .mul(STOI_MUL_LOOKUP[lookupIndex1])
    // .reduceLanesToLong(VectorOperators.ADD);
    //
    // statisticsAggregate_1.accept(value);
    // offset1 += STOI_SIZE_LOOKUP[lookupIndex1];
    // }
    //
    // return offset1;
    // }

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

    /*
     * HashMap entry layout.
     * - Each column is a BYTE (not bit).
     * - AVX-2 vector is 32 bytes.
     * - There are 8 int-s in a vector.
     *
     * 0 1 2 3
     * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | City name |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Size |Samples| Count | |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |Sample0|Sample1|Sample2|Sample3|Sample4|Sample5|Sample6|Sample7|
     * | Samples 08-15 |
     * | Samples 16-23 |
     * | Samples 24-31 |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Min vector containing 8 samples |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Max vector containing 8 samples |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Sum vector containing 8 samples |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     */
    public static class RawHashMap {

        private static final int CITY_NAME_SIZE_OFFSET = 32;
        private static final int SAMPLES_COUNT_OFFSET = 32 + 4;
        private static final int TEMP_SAMPLES_COUNT_OFFSET = SAMPLES_COUNT_OFFSET + 4;

        private static final int TEMP_SAMPLES_OFFSET = 64;
        private static final int MIN_VECTOR_OFFSET = TEMP_SAMPLES_OFFSET + 32 * 4;
        private static final int MAX_VECTOR_OFFSET = MIN_VECTOR_OFFSET + 32;
        private static final int SUM_VECTOR_OFFSET = MAX_VECTOR_OFFSET + 32;

        // final int[] hashMap;
        final MemorySegment hashMapData;

        public RawHashMap(Arena arena) {
            hashMapData = arena.allocate(TABLE_SIZE * (32 // City name
                    + 32 // metadata
                    + 4 * 32 // Samples
                    + 32 // Min vector
                    + 32 // Max vector
                    + 32 // Sum vector
            ));
        }

        private void installNewCity(int mapEntryOffset, int delimiterPosition, Vector<Byte> hashInput) {
            hashInput.intoMemorySegment(hashMapData, mapEntryOffset, ByteOrder.nativeOrder());
            hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + CITY_NAME_SIZE_OFFSET, delimiterPosition);
        }

        // public void addMeasurement(int mapEntryOffset, Vector<Byte> value) {
        public void addMeasurement(int mapEntryOffset, int value) {
            int tempSamplesCount = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + TEMP_SAMPLES_COUNT_OFFSET);

            int tempSampleOffset = mapEntryOffset + TEMP_SAMPLES_OFFSET + (tempSamplesCount << 2);
            hashMapData.set(ValueLayout.JAVA_INT, tempSampleOffset, value);
            tempSamplesCount++;

            if (tempSamplesCount == 32) {
                tempSamplesCount = 0;
                int samplesCount = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + SAMPLES_COUNT_OFFSET);
                hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + SAMPLES_COUNT_OFFSET, samplesCount + 32);
                aggregateMinMaxSum(mapEntryOffset);
            }

            hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + TEMP_SAMPLES_COUNT_OFFSET, tempSamplesCount);
        }

        private void aggregateMinMaxSum(int entryOffset) {
            IntVector minVector = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + MIN_VECTOR_OFFSET, ByteOrder.nativeOrder());
            IntVector maxVector = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + MAX_VECTOR_OFFSET, ByteOrder.nativeOrder());
            IntVector sumVector = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + SUM_VECTOR_OFFSET, ByteOrder.nativeOrder());

            IntVector intVector1 = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + TEMP_SAMPLES_OFFSET, ByteOrder.nativeOrder());
            IntVector intVector2 = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + TEMP_SAMPLES_OFFSET + 32, ByteOrder.nativeOrder());
            IntVector intVector3 = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + TEMP_SAMPLES_OFFSET + 64, ByteOrder.nativeOrder());
            IntVector intVector4 = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + TEMP_SAMPLES_OFFSET + 96, ByteOrder.nativeOrder());

            minVector = minVector.min(intVector1);
            minVector = minVector.min(intVector2);
            minVector = minVector.min(intVector3);
            minVector = minVector.min(intVector4);

            maxVector = maxVector.max(intVector1);
            maxVector = maxVector.max(intVector2);
            maxVector = maxVector.max(intVector3);
            maxVector = maxVector.max(intVector4);

            sumVector = sumVector.add(intVector1);
            sumVector = sumVector.add(intVector2);
            sumVector = sumVector.add(intVector3);
            sumVector = sumVector.add(intVector4);

            minVector.intoMemorySegment(hashMapData, entryOffset + MIN_VECTOR_OFFSET, ByteOrder.nativeOrder());
            maxVector.intoMemorySegment(hashMapData, entryOffset + MAX_VECTOR_OFFSET, ByteOrder.nativeOrder());
            sumVector.intoMemorySegment(hashMapData, entryOffset + SUM_VECTOR_OFFSET, ByteOrder.nativeOrder());
        }
    }

    /*
     * HashMap entry layout.
     * - Each column is a BYTE (not bit).
     * - AVX-2 vector is 32 bytes.
     * - There are 8 int-s in a vector.
     *
     * 0 1 2 3
     * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | City name |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Min | Max | Sum | Len | |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     */
    public static class RawHashMap2 {

        // private static final int CITY_NAME_SIZE_OFFSET = 44;

        private static final int MIN_OFFSET = 32;
        private static final int MAX_OFFSET = 36;
        private static final int SUM_OFFSET = 40;

        final MemorySegment hashMapData;

        public RawHashMap2(Arena arena) {
            hashMapData = arena.allocate(TABLE_SIZE * (32 // City name
                    + 16));
        }

        private void installNewCity(int mapEntryOffset, int delimiterPosition, Vector<Byte> hashInput) {
            hashInput.intoMemorySegment(hashMapData, mapEntryOffset, ByteOrder.nativeOrder());
            // hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + CITY_NAME_SIZE_OFFSET, delimiterPosition);
        }

        // public void addMeasurement(int mapEntryOffset, Vector<Byte> value) {
        public void addMeasurement(int mapEntryOffset, int value) {
            int min = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + MIN_OFFSET);
            int max = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + MAX_OFFSET);
            int sum = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + SUM_OFFSET);

            hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + MIN_OFFSET, Math.min(min, value));
            hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + MAX_OFFSET, Math.max(max, value));
            hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + SUM_OFFSET, sum + value);
        }
        //
        // public void clear() {
        // for (int i = 0; i < hashMapData.byteSize(); i++) {
        // hashMapData.set(ValueLayout.JAVA_BYTE, i, (byte) 0);
        // }
        // }
    }
}
