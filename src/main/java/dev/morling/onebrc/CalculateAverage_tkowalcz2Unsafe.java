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
import java.util.TreeMap;
import java.util.concurrent.*;

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

    public static final long ATOI_MASK = 0b01111111_01111111_01111111_01111111_01111111_01111111_01111111_01111111L;
    public static final long ATOI_NOT_MASK = 0b10000000_10000000_10000000_10000000_10000000_10000000_10000000_10000000L;
    public static final long ATOI_PREDICATE = 0x2E_2E_2E_2E_2E_2E_2E_2EL;
    private static final String FILE = "measurements.txt";

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;
    private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);

    private static final Vector<Byte> DELIMITER_VECTOR = SPECIES.broadcast(';');

    // Used to identify positions where vector containing temperature measurement has '-', '.' and '\n' characters.
    private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');
    private static final Vector<Byte> ASCII_DOT = SPECIES.broadcast('.');

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
    public static final int TABLE_SIZE = 0x80000;

    // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
    public static final int TABLE_SIZE_MASK = TABLE_SIZE - 1;

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Arena arena = Arena.ofShared();
        long start = System.nanoTime();
        int availableProcessors = 8;// Runtime.getRuntime().availableProcessors();

        MemorySegment inputData = mmapDataFile(FILE, arena);
        List<MemorySegment> memorySegments = divideAlongNewlines(inputData, availableProcessors);
        WorkerThread[] workerThreads = new WorkerThread[availableProcessors];
        for (int i = 0; i < workerThreads.length; i++) {
            workerThreads[i] = new WorkerThread(memorySegments.removeFirst(), arena, start);
            workerThreads[i].start();
        }

        TreeMap<String, StatisticsAggregate> results = new TreeMap<>();
        for (WorkerThread workerThread : workerThreads) {
            workerThread.join();
            List<StatisticsAggregate> statistics = workerThread.getStatistics();
            statistics.forEach(aggregate -> results.merge(aggregate.getCityName(), aggregate, StatisticsAggregate::merge));
        }

        System.out.println(results);
        System.exit(0);
        long runtime = System.nanoTime() - start;
        // System.out.println(STR."Runtime: \{TimeUnit.NANOSECONDS.toMillis(runtime)}ms");
    }

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

    public static class WorkerThread extends Thread {

        private final MemorySegment memorySegment;
        private final long startTimestamp;

        private final UnsafeRawHashMap hashMap;
        private List<StatisticsAggregate> statistics;

        public WorkerThread(MemorySegment memorySegment, Arena arena, long startTimestamp) {
            this.memorySegment = memorySegment;
            this.startTimestamp = startTimestamp;
            this.hashMap = new UnsafeRawHashMap(arena);

            setDaemon(true);
        }

        @Override
        public void run() {
            System.out.println(STR."Start lag: \{TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimestamp)}ms");

            execute(Arena.ofConfined(), memorySegment);

            statistics = hashMap.asStatistics();
        }

        public List<StatisticsAggregate> execute(Arena arena, MemorySegment inputData) {
            long start = System.currentTimeMillis();

            long stride = inputData.byteSize() / 3;

            long offset1 = 0;
            long end1 = stride - ByteVector.SPECIES_256.vectorByteSize();

            long offset2 = CalculateAverage_tkowalcz.findPastNewline(inputData, end1);
            long end2 = stride + stride - ByteVector.SPECIES_256.vectorByteSize();

            long offset3 = CalculateAverage_tkowalcz.findPastNewline(inputData, end2);
            long end3 = stride + stride + stride - ByteVector.SPECIES_256.vectorByteSize();
            //
            // long offset4 = CalculateAverage_tkowalcz.findPastNewline(inputData, end3);
            // long end4 = stride + stride + stride + stride - ByteVector.SPECIES_256.vectorByteSize();

            Cursor doubleCursor = executeTriplePumped(inputData, hashMap, offset1, end1, offset2, end2, offset3, end3 /* , offset4, end4 */);
            // offset1 = executeSinglePumped(inputData, dataTable, doubleCursor.offset1(), end1);
            // offset2 = executeSinglePumped(inputData, dataTable, doubleCursor.offset2(), end2);
            // Map<String, StatisticsAggregate> tailResults1 = executeScalar(inputData, offset1, end1);
            // Map<String, StatisticsAggregate> tailResults2 = executeScalar(inputData, offset2, end2);

            List<StatisticsAggregate> result = hashMap.asStatistics();
            // result.addAll(tailResults1.values());
            // result.addAll(tailResults2.values());

            // long end = System.currentTimeMillis();
            // System.out.println(STR."Worker \{Thread.currentThread().getName()}finished in:\{end - start} ms ");
            return result;
        }

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
        // return Arrays.stream(lines)
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

        public static final VectorShuffle<Byte>[] SHUFFLE_1 = new VectorShuffle[]{
                null,
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 1, 2, 4, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31), // - 1 2 . 3
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 0, 2, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31), // 2 . 3
                null,
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 0, 1, 3, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31), // 1 2 . 3
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 1, 3, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31), // - 2 . 3
        };

        public static final VectorShuffle<Byte>[] SHUFFLE_2 = new VectorShuffle[]{
                null,
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 31, 31, 31, 9, 10, 12, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31, 31), // - 1 2 . 3
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 31, 31, 31, 9, 8, 10, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31), // 2 . 3
                null,
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 31, 31, 31, 8, 9, 11, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31), // 1 2 . 3
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 31, 31, 31, 31, 9, 11, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31, 31), // - 2 . 3
        };

        public static final VectorShuffle<Byte>[] SHUFFLE_3 = new VectorShuffle[]{
                null,
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 31, 31, 31, 31, 31, 31, 31, 17, 18, 20, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31, 31), // - 1 2 . 3
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 16, 18, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31, 31), // 2 . 3
                null,
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 31, 31, 31, 31, 31, 31, 31, 16, 17, 19, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31, 31), // 1 2 . 3
                VectorShuffle.fromValues(ByteVector.SPECIES_256, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 17, 19, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31,
                        31, 31, 31, 31, 31, 31), // - 2 . 3
        };

        public static final VectorShuffle<Short> ATOI_SHUFFLE_1 = VectorShuffle.fromValues(ShortVector.SPECIES_256, 0, 3, 0, 0, 0, 7, 0, 0, 0, 11, 0, 0, 0, 15, 0, 0);
        public static final VectorShuffle<Short> ATOI_SHUFFLE_2 = VectorShuffle.fromValues(ShortVector.SPECIES_256, 0, 2, 0, 0, 0, 6, 0, 0, 0, 10, 0, 0, 0, 14, 0, 0);

        public static Cursor executeTriplePumped(
                                                 MemorySegment inputData,
                                                 UnsafeRawHashMap hashMap,
                                                 long offset1,
                                                 long end1,
                                                 long offset2,
                                                 long end2,
                                                 long offset3,
                                                 long end3
        /*
         * ,
         * long offset4,
         * long end4
         */) {
            while (offset1 < end1 && offset2 < end2 && offset3 < end3/* && offset4 < end4 */) {
                Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
                // System.out.println(toString(byteVector1));
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

                /*
                 * Vector<Byte> byteVector4 = SPECIES.fromMemorySegment(inputData, offset4, ByteOrder.nativeOrder());
                 * int firstDelimiter4 = byteVector4.compare(VectorOperators.EQ, DELIMITER_VECTOR).firstTrue();
                 * if (firstDelimiter4 == 32) {
                 * offset4 = findPastNewline(inputData, offset4);
                 * continue;
                 * }
                 * offset4 += firstDelimiter4 + 1;
                 *
                 */
                VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[firstDelimiter1];
                Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);
                VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[firstDelimiter2];
                Vector<Byte> hashInput2 = ZERO.blend(byteVector2, hashMask2);
                VectorMask<Byte> hashMask3 = CITY_LOOKUP_MASK[firstDelimiter3];
                Vector<Byte> hashInput3 = ZERO.blend(byteVector3, hashMask3);
                // VectorMask<Byte> hashMask4 = CITY_LOOKUP_MASK[firstDelimiter4];
                // Vector<Byte> hashInput4 = ZERO.blend(byteVector4, hashMask4);

                int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int index1 = perfectHash32_1 & TABLE_SIZE_MASK;
                int cityNameOffset1 = (index1 << 5) + (index1 << 4);

                ByteVector cityVector1 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityNameOffset1, ByteOrder.nativeOrder());
                if (!cityVector1.compare(VectorOperators.EQ, hashInput1).allTrue()) {
                    cityNameOffset1 = hashMiss(hashMap, cityNameOffset1, index1, hashInput1, firstDelimiter2);
                }

                // long v1 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset1);
                // long resultMask1 = (((ATOI_PREDICATE ^ v1) ^ ATOI_MASK) + 0x01_01_01_01_01_01_01_01L) & ATOI_NOT_MASK;
                // int p1 = Long.numberOfTrailingZeros(resultMask1) / 8;
                // long n1 = convertIntoNumber(p1, v1);

                int perfectHash32_2 = hashInput2.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int index2 = perfectHash32_2 & TABLE_SIZE_MASK;
                int cityNameOffset2 = (index2 << 5) + (index2 << 4);

                ByteVector cityVector2 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityNameOffset2, ByteOrder.nativeOrder());
                if (!cityVector2.compare(VectorOperators.EQ, hashInput2).allTrue()) {
                    cityNameOffset2 = hashMiss(hashMap, cityNameOffset2, index2, hashInput2, firstDelimiter2);
                }

                // long v2 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset2);
                // long resultMask2 = (((ATOI_PREDICATE ^ v2) ^ ATOI_MASK) + 0x01_01_01_01_01_01_01_01L) & ATOI_NOT_MASK;
                // int p2 = Long.numberOfTrailingZeros(resultMask2) / 8;
                // long n2 = convertIntoNumber(p2, v2);

                int perfectHash32_3 = hashInput3.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                int index3 = perfectHash32_3 & TABLE_SIZE_MASK;
                int cityNameOffset3 = (index3 << 5) + (index3 << 4);

                ByteVector cityVector3 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityNameOffset3, ByteOrder.nativeOrder());
                if (!cityVector3.compare(VectorOperators.EQ, hashInput3).allTrue()) {
                    cityNameOffset3 = hashMiss(hashMap, cityNameOffset3, index3, hashInput3, firstDelimiter3);
                }
                //
                // long v3 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset3);
                // long resultMask3 = (((ATOI_PREDICATE ^ v3) ^ ATOI_MASK) + 0x01_01_01_01_01_01_01_01L) & ATOI_NOT_MASK;
                // int p3 = Long.numberOfTrailingZeros(resultMask3) / 8;
                // long n3 = convertIntoNumber(p3, v3);
                //
                // int perfectHash32_4 = hashInput4.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
                // int index4 = perfectHash32_4 & TABLE_SIZE_MASK;
                // int cityNameOffset4 = (index4 << 5) + (index4 << 4);
                //
                // ByteVector cityVector4 = ByteVector.fromMemorySegment(SPECIES, hashMap.hashMapData, cityNameOffset4, ByteOrder.nativeOrder());
                // if (!cityVector4.compare(VectorOperators.EQ, hashInput4).allTrue()) {
                // cityNameOffset4 = hashMiss(hashMap, cityNameOffset4, index4, hashInput4, firstDelimiter4);
                // }
                //
                // long v4 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset4);
                // long resultMask4 = (((ATOI_PREDICATE ^ v4) ^ ATOI_MASK) + 0x01_01_01_01_01_01_01_01L) & ATOI_NOT_MASK;
                // int p4 = Long.numberOfTrailingZeros(resultMask4) / 8;
                // long n4 = convertIntoNumber(p4, v4);

                // long v1 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset1);
                // long resultMask1 = (((ATOI_PREDICATE ^ v1) ^ ATOI_MASK) + 0x01_01_01_01_01_01_01_01L) & ATOI_NOT_MASK;
                // int p1 = Long.numberOfTrailingZeros(resultMask1) / 8;
                // long n1 = convertIntoNumber(p1, v1);
                // hashMap.addMeasurement(cityNameOffset1, (int) p1);
                // offset1 += p1 + 3;

                // long v2 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset2);
                // long resultMask2 = (((ATOI_PREDICATE ^ v2) ^ ATOI_MASK) + 0x01_01_01_01_01_01_01_01L) & ATOI_NOT_MASK;
                // int p2 = Long.numberOfTrailingZeros(resultMask2) / 8;
                // long n2 = convertIntoNumber(p2, v2);
                // hashMap.addMeasurement(cityNameOffset2, (int) p2);
                // offset2 += p2 + 3;

                // long v3 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset3);
                // long resultMask3 = (((ATOI_PREDICATE ^ v3) ^ ATOI_MASK) + 0x01_01_01_01_01_01_01_01L) & ATOI_NOT_MASK;
                // int p3 = Long.numberOfTrailingZeros(resultMask3) / 8;
                // long n3 = convertIntoNumber(p3, v3);
                // hashMap.addMeasurement(cityNameOffset3, (int) p3);
                // offset3 += p3 + 3;

                // long v4 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset4);
                // long resultMask4 = (((ATOI_PREDICATE ^ v4) ^ ATOI_MASK) + 0x01_01_01_01_01_01_01_01L) & ATOI_NOT_MASK;
                // int p4 = Long.numberOfTrailingZeros(resultMask4) / 8;
                // long n4 = convertIntoNumber(p4, v4);
                // hashMap.addMeasurement(cityNameOffset4, (int) p4);
                // offset4 += p4 + 3;

                // Vector<Short> longVector = LongVector.fromArray(LongVector.SPECIES_256, new long[]{v1, v2, v3, 0}, 0)
                // .reinterpretAsBytes()
                // .castShape(ShortVector.SPECIES_256, 0)
                // longVector.sub(ASCII_ZERO).mul();
                // VectorMask<Byte> longVectorMask = longVector.compare(VectorOperators.LT, ASCII_ZERO);
                //
                //

                long long1 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset1);
                long long2 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset2);
                long long3 = inputData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset3);

                ByteVector byteVector = LongVector.fromArray(LongVector.SPECIES_256, new long[]{ long1, long2, long3, 0x30_30_30_30_30_30_30_30L }, 0)
                        .reinterpretAsBytes();
                long allData = byteVector.compare(VectorOperators.LT, ASCII_ZERO).toLong();

                int lookupIndex1 = (int) (allData & 0x7);
                int lookupIndex2 = (int) ((allData >> 8) & 0x7);
                int lookupIndex3 = (int) ((allData >> 16) & 0x7);

                Vector<Byte> sub = byteVector.sub((byte) '0');
                Vector<Short> aaaa = sub.rearrange(SHUFFLE_1[lookupIndex1])
                        .add(sub.rearrange(SHUFFLE_2[lookupIndex2]))
                        .add(sub.rearrange(SHUFFLE_3[lookupIndex3]))
                        .castShape(ShortVector.SPECIES_256, 0)
                        .mul(ATOI_MUL);

                Vector<Short> s1 = aaaa.rearrange(ATOI_SHUFFLE_1);
                Vector<Short> s2 = aaaa.rearrange(ATOI_SHUFFLE_2);
                ShortVector result = (ShortVector) aaaa.add(s1).add(s2);

                hashMap.addMeasurement(cityNameOffset1, result.lane(0));
                hashMap.addMeasurement(cityNameOffset2, result.lane(1));
                hashMap.addMeasurement(cityNameOffset3, result.lane(2));

                offset1 += STOI_SIZE_LOOKUP[lookupIndex1];
                offset2 += STOI_SIZE_LOOKUP[lookupIndex2];
                offset3 += STOI_SIZE_LOOKUP[lookupIndex3];

                // byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
                // VectorMask<Byte> mask1 = byteVector1.compare(VectorOperators.LT, ASCII_ZERO);
                // int lookupIndex1 = (int) (mask1.toLong() & 0x07);
                // ShortVector mul1 = (ShortVector) byteVector1
                // .sub(ASCII_ZERO)
                // .castShape(ShortVector.SPECIES_256, 0)
                // .mul(STOI_MUL_LOOKUP[lookupIndex1]);
                // hashMap.addMeasurement(cityNameOffset1, mul1.lane(0) + mul1.lane(1) + mul1.lane(2) + mul1.lane(3) + mul1.lane(4));
                // offset1 += STOI_SIZE_LOOKUP[lookupIndex1];
                //
                // byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
                // VectorMask<Byte> mask2 = byteVector2.compare(VectorOperators.LT, ASCII_ZERO);
                // int lookupIndex2 = (int) (mask2.toLong() & 0x07);
                // ShortVector mul2 = (ShortVector) byteVector2
                // .sub(ASCII_ZERO)
                // .castShape(ShortVector.SPECIES_256, 0)
                // .mul(STOI_MUL_LOOKUP[lookupIndex2]);
                // hashMap.addMeasurement(cityNameOffset2, mul2.lane(0) + mul2.lane(1) + mul2.lane(2) + mul2.lane(3) + mul2.lane(4));
                // offset2 += STOI_SIZE_LOOKUP[lookupIndex2];
                //
                // byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
                // VectorMask<Byte> mask3 = byteVector3.compare(VectorOperators.LT, ASCII_ZERO);
                // int lookupIndex3 = (int) (mask3.toLong() & 0x07);
                // ShortVector mul3 = (ShortVector) byteVector3
                // .sub(ASCII_ZERO)
                // .castShape(ShortVector.SPECIES_256, 0)
                // .mul(STOI_MUL_LOOKUP[lookupIndex3]);
                // hashMap.addMeasurement(cityNameOffset3, mul3.lane(0) + mul3.lane(1) + mul3.lane(2) + mul3.lane(3) + mul3.lane(4));
                // offset3 += STOI_SIZE_LOOKUP[lookupIndex3];
            }

            return new Cursor(offset1, offset2, offset3);
        }

        public static final ShortVector ATOI_MUL = ShortVector.fromArray(
                ShortVector.SPECIES_256,
                new short[]{ 0, 100, 10, 1, 0, 100, 10, 1, 0, 100, 10, 1, 0, 0, 0, 0 },
                0);

        private static int hashMiss(UnsafeRawHashMap hashMap, int cityNameOffset, int index, Vector<Byte> hashInput, int delimiterPosition) {
            boolean isZero = hashMap.hashMapData.get(ValueLayout.JAVA_BYTE, cityNameOffset) == 0;
            if (isZero) {
                hashMap.installNewCity(cityNameOffset, delimiterPosition, hashInput);
            }
            else {
                System.out.println(toString(hashInput));
                System.out.println("conflict");
                // index = (index + 1) & TABLE_SIZE_MASK;
                //
            }
            //
            return cityNameOffset;
        }

        public static String toString(Vector<Byte> data) {
            byte[] array = data.reinterpretAsBytes().toArray();
            return new String(array, StandardCharsets.UTF_8).replace('\n', ' ');
        }

        public List<StatisticsAggregate> getStatistics() {
            return statistics;
        }

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

    private static MemorySegment mmapDataFile(String fileName, Arena arena, long offset, long size) {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
                FileChannel channel = file.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, offset, size, arena);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static MemorySegment mmapDataFile(String fileName, Arena arena) {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r");
                FileChannel channel = file.getChannel()) {
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
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

    public static class StatisticsAggregate {

        private final String cityName;
        private int min;
        private int max;

        private int sum;
        private int count;

        public StatisticsAggregate(String cityName, int min, int max, int sum, int count) {
            this.cityName = cityName;
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public static StatisticsAggregate merge(StatisticsAggregate one, StatisticsAggregate other) {
            int min = Math.min(one.min, other.min);
            int max = Math.max(one.max, other.max);

            int sum = one.sum + other.sum;
            int count = one.count + other.count;

            return new StatisticsAggregate(one.cityName, min, max, sum, count);
        }

        public String getCityName() {
            return cityName;
        }

        @Override
        public String toString() {
            float average = (sum / 10.0f) / count;
            float actualMin = min / 10.0f;
            float actualMax = max / 10.0f;

            return String.format("%.1f/%.1f/%.1f", actualMin, average, actualMax);
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
     * | Min | Max | Count | Sum |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *
     */
    public static class UnsafeRawHashMap {

        private static final int CITY_NAME_SIZE_OFFSET = 44;

        private static final int MIN_OFFSET = 32;
        private static final int MAX_OFFSET = 36;
        private static final int COUNT_OFFSET = 40;
        private static final int SUM_OFFSET = 44;

        public final MemorySegment hashMapData;
        // final long hashMapDataUnsafe;

        final ArrayList<Integer> cityIndex = new ArrayList<>(500);

        public UnsafeRawHashMap(Arena arena) {
            int tableSize = CalculateAverage_tkowalcz2Unsafe.TABLE_SIZE * (32 /* City name */ + 16);

            hashMapData = arena.allocate(tableSize);
            // hashMapDataUnsafe = UnsafeAccess.UNSAFE.allocateMemory(tableSize);
            // hashMapData = MemorySegment.ofAddress(hashMapDataUnsafe).reinterpret(tableSize);
        }

        public void installNewCity(int mapEntryOffset, int delimiterPosition, Vector<Byte> hashInput) {
            hashInput.intoMemorySegment(hashMapData, mapEntryOffset, ByteOrder.nativeOrder());

            // long baseOffset = hashMapDataUnsafe + mapEntryOffset;
            // UnsafeAccess.UNSAFE.putInt(baseOffset + CITY_NAME_SIZE_OFFSET, delimiterPosition);
            // UnsafeAccess.UNSAFE.putInt(baseOffset + MIN_OFFSET, Integer.MAX_VALUE);
            // UnsafeAccess.UNSAFE.putInt(baseOffset + MAX_OFFSET, Integer.MIN_VALUE);

            hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + MIN_OFFSET, Integer.MAX_VALUE);
            hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + MAX_OFFSET, Integer.MIN_VALUE);
            cityIndex.add(mapEntryOffset);
        }

        public void addMeasurement(int mapEntryOffset, int value) {
            int min = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + MIN_OFFSET);
            int max = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + MAX_OFFSET);
            long countSum = hashMapData.get(ValueLayout.JAVA_LONG, mapEntryOffset + COUNT_OFFSET);

            // long baseOffset = hashMapDataUnsafe + mapEntryOffset;
            // int min = UnsafeAccess.UNSAFE.getInt(baseOffset + MIN_OFFSET);
            // int max = UnsafeAccess.UNSAFE.getInt(baseOffset + MAX_OFFSET);
            // long countSum = UnsafeAccess.UNSAFE.getLong(baseOffset + COUNT_OFFSET);
            countSum += value;
            countSum += 0x00000001_00000000L;

            if (value < min) {
                hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + MIN_OFFSET, value);
                // UnsafeAccess.UNSAFE.putInt(baseOffset + MIN_OFFSET, Math.min(min, value));
            }
            if (value > max) {
                hashMapData.set(ValueLayout.JAVA_INT, mapEntryOffset + MAX_OFFSET, value);
                // UnsafeAccess.UNSAFE.putInt(baseOffset + MAX_OFFSET, Math.max(max, value));
            }

            hashMapData.set(ValueLayout.JAVA_LONG, mapEntryOffset + COUNT_OFFSET, countSum);
            // UnsafeAccess.UNSAFE.putLong(baseOffset + COUNT_OFFSET, countSum);
        }

        public List<StatisticsAggregate> asStatistics() {
            List<StatisticsAggregate> result = new ArrayList<>(cityIndex.size());
            for (int i = 0; i < cityIndex.size(); i++) {
                result.add(toStatistic(cityIndex.get(i)));
            }

            return result;
        }

        public StatisticsAggregate toStatistic(int mapEntryOffset) {
            // long baseOffset = hashMapDataUnsafe + mapEntryOffset;
            // int min = UnsafeAccess.UNSAFE.getInt(baseOffset + MIN_OFFSET);
            // int max = UnsafeAccess.UNSAFE.getInt(baseOffset + MAX_OFFSET);
            // int sum = UnsafeAccess.UNSAFE.getInt(baseOffset + SUM_OFFSET);
            // int count = UnsafeAccess.UNSAFE.getInt(baseOffset + COUNT_OFFSET);
            //
            int min = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + MIN_OFFSET);
            int max = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + MAX_OFFSET);
            int sum = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + SUM_OFFSET);
            int count = hashMapData.get(ValueLayout.JAVA_INT, mapEntryOffset + COUNT_OFFSET);
            //
            String cityName = hashMapData.getUtf8String(mapEntryOffset);
            return new StatisticsAggregate(cityName, min, max, sum, count);
        }
    }
}
