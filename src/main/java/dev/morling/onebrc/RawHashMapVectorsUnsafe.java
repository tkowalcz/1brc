package dev.morling.onebrc;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.Vector;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/*
 *   HashMap entry layout.
 *   - Each column is a BYTE (not bit).
 *   - AVX-2 vector is 32 bytes.
 *   - There are 8 int-s in a vector.
 *
 *    0                   1                   2                   3
 *    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   |                         City name                             |
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   |Samples| Temp  |Sample0|Sample1|Sample2|Sample3|Sample4|Sample5|
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   |Sample6|Sample7|        Min vector containing 8 samples        |
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   |               |        Max vector containing 8 samples        |
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   |               |        Sum vector containing 8 samples        |
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *   |               |
 *   +-+-+-+-+-+-+-+-+
 *
 */
public class RawHashMapVectorsUnsafe {

    private static final int SAMPLES_COUNT_OFFSET = 32;
    private static final int TEMP_SAMPLES_COUNT_OFFSET = SAMPLES_COUNT_OFFSET + 4;

    private static final int TEMP_SAMPLES_OFFSET = TEMP_SAMPLES_COUNT_OFFSET + 4;
    private static final int MIN_VECTOR_OFFSET = TEMP_SAMPLES_OFFSET + 32;
    private static final int MAX_VECTOR_OFFSET = MIN_VECTOR_OFFSET + 32;
    private static final int SUM_VECTOR_OFFSET = MAX_VECTOR_OFFSET + 32;

    final MemorySegment hashMapData;
    final long hashMapDataUnsafe;

    public RawHashMapVectorsUnsafe(Arena arena) {
        int tableSize = CalculateAverage_tkowalcz2Unsafe.TABLE_SIZE * (32 // City name
                + 8 // metadata
                + 32 // Samples
                + 32 // Min vector
                + 32 // Max vector
                + 32 // Sum vector
        );

        hashMapDataUnsafe = UnsafeAccess.UNSAFE.allocateMemory(tableSize);
        hashMapData = MemorySegment.ofAddress(hashMapDataUnsafe).reinterpret(tableSize);
    }

    private void installNewCity(int mapEntryOffset, int delimiterPosition, Vector<Byte> hashInput) {
        hashInput.intoMemorySegment(hashMapData, mapEntryOffset, ByteOrder.nativeOrder());
    }

    //        public void addMeasurement(int mapEntryOffset, Vector<Byte> value) {
    public void addMeasurement(int mapEntryOffset, int value) {
        long baseOffset = hashMapDataUnsafe + mapEntryOffset;
        int tempSamplesCount = UnsafeAccess.UNSAFE.getInt(null, baseOffset + TEMP_SAMPLES_COUNT_OFFSET);

        long tempSampleOffset = baseOffset + TEMP_SAMPLES_OFFSET + (tempSamplesCount << 2);
        UnsafeAccess.UNSAFE.putInt(null, tempSampleOffset, value);
        tempSamplesCount++;

        if (tempSamplesCount == 8) {
            tempSamplesCount = 0;
            int samplesCount = UnsafeAccess.UNSAFE.getInt(null, baseOffset + TEMP_SAMPLES_COUNT_OFFSET);
            UnsafeAccess.UNSAFE.putInt(null, baseOffset + SAMPLES_COUNT_OFFSET, samplesCount + 8);
            aggregateMinMaxSum(mapEntryOffset);
        }

        UnsafeAccess.UNSAFE.putInt(null, baseOffset + TEMP_SAMPLES_COUNT_OFFSET, tempSamplesCount);
    }

    private void aggregateMinMaxSum(int entryOffset) {
        IntVector minVector = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + MIN_VECTOR_OFFSET, ByteOrder.nativeOrder());
        IntVector maxVector = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + MAX_VECTOR_OFFSET, ByteOrder.nativeOrder());
        IntVector sumVector = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + SUM_VECTOR_OFFSET, ByteOrder.nativeOrder());

        IntVector intVector1 = IntVector.fromMemorySegment(IntVector.SPECIES_256, hashMapData, entryOffset + TEMP_SAMPLES_OFFSET, ByteOrder.nativeOrder());

        minVector = minVector.min(intVector1);
        maxVector = maxVector.max(intVector1);
        sumVector = sumVector.add(intVector1);

        minVector.intoMemorySegment(hashMapData, entryOffset + MIN_VECTOR_OFFSET, ByteOrder.nativeOrder());
        maxVector.intoMemorySegment(hashMapData, entryOffset + MAX_VECTOR_OFFSET, ByteOrder.nativeOrder());
        sumVector.intoMemorySegment(hashMapData, entryOffset + SUM_VECTOR_OFFSET, ByteOrder.nativeOrder());
    }
}
