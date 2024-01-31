package dev.morling.onebrc;

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
 *   |  Min  |  Max  |  Sum  |  Len  |                               |
 *   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *
 */
public class RawHashMapUnsafe {

    private static final int CITY_NAME_SIZE_OFFSET = 44;

    private static final int MIN_OFFSET = 32;
    private static final int MAX_OFFSET = 36;
    private static final int SUM_OFFSET = 40;

    public final MemorySegment hashMapData;
    final long hashMapDataUnsafe;

    public RawHashMapUnsafe(Arena arena) {
        int tableSize = CalculateAverage_tkowalcz2Unsafe.TABLE_SIZE * (32 /* City name */ + 16);

        hashMapDataUnsafe = UnsafeAccess.UNSAFE.allocateMemory(tableSize);
        hashMapData = MemorySegment.ofAddress(hashMapDataUnsafe).reinterpret(tableSize);
    }

    public void installNewCity(int mapEntryOffset, int delimiterPosition, Vector<Byte> hashInput) {
        hashInput.intoMemorySegment(hashMapData, mapEntryOffset, ByteOrder.nativeOrder());
        UnsafeAccess.UNSAFE.putInt(null, hashMapDataUnsafe + mapEntryOffset + CITY_NAME_SIZE_OFFSET, delimiterPosition);
    }

    public void addMeasurement(int mapEntryOffset, int value) {
        long baseOffset = hashMapDataUnsafe + mapEntryOffset;
        int min = UnsafeAccess.UNSAFE.getInt(baseOffset + MIN_OFFSET);
        int max = UnsafeAccess.UNSAFE.getInt(baseOffset + MAX_OFFSET);
        int sum = UnsafeAccess.UNSAFE.getInt(baseOffset + SUM_OFFSET);

        UnsafeAccess.UNSAFE.putInt(baseOffset + MIN_OFFSET, Math.min(min, value));
        UnsafeAccess.UNSAFE.putInt(baseOffset + MAX_OFFSET, Math.max(max, value));
        UnsafeAccess.UNSAFE.putInt(baseOffset + SUM_OFFSET, sum + value);
    }
}
