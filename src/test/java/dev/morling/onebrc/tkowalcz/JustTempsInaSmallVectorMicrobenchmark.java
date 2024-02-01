// package dev.morling.onebrc.tkowalcz;
//
// import dev.morling.onebrc.CalculateAverage_tkowalcz;
// import dev.morling.onebrc.RawHashMapUnsafe;
// import jdk.incubator.vector.*;
// import org.openjdk.jmh.annotations.Benchmark;
// import org.openjdk.jmh.annotations.Level;
// import org.openjdk.jmh.annotations.Setup;
// import org.openjdk.jmh.runner.RunnerException;
//
// import java.io.IOException;
// import java.lang.foreign.Arena;
// import java.lang.foreign.MemorySegment;
// import java.lang.foreign.ValueLayout;
// import java.nio.ByteOrder;
// import java.util.Arrays;
//
// public class JustTempsInaSmallVectorMicrobenchmark extends OneBrcMicrobenchmark {
//
// private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
//
// private static final Vector<Byte> ASCII_ZERO = SPECIES.broadcast('0');
// private static final Vector<Byte> DELIMITER_MINUS_ASCII_ZERO_VECTOR = SPECIES.broadcast(';' - '0');
//
// private static final VectorShuffle<Byte>[] STOI_SHUFFLE_1 = new VectorShuffle[]{
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// };
//
// private static final VectorShuffle<Byte>[] STOI_SHUFFLE_2 = new VectorShuffle[]{
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// };
//
// private static final VectorShuffle<Byte>[] STOI_SHUFFLE_3 = new VectorShuffle[]{
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// };
//
// private static final VectorShuffle<Byte>[] STOI_SHUFFLE_4 = new VectorShuffle[]{
// VectorShuffle.fromValues(ByteVector.SPECIES_128, 2, 3, 4, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
// };
//
// // Values used to multiply digits of temperature measurement to get proper int. E.g. -12.3 will be multiplied by
// // 10th element (0, -100, -10, 0, -1) giving '-' * 0 + '1' * -100 + '2' * -10 + '.' * 0 + '3' * -1 = -123.
// // There are four combinations of possible mask results from comparing (less than) vector containing temperature
// // measurement with ASCII_ZERO. Hence, only four entries are populated.
// private static final ShortVector[] STOI_MUL_LOOKUP = {
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0),
// ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0)
// };
//
// static final VectorMask<Byte>[] CITY_LOOKUP_MASK = createMasks32();
//
// private static final Vector<Byte> ZERO = ByteVector.zero(SPECIES);
//
// public static VectorMask<Byte>[] createMasks32() {
// VectorMask<Byte>[] result = new VectorMask[34];
// result[0] = SPECIES.maskAll(false);
// result[1] = SPECIES.maskAll(false);
//
// int maskSource = 0x1;
// for (int i = 2; i < 34; i++) {
// result[i] = VectorMask.fromLong(SPECIES, maskSource);
// maskSource <<= 1;
// maskSource += 1;
// }
//
// return result;
// }
//
// private static final int[] STOI_SIZE_LOOKUP = {
// 0, 0, 0, 0, 0, 4, 6, 0,
// 0, 0, 5, 5, 0, 0, 0, 0
// };
//
// private Arena arena;
// private MemorySegment inputData;
//
// private MemorySegment output;
//
// private MemorySegment hashMapData;
// private int hashMapDataWriteIndex;
//
// @Setup
// public void setup() {
// try {
// arena = Arena.ofShared();
// inputData = mmapDataFile(FILE, arena);
//
// hashMap = new RawHashMapUnsafe(arena);
//// hashMap = new int[TABLE_SIZE];
//// hashMapData = arena.allocate(10_000 * (4 + 100 + 28));
//// output = arena.allocate(6 * 1024 * 1024 * 1024L, 1);
// } catch (IOException e) {
// throw new RuntimeException(e);
// }
// }
//
// @Setup(Level.Iteration)
// public void setupClean() {
// Arrays.fill(hashMap, 0);
// hashMapDataWriteIndex = 0;
// sequence = 0;
//
// installNewCity(0, 16, ByteVector.SPECIES_256.zero());
// }
//
// // "London;14.6\nUpington;20.4\nPalerm"
// // London;-12.3\n
// // ......T.TT.T. -> .T.TT.T...... -> T.TT. -> 0x6
// // London;12.3\n
// // ......TTT.T. -> ..T.TTT...... -> .T.TT -> 0xB
// // London;-2.3\n
// // ......T.T.T. -> ..T.T.T...... -> .T.T. -> 0xA
// // London;2.3\n
// // ......TT.T. -> .T.TT...... -> .T.T -> 0x5
// // AAABBCCDDEE;1
// // ...........T.
// // Benchmark Mode Cnt Score Error Units
// // SmallVectorMicrobenchmark.justParseTemps avgt 2 5.167 s/op
// @Benchmark
// public long justParseTemps() {
// long stride = inputData.byteSize() / 2;
//
// long offset1 = 0;
// long end1 = stride - ByteVector.SPECIES_256.vectorByteSize();
//
// long offset2 = CalculateAverage_tkowalcz.findPastNewline(inputData, end1);
// long end2 = stride + stride - ByteVector.SPECIES_256.vectorByteSize();
//
// long offset3 = CalculateAverage_tkowalcz.findPastNewline(inputData, end2);
// long end3 = stride + stride + stride - ByteVector.SPECIES_256.vectorByteSize();
//
// long outputOffset = 0;
// long collisions = process(offset1, end1, offset2, end2, offset3, end3, collisions, outputOffset);
//
// System.out.println("collisions = " + collisions);
// return collisions;
// }
//
// private long process(long offset1, long end1, long offset2, long end2, long offset3, long end3, long collisions, long outputOffset) {
// while (offset1 < end1 && offset2 < end2 && offset3 < end3) {
// Vector<Byte> byteVector1 = SPECIES.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
// Vector<Byte> byteVector2 = SPECIES.fromMemorySegment(inputData, offset2, ByteOrder.nativeOrder());
// Vector<Byte> byteVector3 = SPECIES.fromMemorySegment(inputData, offset3, ByteOrder.nativeOrder());
//
// Vector<Byte> byteVectorMinusZero1 = byteVector1.sub(ASCII_ZERO);
// VectorMask<Byte> compare1 = byteVectorMinusZero1.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR);
// int compareInt1 = (int) compare1.toLong();
// int delimiterPosition1 = Integer.numberOfTrailingZeros(compareInt1) + 1;
// if (delimiterPosition1 > 11) {
// offset1 = handleOver16(offset1);
// } else {
// VectorMask<Byte> hashMask1 = CITY_LOOKUP_MASK[delimiterPosition1];
// Vector<Byte> hashInput1 = ZERO.blend(byteVector1, hashMask1);
//
// int perfectHash32_1 = hashInput1.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
// int index1 = perfectHash32_1 & TABLE_SIZE_MASK;
// int cityNameOffset = (index1 << 5) + (index1 << 4);
//
// ByteVector cityVector = ByteVector.fromMemorySegment(SPECIES, hashMapData, cityNameOffset, ByteOrder.nativeOrder());
// if (!cityVector.compare(VectorOperators.EQ, hashInput1).allTrue()) {
// collisions++;
// cityNameOffset = hashMiss(hashMap, cityNameOffset, index1, hashInput1, delimiterPosition1);
// }
//
// output.set(ValueLayout.JAVA_INT, outputOffset, index1);
// outputOffset += 2;
// }
//
// Vector<Byte> byteVectorMinusZero2 = byteVector2.sub(ASCII_ZERO);
// VectorMask<Byte> compare2 = byteVectorMinusZero2.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR);
// int compareInt2 = (int) compare2.toLong();
// int delimiterPosition2 = Integer.numberOfTrailingZeros(compareInt2) + 1;
// if (delimiterPosition2 > 11) {
// offset2 = handleOver16(offset2);
// } else {
// short lookupIndex2 = (short) (compareInt2 >> delimiterPosition2 & 0xF);
// // int value2 = (int) byteVectorMinusZero2
// byteVectorMinusZero2
// .rearrange(STOI_SHUFFLE_1[lookupIndex2])
// .castShape(ShortVector.SPECIES_256, 0)
// .mul(STOI_MUL_LOOKUP[lookupIndex2])
// // .reduceLanesToLong(VectorOperators.ADD);
// // output.set(ValueLayout.JAVA_INT_UNALIGNED, outputOffset, value2);
// .intoMemorySegment(output, outputOffset, ByteOrder.nativeOrder());
// outputOffset += 4;
// offset2 += delimiterPosition2 + STOI_SIZE_LOOKUP[lookupIndex2];
//
// VectorMask<Byte> hashMask2 = CITY_LOOKUP_MASK[delimiterPosition2];
// Vector<Byte> hashInput2 = ZERO.blend(byteVector2, hashMask2);
//
// int perfectHash32_2 = hashInput2.reinterpretAsInts().reduceLanes(VectorOperators.ADD);
// int index2 = perfectHash32_2 & TABLE_SIZE_MASK;
// int cityOffset2 = hashMap[index2];
// short cityId2;
// cityId2 = hashMapData.get(ValueLayout.JAVA_SHORT, cityOffset2);
// int cityNameOffset = cityOffset2 + 4;
//
// ByteVector cityVector = ByteVector.fromMemorySegment(SPECIES, hashMapData, cityNameOffset, ByteOrder.nativeOrder());
// if (!cityVector.compare(VectorOperators.EQ, hashInput2).allTrue()) {
// collisions++;
// cityId2 = hashMiss(cityOffset2, index2, hashInput2, delimiterPosition1);
// }
//
// output.set(ValueLayout.JAVA_SHORT, outputOffset, cityId2);
// outputOffset += 2;
// }
// }
//
// return collisions;
// }
//
// private int hashMiss(RawHashMapUnsafe hashMap, int cityNameOffset, int index, Vector<Byte> hashInput, int delimiterPosition) {
//// if (cityOffset == 0) {
// hashMap.installNewCity(cityNameOffset, delimiterPosition, hashInput);
//// } else {
//// index = (index + 1) & TABLE_SIZE_MASK;
////
//// }
////
// return cityNameOffset;
// }
//
// private short installNewCity(int index, int delimiterPosition, Vector<Byte> hashInput) {
// short cityId1 = installEntry(index, delimiterPosition);
//
// hashInput.intoMemorySegment(hashMapData, hashMapDataWriteIndex, ByteOrder.nativeOrder());
// hashMapDataWriteIndex += SPECIES.vectorByteSize();
// return cityId1;
// }
//
// private RawHashMapUnsafe hashMap;
//
// // We will use very large table for hash map to reduce collisions. There is little downside in increasing it as
// // we pay only cost of a reference (so 0x400000 size uses 32m of memory * thread count).
// public static final int TABLE_SIZE = 0x400000;
//
// // Mask to calculate "hashCode % TABLE_SIZE" without division (%).
// public static final int TABLE_SIZE_MASK = TABLE_SIZE - 1;
//
// private short sequence = 0;
//
// private short installEntry(int mapIndex, int cityNameSize) {
// short cityId = sequence++;
//
//// hashMap[mapIndex] = hashMapDataWriteIndex;
//// hashMapData.set(ValueLayout.JAVA_SHORT, hashMapDataWriteIndex, cityId);
//// hashMapDataWriteIndex += 2;
//// hashMapData.set(ValueLayout.JAVA_SHORT, hashMapDataWriteIndex, (short) cityNameSize);
//// hashMapDataWriteIndex += 2;
//
// return cityId;
// }
//
// private static final Vector<Byte> NEWLINE_VECTOR_256 = ByteVector.SPECIES_256.broadcast('\n');
//
// private long handleOver16(long offset1) {
// VectorMask<Byte> newlines1;
// Vector<Byte> byteVector1;
// byteVector1 = ByteVector.SPECIES_256.fromMemorySegment(inputData, offset1, ByteOrder.nativeOrder());
// newlines1 = byteVector1.compare(VectorOperators.EQ, NEWLINE_VECTOR_256);
//
// int newline = newlines1.firstTrue();
// if (newline == 32) {
// while (inputData.get(ValueLayout.JAVA_BYTE, offset1) != '\n') {
// offset1++;
// }
//
// offset1++;
// } else {
// offset1 += newline + 1;
// }
// return offset1;
// }
//
// public static void main(String[] args) throws RunnerException {
// runWithPerfAsm(JustTempsInaSmallVectorMicrobenchmark.class.getSimpleName());
// // run(JustTempsInaSmallVectorMicrobenchmark.class.getSimpleName());
// // runWithJFR(JustTempsInaSmallVectorMicrobenchmark.class.getSimpleName());
// }
// }
