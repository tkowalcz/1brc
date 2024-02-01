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
package dev.morling.onebrc.tkowalcz;

import jdk.incubator.vector.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

class OptimisedIntegerParsingTest {

    private static final Vector<Byte> ASCII_ZERO = ByteVector.SPECIES_256.broadcast('0');
    private static final Vector<Byte> DELIMITER_MINUS_ASCII_ZERO_VECTOR = ByteVector.SPECIES_256.broadcast(';' - '0');

    private static final ShortVector[] STOI_MUL_LOOKUP = {
            null,
            null,
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 0, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 100, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            null,
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0),
            ShortVector.fromArray(ShortVector.SPECIES_256, new short[]{ -100, -10, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0)
    };

    private static final VectorShuffle<Byte>[] STOI_REARRANGE_1 = new VectorShuffle[]{
            null
    };

    private static final VectorShuffle<Byte>[] STOI_REARRANGE_2 = new VectorShuffle[]{
            null
    };

    private static final VectorShuffle<Byte>[] STOI_REARRANGE_3 = new VectorShuffle[]{
            null
    };

    // Toliara;-12.3\n
    // .......T.TT.T. -> T.T -> 0x5
    // Toliara;12.3\n
    // .......TTT.T. -> .TT -> 0x3
    // Toliara;-2.3\n
    // .......T.T.T. -> .T. -> 0x2
    // Toliara;2.3\n
    // .......TT.T. -> ..T -> 0x1

    // Toliara;-12.3\n
    // .T.TT.T....... -> .T.TT. -> TT. -> 0x6
    // Toliara;12.3\n
    // .T.TTT....... -> .T.TT -> .TT -> 0x3
    // Toliara;-2.3\n
    // .T.T.T....... -> .T.T. -> .T. -> 0x2
    // Toliara;2.3\n
    // .T.TT....... -> .T.T -> T.T -> 0x5
    @Test
    void shouldParseNegativeNumberWithTwoIntegralDigits() {
        // Given
        Vector<Byte> vector1 = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "Toliara;-34.9\nSydney;9.8\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        Vector<Byte> vector2 = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "Sydney;9.8\nToliara;-34.9\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        Vector<Byte> vector3 = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "Brazzaville;24.2\nToliara;-34.9\nSydney;9.8".getBytes(StandardCharsets.UTF_8),
                0);

        // When
        Vector<Byte> sub1 = vector1.sub(ASCII_ZERO);
        Vector<Byte> sub2 = vector2.sub(ASCII_ZERO);
        Vector<Byte> sub3 = vector3.sub(ASCII_ZERO);
        VectorMask<Byte> mask1 = sub1.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR);
        VectorMask<Byte> mask2 = sub2.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR);
        VectorMask<Byte> mask3 = sub3.compare(VectorOperators.UNSIGNED_LE, DELIMITER_MINUS_ASCII_ZERO_VECTOR);

        int lookupIndex1 = (int) ((mask1.toLong() >> 7) & 0x7);
        int lookupIndex2 = (int) ((mask2.toLong() >> 7) & 0x7);
        int lookupIndex3 = (int) ((mask3.toLong() >> 7) & 0x7);

        sub1.rearrange(STOI_REARRANGE_1[lookupIndex1])
                .add(sub2.rearrange(STOI_REARRANGE_2[lookupIndex2]))
                .add(sub3.rearrange(STOI_REARRANGE_3[lookupIndex3]));

        // long value = vector
        // .castShape(ShortVector.SPECIES_256, 0)
        // .mul(STOI_MUL_LOOKUP[lookupIndex])
        // .reduceLanesToLong(VectorOperators.ADD);

        // Then
        // assertThat(value).isEqualTo(-349L);
    }

    @Test
    void shouldParsePositiveNumberWithTwoIntegralDigits() {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "12.3\nToliara;34.9\nSydney;9.8\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x07);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(123L);
    }

    @Test
    void shouldParseNegativeNumberWithOneIntegralDigit() {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "-2.3\nToliara;34.9\nSydney;9.8\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x07);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(-23L);
    }

    @Test
    void shouldParsePositiveNumberWithOneIntegralDigit() {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                "2.3\nToliara;34.9\nSydney;9.8\nBrazzaville;24.2".getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x07);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(23L);
    }

    static Stream<Arguments> measurements() {
        return Stream.of(
                of("34.9\nSydney;34.9\nSydney;9.8\nBrazzaville;24.2", 349),
                of("9.8\nBrazzaville;34.9\nSydney;9.8\nBrazzaville;24.2", 98),
                of("24.2\nMilan;34.9\nSydney;9.8\nBrazzaville;24.2", 242),
                of("15.1\nPetropavlovsk-Kamchatsky;34.9\nSydney;9.8\nBrazzaville;24.2", 151),
                of("-5.9\nErbil;34.9\nSydney;9.8\nBrazzaville;24.2", -59),
                of("11.2\nBangui;34.9\nSydney;9.8\nBrazzaville;24.2", 112),
                of("22.9\nLyon;34.9\nSydney;9.8\nBrazzaville;24.2", 229),
                of("2.1\nLuanda;34.9\nSydney;9.8\nBrazzaville;24.2", 21),
                of("32.3\nJayapura;34.9\nSydney;9.8\nBrazzaville;24.2", 323));
    }

    @ParameterizedTest
    @MethodSource("measurements")
    void shouldParseMany(String input, long expected) {
        // Given
        Vector<Byte> vector = ByteVector.fromArray(
                ByteVector.SPECIES_256,
                input.getBytes(StandardCharsets.UTF_8),
                0);

        // When
        VectorMask<Byte> mask = vector.compare(VectorOperators.LT, ASCII_ZERO);
        int lookupIndex = (int) (mask.toLong() & 0x07);

        long value = vector
                .sub(ASCII_ZERO)
                .castShape(ShortVector.SPECIES_256, 0)
                .mul(STOI_MUL_LOOKUP[lookupIndex])
                .reduceLanesToLong(VectorOperators.ADD);

        // Then
        assertThat(value).isEqualTo(expected);
    }
}
