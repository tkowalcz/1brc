package dev.morling.onebrc.tkowalcz;

import org.assertj.core.internal.Longs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class ScalarParseTest {

    static Stream<Arguments> numbers() {
        return Stream.of(
                Arguments.of("-12.3\n"),
                Arguments.of("12.3\n "),
                Arguments.of("-2.3\n "),
                Arguments.of("2.3\n  "));
    }

    @ParameterizedTest
    @MethodSource("numbers")
    void shouldParse(String number) {
        byte[] string = number.getBytes(StandardCharsets.UTF_8);
        long value = fromBytes(string[0], string[1], string[2], string[3], string[4], string[5], (byte) 0, (byte) 0);
        System.out.println(Long.toHexString(value));

        // long mask = 0b01111111_01111111_01111111_01111111_01111111_00000000_00000000_00000000L;
        // long notMask = 0b10000000_10000000_10000000_10000000_10000000_00000000_00000000_00000000L;
        long mask = 0b01111111_01111111_01111111_01111111_01111111_01111111_00000000_00000000L;
        long notMask = 0b10000000_10000000_10000000_10000000_10000000_10000000_00000000_00000000L;
        long predicate = 0x2E_2E_2E_2E_2E_2E_2E_2EL;
        long resultMask = (((predicate ^ value) ^ mask) + 0x01_01_01_01_01_01_01_01L) & notMask;

        // resultMask = resultMask >>> 7;
        System.out.println(String.format("%064d", new BigInteger(Long.toBinaryString(resultMask))));
        // System.out.println(Long.toBinaryString(resultMask));
        System.out.println(Long.numberOfLeadingZeros(resultMask) / 8);
    }

    public static long fromBytes(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
        return ((long) b1 & 255L) << 56 | ((long) b2 & 255L) << 48 | ((long) b3 & 255L) << 40 | ((long) b4 & 255L) << 32 | ((long) b5 & 255L) << 24
                | ((long) b6 & 255L) << 16 | ((long) b7 & 255L) << 8 | (long) b8 & 255L;
    }
}
