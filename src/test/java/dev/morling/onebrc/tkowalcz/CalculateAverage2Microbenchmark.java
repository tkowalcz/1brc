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

import dev.morling.onebrc.CalculateAverage_tkowalcz;
import dev.morling.onebrc.CalculateAverage_tkowalcz2;
import jdk.incubator.vector.ByteVector;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

@Threads(1)
public class CalculateAverage2Microbenchmark extends OneBrcMicrobenchmark {

    private static final String FILE = "measurements.txt";

    private Arena arena;
    private MemorySegment inputData;
    private CalculateAverage_tkowalcz2.RawHashMap2 hashMap;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);

            hashMap = new CalculateAverage_tkowalcz2.RawHashMap2(arena);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup(Level.Invocation)
    public void clearMap() {
        // hashMap.clear();
    }

    @Benchmark
    public CalculateAverage_tkowalcz2.DoubleCursor doublePumped() {
        long stride = inputData.byteSize() / 3;

        long offset1 = 0;
        long end1 = stride - ByteVector.SPECIES_256.vectorByteSize();

        long offset2 = CalculateAverage_tkowalcz.findPastNewline(inputData, end1);
        long end2 = stride + stride - ByteVector.SPECIES_256.vectorByteSize();

        long offset3 = CalculateAverage_tkowalcz.findPastNewline(inputData, end2);
        long end3 = stride + stride + stride - ByteVector.SPECIES_256.vectorByteSize();

        long offset4 = CalculateAverage_tkowalcz.findPastNewline(inputData, end3);
        long end4 = stride + stride + stride + stride - ByteVector.SPECIES_256.vectorByteSize();

        return CalculateAverage_tkowalcz2.executeDoublePumped(inputData,
                hashMap,
                offset1,
                end1,
                offset2,
                end2,
                offset3,
                end3);
    }

    public static void main(String[] args) throws RunnerException {
        runWithPerfAsm(CalculateAverage2Microbenchmark.class.getSimpleName());
        // runWithJFR(CalculateAverage2Microbenchmark.class.getSimpleName());
    }
}
