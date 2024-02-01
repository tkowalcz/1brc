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
import dev.morling.onebrc.CalculateAverage_tkowalcz2Unsafe;
import jdk.incubator.vector.ByteVector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@Threads(1)
public class CalculateAverage2UnsafeMicrobenchmark extends OneBrcMicrobenchmark {

    private static final String FILE = "measurements.txt";

    private Arena arena;
    private MemorySegment inputData;
    private CalculateAverage_tkowalcz2Unsafe.UnsafeRawHashMap hashMap;

    @Setup
    public void setup() {
        try {
            arena = Arena.ofShared();
            inputData = mmapDataFile(FILE, arena);

            hashMap = new CalculateAverage_tkowalcz2Unsafe.UnsafeRawHashMap(arena);
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
    public List<CalculateAverage_tkowalcz2Unsafe.StatisticsAggregate> doublePumped() {
        CalculateAverage_tkowalcz2Unsafe.WorkerThread workerThread = new CalculateAverage_tkowalcz2Unsafe.WorkerThread(
                null,
                arena,
                0);

        return workerThread.execute(arena, inputData);
    }

    public static void main(String[] args) throws RunnerException {
        runWithPerfAsm(CalculateAverage2UnsafeMicrobenchmark.class.getSimpleName());
        // runWithJFR(CalculateAverage2Microbenchmark.class.getSimpleName());
        
    }
}
