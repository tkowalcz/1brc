/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.morling.onebrc;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.function.BiConsumer;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;

/**
 * Collection of IO utilities for dealing with files, especially mapping and un-mapping.
 */
@SuppressWarnings("deprecation")
public final class IoUtil {
    /**
     * Size in bytes of a file page.
     */
    public static final int BLOCK_SIZE = 4 * 1024;

    private static final byte[] FILLER = new byte[BLOCK_SIZE];
    private static final int MAP_READ_ONLY = 0;
    private static final int MAP_READ_WRITE = 1;
    private static final int MAP_PRIVATE = 2;

    static class MappingMethods {
        static final MethodHandle MAP_FILE_DISPATCHER;
        static final MethodHandle UNMAP_FILE_DISPATCHER;
        static final Object FILE_DISPATCHER;
        static final MethodHandle GET_FILE_DESCRIPTOR;
        static final MethodHandle MAP_WITH_SYNC_ADDRESS;
        static final MethodHandle MAP_ADDRESS;
        static final MethodHandle UNMAP_ADDRESS;

        static {
            try {
                final Class<?> fileChannelClass = Class.forName("sun.nio.ch.FileChannelImpl");
                final Class<?> fileDispatcherClass = Class.forName("sun.nio.ch.FileDispatcher");
                MethodHandles.Lookup lookup = MethodHandles.lookup();

                Object fileDispatcher = null;
                MethodHandle mapFileDispatcher = null;
                MethodHandle getFD = null;
                MethodHandle mapAddress = null;
                MethodHandle mapWithSyncAddress = null;
                MethodHandle unmapFileDispatcher = null;
                MethodHandle unmapAddress = null;
                try {
                    final Field implLookupField = MethodHandles.Lookup.class.getDeclaredField("IMPL_LOOKUP");
                    lookup = (MethodHandles.Lookup) UnsafeAccess.UNSAFE.getObject(
                            MethodHandles.Lookup.class, UnsafeAccess.UNSAFE.staticFieldOffset(implLookupField));
                    fileDispatcher = lookup.unreflectGetter(fileChannelClass.getDeclaredField("nd")).invoke();
                    getFD = lookup.unreflectGetter(fileChannelClass.getDeclaredField("fd"));
                    mapFileDispatcher = lookup.unreflect(fileDispatcherClass.getDeclaredMethod(
                            "map",
                            FileDescriptor.class,
                            int.class,
                            long.class,
                            long.class,
                            boolean.class));
                    unmapFileDispatcher = lookup.unreflect(
                            fileDispatcherClass.getDeclaredMethod("unmap", long.class, long.class));
                }
                catch (final Throwable ex) {
                    unmapAddress = lookup.unreflect(getMethod(fileChannelClass, "unmap0", long.class, long.class));
                    try {
                        mapWithSyncAddress = lookup.unreflect(getMethod(
                                fileChannelClass, "map0", int.class, long.class, long.class, boolean.class));
                    }
                    catch (final Exception ex2) {
                        mapAddress = lookup.unreflect(getMethod(
                                fileChannelClass, "map0", int.class, long.class, long.class));
                    }
                }

                MAP_FILE_DISPATCHER = mapFileDispatcher;
                UNMAP_FILE_DISPATCHER = unmapFileDispatcher;
                FILE_DISPATCHER = fileDispatcher;
                GET_FILE_DESCRIPTOR = getFD;
                MAP_WITH_SYNC_ADDRESS = mapWithSyncAddress;
                MAP_ADDRESS = mapAddress;
                UNMAP_ADDRESS = unmapAddress;
            }
            catch (final Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private static Method getMethod(
                                        final Class<?> klass, final String name, final Class<?>... parameterTypes)
                throws NoSuchMethodException {
            final Method method = klass.getDeclaredMethod(name, parameterTypes);
            method.setAccessible(true);
            return method;
        }
    }

    private IoUtil() {
    }

    /**
     * Map a range of a file and return the address at which the range begins.
     *
     * @param fileChannel to be mapped.
     * @param mode        for the mapped region.
     * @param offset      within the file the mapped region should start.
     * @param length      of the mapped region.
     * @return the address at which the mapping starts.
     */
    public static long map(
                           final FileChannel fileChannel, final FileChannel.MapMode mode, final long offset, final long length) {
        try {
            if (null != MappingMethods.MAP_FILE_DISPATCHER) {
                final FileDescriptor fd = (FileDescriptor) MappingMethods.GET_FILE_DESCRIPTOR.invoke(fileChannel);
                return (long) MappingMethods.MAP_FILE_DISPATCHER.invoke(
                        MappingMethods.FILE_DISPATCHER, fd, getMode(mode), offset, length, false);
            }
            else if (null != MappingMethods.MAP_ADDRESS) {
                return (long) MappingMethods.MAP_ADDRESS.invoke(fileChannel, getMode(mode), offset, length);
            }
            else {
                return (long) MappingMethods.MAP_WITH_SYNC_ADDRESS.invoke(
                        fileChannel, getMode(mode), offset, length, false);
            }
        }
        catch (final Throwable ex) {
            throw new RuntimeException(ex);
        }
    }

    private static int getMode(final FileChannel.MapMode mode) {
        if (mode == READ_ONLY) {
            return MAP_READ_ONLY;
        }
        else if (mode == READ_WRITE) {
            return MAP_READ_WRITE;
        }
        else {
            return MAP_PRIVATE;
        }
    }
}
