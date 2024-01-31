#!/bin/sh
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Don't do -XX:+AlwaysPreTouch
JAVA_OPTS_JFR="-XX:StartFlightRecording=duration=60s,filename=myrecording.jfr,delay=1s"
#JAVA_OPTS_COMPILE='-XX:CompileCommand="option dev.morling.onebrc/CalculateAverage_tkowalcz.execute*"'
#JAVA_OPTS_COMPILE='-XX:CompileThreshold=1'
JAVA_OPTS_COMPILE='-XX:+TrustFinalNonStaticFields -XX:CompileThreshold=2048 -XX:-UseCountedLoopSafepoints' #-XX:InlineSmallCode=10000 -XX:CompileThreshold=10 -XX:-TieredCompilation'
#JAVA_OPTS_COMPILE='-XX:CompileCommand="option dev.morling.onebrc/CalculateAverage_tkowalcz.execute*"'
JAVA_OPTS_VECTOR="--add-modules jdk.incubator.vector -XX:+EnableVectorReboxing -XX:+EnableVectorAggressiveReboxing -Djdk.incubator.vector.VECTOR_ACCESS_OOB_CHECK=0"
JAVA_OPTS_GC="-Xmx8g -Xmn8g -XX:+UseEpsilonGC -XX:-AlwaysPreTouch -XX:+UseTransparentHugePages -XX:-UseCompressedOops"
#JAVA_OPTS_GC="-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -Xmx8g -Xmn8g"
# -XX:-LoadCachedCode -XX:CacheDataStore=application.cds
JAVA_OPTS="--enable-preview  -XX:+UnlockExperimentalVMOptions  ${JAVA_OPTS_GC} ${JAVA_OPTS_VECTOR} ${JAVA_OPTS_COMPILE}"
perf record -F 99 java $JAVA_OPTS --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_tkowalcz
