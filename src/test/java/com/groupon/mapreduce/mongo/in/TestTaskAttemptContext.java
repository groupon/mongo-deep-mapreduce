/*
Copyright (c) 2013, Groupon, Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

Neither the name of GROUPON nor the names of its contributors may be
used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package com.groupon.mapreduce.mongo.in;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.net.URI;

public class TestTaskAttemptContext implements TaskAttemptContext {
    private Configuration config;

    public TestTaskAttemptContext(Configuration c) {
        config = c;
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
        return null;
    }

    @Override
    public void setStatus(String s) {
      
    }

    @Override
    public String getStatus() {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }

    @Override
    public Credentials getCredentials() {
        return null;
    }

    @Override
    public JobID getJobID() {
        return null;
    }

    @Override
    public int getNumReduceTasks() {
        return 0;
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
        return null;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return null;
    }

    @Override
    public Class<?> getOutputValueClass() {
        return null;
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
        return null;
    }

    @Override
    public Class<?> getMapOutputValueClass() {
        return null;
    }

    @Override
    public String getJobName() {
        return null;
    }

    @Override
    public boolean userClassesTakesPrecedence() {
        return false;
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public RawComparator<?> getSortComparator() {
        return null;
    }

    @Override
    public String getJar() {
        return null;
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
        return null;
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
        return false;
    }

    @Override
    public boolean getProfileEnabled() {
        return false;
    }

    @Override
    public String getProfileParams() {
        return null;
    }

    @Override
    public String getUser() {
        return null;
    }

    @Override
    public boolean getSymlink() {
        return false;
    }

    @Override
    public Path[] getArchiveClassPaths() {
        return new Path[0];
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
        return new URI[0];
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
        return new URI[0];
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
        return new Path[0];
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
        return new Path[0];
    }

    @Override
    public Path[] getFileClassPaths() {
        return new Path[0];
    }

    @Override
    public String[] getArchiveTimestamps() {
        return new String[0];
    }

    @Override
    public String[] getFileTimestamps() {
        return new String[0];
    }

    @Override
    public int getMaxMapAttempts() {
        return 0;
    }

    @Override
    public int getMaxReduceAttempts() {
        return 0;
    }

    @Override
    public void progress() {
      
    }
}
