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

import com.groupon.mapreduce.mongo.JobUtil;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class wraps a Mongo Extent class for Hadoop, which includes providing the local machines
 * on which that extent is stored.
 */
public class MongoInputSplit extends InputSplit implements Writable {
    private Extent extent = null;
    private List<String> locations = new ArrayList<String>();

    public MongoInputSplit(Extent extent, FileSystem fileSystem) {
        this.extent = extent;

        // determine which machines each machine resides on, used for locality during a MapReduce
        try {
            Path path = extent.getPath();
            FileStatus status = fileSystem.getFileStatus(path);
            BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(
                    status, extent.getOffset(), extent.getLength());

            for (BlockLocation blockLocation : blockLocations)
                for (String host : blockLocation.getHosts())
                    locations.add(host);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not get MongoInputSplit locations", e);
        }
    }

    public MongoInputSplit() {}

    public Extent getExtent() {
        return extent;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return extent.getLength();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        String[] locs = new String[locations.size()];
        for (int i = 0; i < locations.size(); i++)
            locs[i] = locations.get(i);
        return locs;
    }

    /**
     * Serialize MongoInputSplit to binary by first writing the Extent out, then the number of locations,
     * then each location as a String
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (extent == null || locations == null)
            throw new RuntimeException("Could not write Split");

        extent.write(dataOutput);
        dataOutput.writeInt(locations.size());
        for (String location : locations)
            JobUtil.writeString(location, dataOutput);
    }

    /**
     * Deserialize by reading the extent from the binary dataInput, then the number of locations, then each location
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        extent = new Extent();
        extent.readFields(dataInput);
        int length = dataInput.readInt();
        locations = new ArrayList<String>();

        for (int i = 0; i < length; i++)
            locations.add(JobUtil.readString(dataInput));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MongoInputSplit that = (MongoInputSplit) o;
        return this.extent.equals(that.extent) && this.locations.equals(that.locations);
    }
}
