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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * This is a Mongo model that holds a block of records. Each Extent is represented as an InputSplit when a
 * MapReduce job is run, thus each InputSplit holds one Extent, and Extent must be Writable. A Mongo database
 * is essentially a doubly linked list of Extents, which each hold a doubly linked list of Records. Thus,
 * each Extent holds the DiskLocs for the next and previous extents, and the DiskLocs for its first and last
 * Record.
 */
public class Extent implements Writable {
    private static final int HEADER_SIZE = 4 + 8 + 8 + 8 + 4 + 8 + 8 + 128;
    private static final String MAGIC_NUM = "DCBA";

    private Path path;
    private DiskLoc myLoc;
    private DiskLoc prevExtent;
    private DiskLoc nextExtent;
    private DiskLoc firstRecord;
    private DiskLoc lastRecord;
    private int length;

    private byte[] cache = null;

    /**
     * We construct an Extent with location information, then the Extent's fields are loaded from that file
     * and offset.
     * @param namespace Namespace of this Extent, used to fetch the relevant file.
     * @param loc DiskLoc of the start of this extent, used to read from the correct location.
     */
    public Extent(Namespace namespace, DiskLoc loc) {
        path = namespace.getPath(loc.getFileNum());
        myLoc = loc;
        FSDataInputStream stream = namespace.getStream(loc.getFileNum());

        byte[] buff = new byte[HEADER_SIZE];

        try {
            stream.seek(loc.getOffset());
            stream.read(buff);
        }
        catch(IOException e) {
            throw new RuntimeException("Failed to read extent", e);
        }

        String start = new String(buff, 0, 4);
        if (!start.equals(MAGIC_NUM))
            throw new RuntimeException("Did not find expected extent magic number");

        myLoc           = new DiskLoc(JobUtil.readInt(buff, 4), JobUtil.readInt(buff, 8));
        nextExtent      = new DiskLoc(JobUtil.readInt(buff, 12), JobUtil.readInt(buff, 16));
        prevExtent      = new DiskLoc(JobUtil.readInt(buff, 20), JobUtil.readInt(buff, 24));
        length          = JobUtil.readInt(buff, 28 + 128);
        firstRecord     = new DiskLoc(JobUtil.readInt(buff, 32 + 128), JobUtil.readInt(buff, 36 + 128));
        lastRecord      = new DiskLoc(JobUtil.readInt(buff, 40 + 128), JobUtil.readInt(buff, 44 + 128));

        if (!loc.equals(myLoc))
            throw new RuntimeException("Extent " + loc.toString() + " could not be found. " + myLoc.toString());
    }

    public Extent() {}

    public Path getPath() {
        return path;
    }

    public boolean hasNextExtent() {
        return nextExtent.getFileNum() > 0;
    }

    public DiskLoc getNextExtent() {
        return nextExtent;
    }

    public int getOffset() {
        return myLoc.getOffset();
    }

    public Record getFirstRecord(FileSystem fs) {
        if (firstRecord.getFileNum() < 0 || firstRecord.getOffset() < 0)
            return null;
        return new Record(this, firstRecord.getOffset() - myLoc.getOffset(), fs);
    }

    /**
     * This buffer actually holds the Records in this extent, but we don't load it until we need it. Once
     * loaded, its cached.
     * @param fs Filesystem with which to load the Record buffer.
     * @return Byte array of the Record buffer.
     */
    public byte[] getBuffer(FileSystem fs) {
        if (cache != null)
            return cache;

        byte[] buff = new byte[length];

        try {
            FSDataInputStream stream = fs.open(path);
            stream.seek(myLoc.getOffset());
            stream.readFully(buff);
            stream.close();
        } catch (Exception e) {
            throw new RuntimeException("Could not get extent buffer", e);
        }

        cache = buff;
        return buff;
    }

    public int getLength() {
        return length;
    }

    public Iterator<Record> iterator(final FileSystem fs) {
        return new Iterator<Record>() {
            Record current = getFirstRecord(fs);

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Record next() {
                if (!hasNext())
                    return null;

                Record temp = current;
                current = current.getNextRecord(fs);
                return temp;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Tried to remove record");
            }
        };
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (path == null)
            throw new RuntimeException("Could not write path");

        JobUtil.writeString(path.toString(), dataOutput);

        if (myLoc == null || prevExtent == null || nextExtent == null ||
                firstRecord == null || lastRecord == null)
            throw new RuntimeException("Could not write extend DataLocs");

        myLoc.write(dataOutput);
        prevExtent.write(dataOutput);
        nextExtent.write(dataOutput);
        firstRecord.write(dataOutput);
        lastRecord.write(dataOutput);

        dataOutput.writeInt(length);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        path = new Path(JobUtil.readString(dataInput));

        myLoc = new DiskLoc(0, 0);
        myLoc.readFields(dataInput);
        prevExtent = new DiskLoc(0, 0);
        prevExtent.readFields(dataInput);
        nextExtent = new DiskLoc(0, 0);
        nextExtent.readFields(dataInput);
        firstRecord = new DiskLoc(0, 0);
        firstRecord.readFields(dataInput);
        lastRecord = new DiskLoc(0, 0);
        lastRecord.readFields(dataInput);

        length = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || o.getClass() != o.getClass()) return false;

        Extent that = (Extent) o;

        if (!this.path.equals(that.path))
            return false;

        if (!this.myLoc.equals(that.myLoc))
            return false;

        if (!this.prevExtent.equals(that.prevExtent))
            return false;

        if (!this.nextExtent.equals(that.nextExtent))
            return false;

        if (!this.firstRecord.equals(that.firstRecord))
            return false;

        if (!this.lastRecord.equals(that.lastRecord))
            return false;

        if (this.length != that.length)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "extent " + myLoc.toString() + " next " + nextExtent.toString() + " prev " + prevExtent.toString()
                + " firstRecord " + firstRecord.toString();
    }
}
