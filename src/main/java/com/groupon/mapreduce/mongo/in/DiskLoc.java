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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is a Mongo model that represents a location within a namespace. The variables are fileNum, which
 * holds a number of a DB file (e.g. 4 for database.4), and offset, which represents an offset in bytes
 * within that file. Its safe to use an int rather than a long, since these database files are never
 * larger than 2GB.
 */
class DiskLoc implements Writable {
    public DiskLoc(int fileNum, int offset) {
        this.fileNum = fileNum;
        this.offset = offset;
    }

    private int fileNum;
    private int offset;

    public int getFileNum() {
        return fileNum;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "{DiskLoc " + fileNum + " " + offset + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiskLoc that = (DiskLoc) o;
        return (this.fileNum == that.fileNum && this.offset == that.offset);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(fileNum);
        dataOutput.writeInt(offset);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fileNum = dataInput.readInt();
        offset = dataInput.readInt();
    }
}