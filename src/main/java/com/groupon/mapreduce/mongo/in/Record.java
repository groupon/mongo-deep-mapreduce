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
import org.apache.hadoop.fs.FileSystem;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;

/**
 * This is the representation of a Mongo record as stored in binary format in one of the database files. This
 * contains the record length and offset, and the offsets of the next and previous records, in addition to
 * the BSON payload, which we load lazily.
 */
class Record {
    private Extent extent;
    private int offset;

    private int length;
    private int extentOffset;
    private int nextRecord;
    private int prevRecord;
    private int BSONlength;

    private BSONObject cache = null;

    public Record(Extent extent, int off, FileSystem fs) {
        if (off < 0)
            throw new RuntimeException("Tried to create record with offset " + off + " in " + extent.toString());

        this.extent = extent;
        this.offset = off;
        byte[] buff = extent.getBuffer(fs);

        length = JobUtil.readInt(buff, off);
        extentOffset = JobUtil.readInt(buff, off + 4);
        nextRecord = JobUtil.readInt(buff, off + 8);
        prevRecord = JobUtil.readInt(buff, off + 12);
        BSONlength = JobUtil.readInt(buff, off + 16);

        if (extentOffset != extent.getOffset())
            throw new RuntimeException("Record at extent offset " + extent.getOffset() +
                    " had extent offset set to " + extent.getOffset());
    }

    public String getId(FileSystem fs) {
        return getContent(fs).get("_id").toString();
    }

    public boolean hasNextRecord() {
        return nextRecord >= 0;
    }

    public Record getNextRecord(FileSystem fs) {
        if (!hasNextRecord())
            return null;
        return new Record(extent, nextRecord - extent.getOffset(), fs);
    }

    public BSONObject getContent(FileSystem fs) {
        if (cache != null)
            return cache;

        byte[] extentBuffer = extent.getBuffer(fs);
        byte[] buff = new byte[BSONlength];

        for (int i = offset + 16; i < offset + 16 + BSONlength; i++)
            buff[i - offset - 16] = extentBuffer[i];

        BasicBSONDecoder decoder = new BasicBSONDecoder();
        cache = decoder.readObject(buff);
        return cache;
    }

    @Override
    public String toString() {
        return "Record " + (offset + extentOffset) + " length " + length + " extent offset " + extentOffset +
                " nextRecord " + nextRecord + " prevRecord " + prevRecord + " BSONLength " + BSONlength +
                " in " + extent.toString();
    }
}
