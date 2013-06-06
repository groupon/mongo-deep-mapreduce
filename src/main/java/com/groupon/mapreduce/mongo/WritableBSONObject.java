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

package com.groupon.mapreduce.mongo;

import org.apache.hadoop.io.Writable;
import org.bson.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This is a simple wrapper for a BSONObject which uses the existing BSON serializers to encode and decode
 * BSONObjects to binary, fulfilling the Writable interface.
 */
public class WritableBSONObject implements Writable {
    BSONObject obj;

    /**
     * Construct using an existing BSON object.
     * @param obj BSON object to store.
     */
    public WritableBSONObject(BSONObject obj) {
        this.obj = obj;
    }

    /**
     * Construct with an empty BasicBSONObject.
     */
    public WritableBSONObject() {
        this.obj = new BasicBSONObject();
    }

    /**
     * Put to the contained BSONObject using the given key and value.
     * @param key String representing the key to put.
     * @param value Object representing the value.
     */
    public void put(String key, Object value) {
        obj.put(key, value);
    }

    /**
     * Get an object from the contained BSONObject with the given key.
     * @param key Key to fetch.
     * @return Object corresponding to that key.
     */
    public Object get(String key) {
        return obj.get(key);
    }

    /**
     * Get contained BSONObject. This object is not cloned, so encapsulation is not guaranteed here.
     * @return Contained BSONObject.
     */
    public BSONObject getBSONObject() {
        return obj;
    }

    /**
     * Required by the Writable interface, this just uses the existing BSON serializers and writes to
     * the DataOutput argument.
     * @param dataOutput Output to accept serialized BSON.
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        BSONEncoder encoder = new BasicBSONEncoder();
        byte[] bytes = encoder.encode(obj);
        dataOutput.write(bytes);
    }

    /**
     * Read BSONObject from the DataInput argument. This checks the length and then uses the existing BSON
     * serializers to reconstruct the object.
     * @param dataInput DataInput from which we read the BSONObject.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        byte[] lengthBytes = new byte[4];
        dataInput.readFully(lengthBytes, 0, 4);
        int length = JobUtil.readInt(lengthBytes, 0);
        byte[] bytes = new byte[length];

        dataInput.readFully(bytes, 4, length - 4);
        bytes[0] = lengthBytes[0];
        bytes[1] = lengthBytes[1];
        bytes[2] = lengthBytes[2];
        bytes[3] = lengthBytes[3];

        BSONDecoder decoder = new BasicBSONDecoder();
        BSONCallback callback = new BasicBSONCallback();
        decoder.decode(bytes, callback);
        obj = (BSONObject) callback.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WritableBSONObject that = (WritableBSONObject) o;
        return this.obj.equals(that.obj);
    }
}
