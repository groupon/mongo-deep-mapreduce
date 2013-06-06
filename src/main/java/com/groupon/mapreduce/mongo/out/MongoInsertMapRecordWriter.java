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

package com.groupon.mapreduce.mongo.out;

import com.mongodb.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This is the RecordWriter which actually handles inserting MapWritables into Mongo.
 */
public class MongoInsertMapRecordWriter extends RecordWriter<Text, MapWritable> {
    private DBCollection coll;
    private boolean upsert;

    public MongoInsertMapRecordWriter(DBCollection coll, boolean upsert) {
        this.coll = coll;
        this.upsert = upsert;
    }

    /**
     * Given a Writable value, return its basic Java object equivalent based on its type.
     * @param value
     * @return
     */
    private Object cast(Writable value) {
        Class cls = value.getClass();

        if (cls.equals(NullWritable.class))
            return null;

        if (cls.equals(Text.class))
            return value.toString();

        if(cls.equals(IntWritable.class))
            return ((IntWritable) value).get();

        if(cls.equals(FloatWritable.class))
            return ((FloatWritable) value).get();

        if(cls.equals(DoubleWritable.class))
            return ((DoubleWritable) value).get();

        if(cls.equals(LongWritable.class))
            return ((LongWritable) value).get();

        if (cls.equals(ArrayWritable.class)) {
            BasicDBList list = new BasicDBList();
            Writable[] array = ((ArrayWritable) value).get();

            for (Writable v : array)
                list.add(cast(v));

            return list;
        }

        throw new RuntimeException("Could not handle writable class " + cls.getName());
    }

    /**
     * Write out the given MapWritable to Mongo with the collection passed in at construction.
     * @param o Key value output from MR job, this is ignored.
     * @param o2 Document to write to Mongo.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text o, MapWritable o2) throws IOException, InterruptedException {
        DBObject dbObject = new BasicDBObject();

        // cast MapWritable to Java types in a DBObject
        for (Writable key : o2.keySet()) {
            String k = key.toString();
            Writable value = o2.get(key);
            dbObject.put(k, cast(value));
        }

        BasicDBObject criteria = new BasicDBObject();
        criteria.put("_id", dbObject.get("_id").toString());

        if (upsert)
            coll.update(criteria, dbObject, true, false);
        else
            coll.insert(dbObject, WriteConcern.SAFE);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    }
}
