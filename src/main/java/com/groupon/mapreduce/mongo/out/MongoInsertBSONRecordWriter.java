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

import com.groupon.mapreduce.mongo.WritableBSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This is the RecordWriter which actually handles inserting WritableBSONObjects into Mongo.
 */
public class MongoInsertBSONRecordWriter extends RecordWriter<Text, WritableBSONObject> {
    private boolean upsert;
    private DBCollection coll;


    public MongoInsertBSONRecordWriter(DBCollection coll, boolean upsert) {
        this.coll = coll;
        this.upsert = upsert;
    }

    /**
     * Write out the given WritableBSONObject to Mongo with the collection passed in at construction.
     * @param key Key value output from MR job, this is ignored.
     * @param value Document to write to Mongo.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, WritableBSONObject value) throws IOException, InterruptedException {
        value.getBSONObject();
        DBObject dbObject = (DBObject) value.getBSONObject();
        BasicDBObject criteria = new BasicDBObject();
        criteria.put("_id", dbObject.get("_id").toString());

        if (upsert)
            coll.update(criteria, dbObject, true, false);
        else
            coll.insert(dbObject);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    }
}
