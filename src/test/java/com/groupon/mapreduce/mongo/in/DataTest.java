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

import com.groupon.mapreduce.mongo.*;
import com.groupon.mapreduce.mongo.in.MongoInputFormat;
import com.groupon.mapreduce.mongo.in.MongoInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DataTest {
    private void testCollection(String coll, FileSystem fs, Path path) throws Exception {
        MongoInputFormat.setCollection(coll);

        MongoInputFormat inputFormat = new MongoInputFormat();
        List<MongoInputSplit> splits = inputFormat.getSplitsFromFile(fs, path);

        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        TaskAttemptContext context = new TestTaskAttemptContext(conf);
        int count = 0;

        for (MongoInputSplit split : splits) {
            RecordReader<Text, WritableBSONObject> reader = inputFormat.createRecordReader(split, context);
            while (reader.nextKeyValue()) {
                assertNotNull(reader.getCurrentKey());
                WritableBSONObject obj = reader.getCurrentValue();
                Map record = obj.getBSONObject().toMap();

                Assert.assertEquals(new Integer(count), JobUtil.get(record, "key1").get(0));
                assertEquals(new Integer(count * 2), JobUtil.get(record, "key2").get(0));
                assertEquals(String.valueOf(count), JobUtil.get(record, "key3").get(0));
                assertEquals(new Integer(count), JobUtil.get(record, "key4.key5").get(0));
                assertEquals("one", JobUtil.get(record, "key6").get(0));
                assertEquals("two", JobUtil.get(record, "key6").get(1));
                assertEquals("three", JobUtil.get(record, "key6").get(2));
                count++;
            }
        }

        Assert.assertEquals(GenerateTestDB.NUM_RECORDS, count);
    }

    @Test
    public void testData() throws Exception {
        Path path = new Path(NamespaceIndexTest.DB_FILE);

        FileSystem fs = NamespaceIndexTest.getFilesystem();

        MongoInputFormat.setDatabase(GenerateTestDB.DB_NAME);
        MongoInputFormat.setMongoDirectory(new Path("src/test"));

        testCollection(GenerateTestDB.COLL_NAME_1, fs, path);
        testCollection(GenerateTestDB.COLL_NAME_2, fs, path);
    }
}
