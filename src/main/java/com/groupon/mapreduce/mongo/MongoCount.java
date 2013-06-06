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

import com.groupon.mapreduce.mongo.in.MongoInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * This is a MapReduce job that demonstrates counting a collection that has been loaded into HDFS. It
 * uses the MongoInputFormat and doesn't need any Reducers.
 * It expects the path of the mongo files, database name, and collection name.
 * With the run script this looks like:
 * ./run com.groupon.mapreduce.mongo.MongoCount hadoophost.com /mongo/files dbname collname
 */
public class MongoCount extends Configured implements Tool {
    final Logger logger = LoggerFactory.getLogger(MongoCount.class);

    private static final IntWritable one = new IntWritable(1);

    public static class Map extends Mapper<Text, WritableBSONObject, Text, IntWritable> {
        @Override
        public void map(Text key, WritableBSONObject value, Context context)
                throws IOException, InterruptedException {
            context.write(new Text("count"), one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            Iterator<IntWritable> iterator = values.iterator();

            while (iterator.hasNext())
                sum += iterator.next().get();
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MongoCount(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            logger.error("Expects arguments: <path>, <db>, <collection>");
            logger.error("    path: HDFS path to directory holding Mongo files");
            logger.error("    db: name of database, this will search for <path>/<db>.ns");
            logger.error("    collection: name of collection, this should be in the DB <db>");
        }
        Configuration conf = getConf();

        Path path = new Path(args[0]);
        String dbname = args[1];
        String collname = args[2];

        MongoInputFormat.setMongoDirectory(path);
        MongoInputFormat.setDatabase(dbname);
        MongoInputFormat.setCollection(collname);

        Job job = new Job(conf);

        job.setJarByClass(MongoCount.class);
        job.setJobName("Count Mongo collection");
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(MongoInputFormat.class);

        Path outPath = new Path("/tmp/mongocount");
        TextOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath))
            fs.delete(outPath, true);

        logger.info("Starting job...");

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
