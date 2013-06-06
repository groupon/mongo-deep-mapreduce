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

import com.groupon.mapreduce.mongo.out.MongoInsertMapOutputFormat;
import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a helper job that reads a Hive table and inserts the contents into a Mongo collection.
 * It expects the hive host, mongo host, and hive table as arguments. An example of this job using the run
 * script is:
 * ./run com.groupon.mapreduce.mongo.HiveToMongo hadoophost.com hivehost.com mongohost.com tablename
 */
public class HiveToMongo extends Configured implements Tool {
    final Logger logger = LoggerFactory.getLogger(HiveToMongo.class);

    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
        private static List<String[]> meta;

        /**
         * Fetch the table metadata using a SQL DESCRIBE statement through JDBC. This is run once in each Mapper.
         * @param conf Hive config to get the correct table.
         */
        private static void fetchMetaData(Configuration conf) {
            try {
                Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                Connection conn = DriverManager.getConnection(
                        "jdbc:hive://" + conf.get("hiveHost") + ":10000/default", "", "");

                Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery("describe " + conf.get("tableName"));

                meta = new ArrayList<String[]>();
                
                while (res.next())
                  meta.add(new String[] { res.getString(1), res.getString(2) });
            }
            catch (Exception e) {
                throw new RuntimeException("Could not fetch Hive metadata", e);
            }
        }

        private static final String arrayRegex = "array<([a-z]+)>";
        private static final String mapRegex = "map<([a-z]+),([a-z]+)>";

        /**
         * Map from the Hive type system to Hadoop Writable types.
         * @param type String representing a Hive type.
         * @return Class representing the corresponding Writable type.
         */
        private Class<? extends Writable> getWritableType(String type) {
            if (type.equals("string"))
                return Text.class;
            if (type.equals("int"))
                return IntWritable.class;
            if (type.equals("float"))
                return FloatWritable.class;
            if (type.equals("double"))
                return DoubleWritable.class;
            if (type.equals("bigint"))
                return LongWritable.class;
            if (type.matches(arrayRegex))
                return ArrayWritable.class;
            if (type.matches(mapRegex))
                return MapWritable.class;
            throw new RuntimeException("could not get writable type for: " + type);
        }

        /**
         * Cast a variable from a String we get from Hive for all data, to a Writable Hadoop type.
         * @param field Content of the variable, always a String in Hive results.
         * @param type The type of the variable, pulled from the Hive table metadata.
         * @return Field cast to a Writable variable.
         */
        private Writable cast(String field, String type) {
            Class<? extends Writable> writableType = getWritableType(type);

            if (field.equals("\\N"))
                return NullWritable.get();

            if (writableType.equals(Text.class))
                return new Text(field);

            if (writableType.equals(IntWritable.class))
                return new IntWritable(Integer.parseInt(field));

            if (writableType.equals(FloatWritable.class))
                return new FloatWritable(Float.parseFloat(field));

            if (writableType.equals(DoubleWritable.class))
                return new DoubleWritable(Double.parseDouble(field));

            if (writableType.equals(LongWritable.class))
                return new LongWritable(Long.parseLong(field));

            if (writableType.equals(ArrayWritable.class)) {
                Matcher matcher = Pattern.compile(arrayRegex).matcher(type);
                matcher.matches();
                String arrayType = matcher.group(1);

                String[] values = field.split("\02"); // This is the ASCII field separator character
                Writable[] writableValues = new Writable[values.length];

                for (int i = 0; i < values.length; i++)
                    writableValues[i] = cast(values[i], arrayType);

                return new ArrayWritable(writableType, writableValues);
            }

            // TODO implement correct handling of Hive Maps
            if (writableType.equals(ArrayWritable.class)) {
                System.err.println("Hive maps not yet implemented");
                return NullWritable.get();
            }

            throw new RuntimeException("Could not handle column type: " + type);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (meta == null)
                fetchMetaData(context.getConfiguration());

            String[] fields = value.toString().split("\01");

            MapWritable document = new MapWritable();

            for (int i = 0; i < fields.length; i++) {
                String type;
                Text colName;

                try {
                    colName = new Text(meta.get(i)[0]);
                    type = meta.get(i)[1];

                    document.put(colName, cast(fields[i], type));
                }
                catch (Exception e) {
                    throw new RuntimeException("Could not handle column " + i, e);
                }
            }

            context.write(null, document);
            context.progress();
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new HiveToMongo(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            logger.error("requires hive host, mongo host, and hive table args");
            return 1;
        }

        String tmpDir = "/tmp/hive-mongoexport";
        String hiveHost = args[0];
        String mongoHost = args[1];
        String tableName = args[2];

        logger.info("hive host: " + hiveHost);
        logger.info("mongo host: " + mongoHost);
        logger.info("table name: " + tableName);

        logger.info("removing existing mongo collection...");
        Mongo mongo = new Mongo(mongoHost, 27017);
        DB db = mongo.getDB("hive");
        DBCollection coll = db.getCollection(tableName);
        // we call a remove rather than a drop, because in general its unsafe to drop a sharded collection
        coll.remove(new BasicDBObject(), new WriteConcern(1));

        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive://" + hiveHost + ":10000/default", "", "");

        Statement stmt = conn.createStatement();
        logger.info("describe " + tableName);
        ResultSet res = stmt.executeQuery("describe " + tableName);

        while (res.next())
            logger.info(res.getString(1) + "  " + res.getString(2));

        // these are necessary in the Hive shell if dumping an HBase/Hive table
        stmt.executeQuery("add jar /usr/local/lib/hive/lib/hbase-0.92.1-cdh4.0.1.jar");
        stmt.executeQuery("add jar /usr/local/lib/hive/lib/hive-hbase-handler-0.8.1-cdh4.0.1.jar");
        stmt.executeQuery("add jar /usr/local/lib/hive/lib/zookeeper-3.4.3-cdh4.0.1.jar");

        // we first write out the hive table to HDFS, so that we can form InputSplits over it
        logger.info("Dumping Hive table...");
        stmt.executeQuery("insert overwrite directory '" + tmpDir + "' " +
                "select * from " + tableName);
        logger.info("Dumped Hive table");

        logger.info("Running job HiveToMongo");
        logger.info("Using input path " + tmpDir);

        Configuration conf = getConf();
        conf.set("hiveHost", hiveHost);
        conf.set("tableName", tableName);

        // output to a Mongo host in DB 'hive' with the Hive table name
        MongoInsertMapOutputFormat.setMongoConf(conf, mongoHost, 27017, "hive", tableName);
        Job job = new Job(conf);

        job.setNumReduceTasks(0);
        job.setJarByClass(HiveToMongo.class);
        job.setJobName("HiveToMongo");
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setOutputFormatClass(MongoInsertMapOutputFormat.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        TextInputFormat.addInputPath(job, new Path(tmpDir));

        logger.info("Starting job...");

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
