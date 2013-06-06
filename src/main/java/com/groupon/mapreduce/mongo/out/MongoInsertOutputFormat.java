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

import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This is an abstract OutputFormat that can be implemented depending on the expected output data type. Handling
 * of getRecordWriter(), which returns a RecordWriter relevant to this type, is up to classes that extend this.
 * This OutputFormat expects the following configuration variables in Hadoop:
 * - MongoInsertOutputFormat.MONGO_HOST       - Mongo host on which to insert
 * - MongoInsertOutputFormat.MONGO_PORT       - Mongo port on which to insert
 * - MongoInsertOutputFormat.MONGO_DB_NAME    - DB name in which to insert
 * - MongoInsertOutputFormat.MONGO_COLL_NAME  - Collection name in which to insert
 *
 * This parameter is optional:
 * - MongoInsertOutputFormat.MONGO_UPSERT     - 'true' or 'false' depending on if Mongo upserts should be allowed
 *                                              defaults to false
 *
 * @param <T> Output data type to expect.
 */
abstract class MongoInsertOutputFormat<T> extends OutputFormat<Text, T> {
    protected static DBCollection mongoColl = null;
    public static final String MONGO_HOST = "mongoHost";
    public static final String MONGO_PORT = "mongoPort";
    public static final String MONGO_DB_NAME = "mongoDb";
    public static final String MONGO_COLL_NAME = "mongoColl";
    public static final String MONGO_UPSERT = "mongoUpsert";

    /**
     * Configure Mongo with the given config and open a connection, storing the DBCollection object statically.
     * @param conf Hadoop configuration object.
     */
    protected static void setMongoParams(Configuration conf) {
        String host = conf.get(MONGO_HOST);
        String db = conf.get(MONGO_DB_NAME);
        String collection = conf.get(MONGO_COLL_NAME);
        int port = Integer.parseInt(conf.get(MONGO_PORT));

        try {
            MongoOptions options = new MongoOptions();
            options.autoConnectRetry = true;
            options.connectTimeout = 10000; // timeout is 10s
            options.connectionsPerHost = 50; // default is 10
            options.threadsAllowedToBlockForConnectionMultiplier = 500; // default is 5
            options.fsync = false; // don't wait for disk flush
            options.w = 1; // ensure write to memory succeeded

            Mongo mongo = new Mongo(new ServerAddress(host, port), options);
            mongoColl = mongo.getDB(db).getCollection(collection);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not connect to mongo host " + host + ":" + port, e);
        }
    }

    /**
     * Helper to configure Hadoop with the given Mongo parameters.
     * @param conf Configuration object in which to set Mongo config.
     * @param host Mongo Host.
     * @param port Mongo port.
     * @param db Mongo database name.
     * @param coll Mongo collection name.
     */
    public static void setMongoConf(Configuration conf, String host, Integer port, String db, String coll) {
        conf.set(MONGO_HOST, host);
        conf.set(MONGO_PORT, port.toString());
        conf.set(MONGO_DB_NAME, db);
        conf.set(MONGO_COLL_NAME, coll);
        if (conf.get(MONGO_UPSERT) == null)
            setDoUpsert(conf, false);
    }

    /**
     * Helper to configure Hadoop with the given MONGO_UPSERT value.
     * @param conf Configuration object in which to set Mongo config.
     * @param doUpsert True if upsert is allowed.
     */
    public static void setDoUpsert(Configuration conf, boolean doUpsert) {
        if (doUpsert)
            conf.set(MONGO_UPSERT, "true");
        else
            conf.set(MONGO_UPSERT, "false");
    }

    /**
     * Look for upsert parameter in config, defaulting to false.
     * @param context Context with which we get config.
     * @return True if upserts are allowed.
     */
    protected static boolean getDoUpsert(TaskAttemptContext context) {
        String doUpsertString = context.getConfiguration().get(MONGO_UPSERT);
        if (doUpsertString != null && doUpsertString.equals("true"))
            return true;
        return false;
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext)
            throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new MongoInsertOutputCommitter();
    }
}
