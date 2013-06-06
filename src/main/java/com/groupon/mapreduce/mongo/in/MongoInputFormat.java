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

import com.groupon.mapreduce.mongo.WritableBSONObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This InputFormat reads Records from files in the Mongo on-disk binary format. It requires the location
 * in the FileSystem of these files, and a Mongo DB/Collection to look for in this directory. It expects to
 * see dbname.ns and dbname.0, with more data in dbname.i, where i is an integer increasing from 0. Using
 * these files, it creates InputSplits with Mongo Extents.
 */
public class MongoInputFormat extends InputFormat<Text, WritableBSONObject> {
    static final Logger logger = LoggerFactory.getLogger(MongoInputFormat.class);
    
    private static Path mongoDirectory;
    private static String database;
    private static String collection;

    /**
     * Set the directory to search for Mongo files, which should be in one or more directories within the
     * given directory. In other words for a Mongo DB at hdfs://host/mongo/host1/db.ns, pass in "/mongo/host1".
     * @param path_ Path given to MongoInputFormat to search for extents
     */
    public static void setMongoDirectory(Path path_) {
        logger.info("MongoInputFormat set mongoDirectory " + path_.toString());
        mongoDirectory = path_;
    }

    /**
     * Set the database to MapReduce over. For example, if hdfs://host/mongo/host1/db.ns exists, enter "db".
     * @param database_ Name of database over which you want to Map
     */
    public static void setDatabase(String database_) {
        logger.info("MongoInputFormat set database " + database_);
        database = database_;
    }

    /**
     * Collection over which you want to mapreduce. This should be a member of the database passed in
     * with setDatabase().
     * @param collection_ Name of the collection over which you want to Map
     */
    public static void setCollection(String collection_) {
        logger.info("MongoInputFormat set collection " + collection_);
        collection = collection_;
    }

    public MongoInputFormat() {
        logger.info("Constructing new MongoInputFormat");
    }

    public List<MongoInputSplit> getSplitsFromFile(FileSystem fs, Path path) {
        logger.info("Getting Mongo splits from file " + path.toString());

        List<MongoInputSplit> splits = new ArrayList<MongoInputSplit>();
        NamespaceIndex index;

        try {
            index = new NamespaceIndex(fs, path);
        }
        catch(FileNotFoundException e) {
            throw new RuntimeException("MongoInputSplit failed to find splits", e);
        }
        Namespace namespace = index.getNamespace(database + "." + collection);

        if (namespace == null)
            throw new RuntimeException("Could not find namespace " + collection);

        for (Iterator<Extent> i = namespace.extentIterator(); i.hasNext(); ) {
            Extent extent = i.next();
            logger.info("Found extent " + extent.getPath().toString() +
                    " offset " + extent.getOffset());
            splits.add(new MongoInputSplit(extent, fs));
        }

        return splits;
    }

    /**
     * This is required by InputFormat, and returns a list of InputSplits found by searching in the given
     * directory for Mongo data.
     * @param jobContext Context passed through when the job is run, useful for getting config.
     * @return List of InputSplits.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        logger.info("Getting Mongo splits");
        List<InputSplit> splits = new ArrayList<InputSplit>();

        FileSystem fs = mongoDirectory.getFileSystem(jobContext.getConfiguration());
        Path namespacePath = mongoDirectory.suffix("/" + database + ".ns");
        logger.info("searching for " + namespacePath.toString());

        if (!fs.exists(namespacePath))
            logger.info("Could not find Mongo DB at " + namespacePath.toString());
        else
            splits.addAll(getSplitsFromFile(fs, namespacePath));


        if (splits.size() == 0) {
            logger.info("Could not find any data in that Mongo collection");
            logger.info("There will be 0 input records");
        }

        return splits;
    }

    /**
     * This is required by InputFormat, and just makes a RecordReader, given an InputSplit.
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, WritableBSONObject> createRecordReader(InputSplit inputSplit,
                                                                     TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        MongoRecordReader reader = new MongoRecordReader();
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }

}