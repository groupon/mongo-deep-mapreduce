Mongo Deep MapReduce
====================

This is a library of MongoDB related Hadoop MapReduce classes, in particular an InputFormat that reads directly
from Mongo's binary on-disk format. Developed by Peter Bakkum at Groupon in Palo Alto.

Problem: If you want to use Hadoop MapReduce with a Mongo collection you currently have two options:
- You can execute one or more cursors over the entire cluster in your MapReduce job.
- You can export the collection as BSON or JSON, which also executes a cursor over the entire collection,
and MapReduce over the exported data.

However, with a large data set that significantly exceeds the available memory on the Mongo host, these options
can both be prohibitively time consuming.

Solution: Move the raw Mongo files into HDFS, without exporting, and MapReduce over them using this library.

Mongo uses a proprietary binary format to manage its data, which is essentially a doubly-linked list of BSON records.
By reading this format directly, we obviate the need for expensive data conversion prior to a Hadoop MapReduce,
and we can utilize the full throughput of the Hadoop cluster when reading the data, rather than using single-threaded
cursors.

Data Format
-----------

Data stored on disk by Mongo is generally in groups of files that look like

```
dbname.ns
dbname.0
dbname.1
dbname.2
...
```

`dbname.ns` is a namespace file. This is a hash table of namespace records, which contain a collection name and
the first and last Extent of the collection. We use DiskLocs as pointers to Extents. A DiskLoc is essentially

```C
struct DiskLoc {
    int fileNum;
    int offset;
}
```

written out to disk. The fileNum is the postfix number on the files shown above, and the offset is the byte offset
within that file.

An extent is the major unit of physical organization within a Mongo collection. A collection is a doubly-linked list
of extents, that each hold a block of records within them. The extents are spread across the database files and
each contains a doubly-linked list of Records.

Using MongoInputFormat
----------------------

This has been written using the newer `mapreduce` interface and CDH4.0.1 and tested against the binary data formats
from Mongo 2.0 and 2.2. It should work out of the box with those
systems but may require some tweaking of the dependencies to work on different versions of Hadoop, or be changed
for future versions of Mongo with different on-disk formats. Once included as a
dependency, you can use this library as you would any other Hadoop InputFormat by configuring it to point to
the Mongo data in HDFS and the Mongo database and collection you want to query.

Basic use looks like:

```Java
MongoInputFormat.setMongoDirectory(path);
MongoInputFormat.setDatabase(dbname);
MongoInputFormat.setCollection(collname);

job.setInputFormatClass(MongoInputFormat.class);
```

You can then implement a Mapper like:

```Java
public static class Map extends Mapper<Text, WritableBSONObject, Text, Text> {
    @Override
    public void map(Text key, WritableBSONObject value, Context context)
            throws IOException, InterruptedException {

        context.write(null, new Text(value.getBSONObject().toString()));
    }
}
```

Look at the provided [MongoToJson](src/main/java/com/groupon/mapreduce/mongo/MongoToJson.java) job for a full example.

Running the Tests
-----------------

To run the tests you must first generate a set of test database files.

- Start a local Mongo instance at port 27017
- Run the following to build and insert specific data used for testing

```
mvn -Dmaven.test.skip=true clean package
mvn test-compile
java -cp target/test-classes/:target/mongo-deep-mapreduce-jar-with-dependencies.jar com.groupon.mapreduce.mongo.GenerateTestDB
```

- Then copy the deepmr_test* files from wherever your Mongo instance keeps its data (often in /data) to src/test/db
- Now you can run `mvn test`
