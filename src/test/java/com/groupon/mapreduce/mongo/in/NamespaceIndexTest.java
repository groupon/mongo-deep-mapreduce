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

import com.groupon.mapreduce.mongo.in.Extent;
import com.groupon.mapreduce.mongo.in.Namespace;
import com.groupon.mapreduce.mongo.in.NamespaceIndex;
import com.groupon.mapreduce.mongo.in.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Iterator;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NamespaceIndexTest {
    public static final String DB_FILE = "src/test/db/deepmr_test.ns";

    public static FileSystem getFilesystem() {
        FileSystem fs = new LocalFileSystem();
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        try {
            fs.initialize(new URI("file:///"), conf);
        } catch (Exception e) {
            throw new RuntimeException("Could not get namespace index", e);
        }

        return fs;
    }

    public static NamespaceIndex getNamespaceIndex() throws FileNotFoundException {
        Path path = new Path(DB_FILE);
        return new NamespaceIndex(getFilesystem(), path);
    }

    @Test
    public void testScan() throws Exception {
        NamespaceIndex nsi = getNamespaceIndex();

        Namespace ns = nsi.getNamespace("deepmr_test.testcoll1");
        assertNotNull(ns);

        Iterator<Record> i = ns.iterator();

        while (i.hasNext()) {
            Record r = i.next();
            assertNotNull(r);
        }

        ns = nsi.getNamespace("deepmr_test.testcoll2");
        assertNotNull(ns);

        i = ns.iterator();

        while (i.hasNext()) {
            Record r = i.next();
            assertNotNull(r);
        }
    }

    @Test
    public void testExtentIterator() throws FileNotFoundException {
        NamespaceIndex nsi = getNamespaceIndex();
        Namespace namespace = nsi.getNamespace("deepmr_test.testcoll1");
        FileSystem fs = getFilesystem();

        for (Iterator<Extent> i = namespace.extentIterator(); i.hasNext(); ) {
            Extent extent = i.next();
            assertTrue(extent.getLength() > 0);

            for (Iterator<Record> recordi = extent.iterator(fs); recordi.hasNext(); ) {
                Record r = recordi.next();
                assertTrue(r.getContent(fs) != null);
            }
        }
    }
}
