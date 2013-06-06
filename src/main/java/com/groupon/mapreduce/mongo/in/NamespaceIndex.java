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

import com.groupon.mapreduce.mongo.JobUtil;
import com.groupon.mapreduce.mongo.in.DiskLoc;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is used to manage the namespace file associated with a Mongo database. The file is a hash table written
 * directly to disk, so collections are discovered by iterating over the entire file and searching for values
 * written to the table. Once found, this information is used to construct a Namespace object, which has information
 * about the extents in a given collection.
 */
class NamespaceIndex {
    private Map<String, Namespace> namespaces = new HashMap<String, Namespace>();
    private static final int NODE_SIZE = 628;

    public NamespaceIndex(FileSystem fileSystem, Path path) throws FileNotFoundException {
        byte[] content;

        try {
            content = readFile(fileSystem, path);
        }
        catch(IOException e) {
            throw new FileNotFoundException("Could not open Mongo Namespace file: " + path.toString());
        }

        for (int i = 0; i < content.length; i += NODE_SIZE) {
            int hash = JobUtil.readInt(content, i);
            if (hash == 0)
                continue;

            String name = new String(content, i + 4, 128).trim();

            DiskLoc firstExtent = new DiskLoc(JobUtil.readInt(content, i + 132), JobUtil.readInt(content, i + 136));
            DiskLoc lastExtent  = new DiskLoc(JobUtil.readInt(content, i + 140), JobUtil.readInt(content, i + 144));
            namespaces.put(name, new Namespace(fileSystem, path.getParent(), name, firstExtent, lastExtent));
        }
    }

    public Namespace getNamespace(String name) {
        return namespaces.get(name);
    }

    private static byte[] readFile (FileSystem fileSystem, Path file) throws IOException {
        FSDataInputStream stream = fileSystem.open(file);

        try {
            long longlength = fileSystem.getFileStatus(file).getLen();
            int length = (int) longlength;
            if (length != longlength) throw new IOException("File size >= 2 GB");

            byte[] data = new byte[length];
            stream.readFully(data);
            return data;
        }
        finally {
            stream.close();
        }
    }

}
