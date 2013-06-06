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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Iterator;

/**
 * This class represents the namespace information for a collection found in the Mongo database namespace index file.
 * These are pulled out with a NamespaceIndex file and essentially just contain pointers to the first and last
 * extents in a collection. This provides some capability to iterate over these extents.
 *
 */
class Namespace implements Iterable<Record> {
    FileSystem fileSystem;
    Path directory;
    String name;
    DiskLoc firstExtent;
    DiskLoc lastExtent;

    public Namespace(FileSystem fileSystem, Path directory, String name, DiskLoc firstExtent, DiskLoc lastExtent) {
        this.fileSystem = fileSystem;
        this.directory = directory;
        this.name = name; // this is the full dbname.collectionname
        this.firstExtent = firstExtent;
        this.lastExtent = lastExtent;
    }

    public Extent getFirstExtent() {
        return new Extent(this, firstExtent);
    }

    public Extent getNextExtent(Extent e) {
        DiskLoc next = e.getNextExtent();
        if (next.getFileNum() == -1)
            return null;
        return new Extent(this, next);
    }

    private String dbName() {
        return name.split("\\.")[0];
    }

    public Path getPath(int fileNum) {
        return new Path(directory, dbName() + "." + fileNum);
    }

    public FSDataInputStream getStream(int fileNum) {
        Path path = getPath(fileNum);
        try {
            return fileSystem.open(getPath(fileNum));
        }
        catch (Exception e) {
            throw new RuntimeException("Could not open file " + path.toString());
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return new Iterator<Record>() {
            Extent currentExtent = null;
            Iterator<Record> extentIterator = null;

            private void advance() {
                if (currentExtent == null) {
                    currentExtent = getFirstExtent();
                    extentIterator = currentExtent.iterator(fileSystem);
                }

                while (!extentIterator.hasNext()) {
                    if (!currentExtent.hasNextExtent())
                        return;

                    currentExtent = getNextExtent(currentExtent);
                    extentIterator = currentExtent.iterator(fileSystem);
                }
            }

            @Override
            public boolean hasNext() {
                advance();
                return extentIterator.hasNext();
            }

            @Override
            public Record next() {
                advance();
                return extentIterator.next();
            }

            @Override
            public void remove() {
                throw new RuntimeException("Tried to remove record, not implemented");
            }
        };
    }

    public Iterator<Extent> extentIterator() {
        return new Iterator<Extent>() {
            private Extent current = getFirstExtent();

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Extent next() {
                if (!hasNext())
                    return null;

                Extent old = current;
                current = getNextExtent(current);
                return old;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Tried to remove extent, not implemented");
            }
        };
    }
}
