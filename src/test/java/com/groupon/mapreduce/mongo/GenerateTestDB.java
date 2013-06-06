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

import com.mongodb.*;

public class GenerateTestDB {
    public static final int NUM_RECORDS = 100000;
    public static final String DB_NAME = "deepmr_test";
    public static final String COLL_NAME_1 = "testcoll1";
    public static final String COLL_NAME_2 = "testcoll2";

    public static void main(String[] args) throws Exception {
        Mongo mongo = new Mongo("localhost", 27017);
        DB db = mongo.getDB(DB_NAME);
        db.dropDatabase();
        DBCollection coll1 = db.getCollection(COLL_NAME_1);
        DBCollection coll2 = db.getCollection(COLL_NAME_2);

        for (int i = 0; i < NUM_RECORDS; i++) {
            final int j = i;

            DBObject doc = new BasicDBObject() {{
                put("key1", j);
                put("key2", j * 2);
                put("key3", String.valueOf(j));
                put("key4", new BasicDBObject() {{
                    put("key5", j);
                }});
                put("key6", new BasicDBList() {{
                    add("one");
                    add("two");
                    add("three");
                }});
            }};

            coll1.insert(doc);
            coll2.insert(doc);
        }
    }
}
