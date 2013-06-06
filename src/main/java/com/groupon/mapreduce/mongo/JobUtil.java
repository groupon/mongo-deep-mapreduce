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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is a class of static utility functions which can be accessed in a MapReduce job.
 */
public class JobUtil {
    /**
     * Given a Map, fetch fields with a JSONPath-like dot syntax. Example: JobUtil.get(map, "key1.key2")
     * @param map Map from which to fetch values.
     * @param path JSONPath-like dot-delimited path within the map.
     * @return Values found, this is always a list because if the path includes an array, then the remaining
     * path is applied to each element of the array, thus there is the potential for multiple results.
     */
    public static List get(Map map, String path) {
        String[] fields = path.split("\\.");
        List found = new ArrayList();
        get(map, fields, 0, found);
        return found;
    }

    private static void get(Map map, String[] fields, int index, List found) {
        Object match = map.get(fields[index]);

        if (index == fields.length - 1 && match != null) {
            if (match instanceof List)
                found.addAll((List) match);
            else
                found.add(match);
        }
        else if (match instanceof Map)
            get((Map) match, fields, index + 1, found);
        else if (match instanceof List)
            get((List) match, fields, index + 1, found);
    }

    private static void get(List list, String[] fields, int index, List found) {
        for (Object match : list) {
            if (match instanceof Map)
                get((Map) match, fields, index, found);
            else if (match instanceof List)
                get((List) match, fields, index, found);
        }
    }

    /**
     * Write a String to a DataOutput.
     * @param s
     * @param output
     * @throws IOException
     */
    public static void writeString(String s, DataOutput output) throws IOException {
        output.writeInt(s.length());
        output.writeChars(s);
    }

    /**
     * Read a String from a DataInput
     * @param input
     * @return
     * @throws IOException
     */
    public static String readString(DataInput input) throws IOException {
        int length = input.readInt();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = input.readChar();

        return new String(chars);
    }

    /**
     * Read an int out of a byte array at the given offset
     * @param content
     * @param offset
     * @return
     */
    public static int readInt(byte[] content, int offset) {
        int x;
        x  = (content[offset + 3] & 0xFF) << 24;
        x |= (content[offset + 2] & 0xFF) << 16;
        x |= (content[offset + 1] & 0xFF) << 8;
        x |= (content[offset] & 0xFF);
        return x;
    }
}
