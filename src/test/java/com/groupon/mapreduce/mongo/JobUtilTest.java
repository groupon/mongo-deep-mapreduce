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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JobUtilTest {
    private Map m = new HashMap() {{
        put("a", 1);
        put("b", 2);
        put("c", new HashMap() {{
            put("x", 10);
            put("y", 11);
        }});
        put("d", new ArrayList() {{
            add(0);
            add(new HashMap() {{
                put("x", 1);
                put("y", 2);
                put("z", new ArrayList() {{
                    add(4);
                    add(5);
                }});
            }});
            add(new HashMap() {{
                put("x", 3);
                put("y", 4);
                put("z", new ArrayList() {{
                    add(6);
                    add(7);
                }});
            }});
        }});
    }};

    @Test
    public void TestOneLevelGet() {
        List r = JobUtil.get(m, "a");
        assertEquals(r.size(), 1);
        assertEquals(r.get(0), 1);
    }

    @Test
    public void TestEmptyGet() {
        List r = JobUtil.get(m, "z");
        assertEquals(r.size(), 0);
    }

    @Test
    public void TestComplexEmptyGet() {
        List r = JobUtil.get(m, "d.y.foo");
        assertEquals(r.size(), 0);
    }

    @Test
    public void TestTwoLevelGet() {
        List r = JobUtil.get(m, "c.x");
        assertEquals(r.size(), 1);
    }

    @Test
    public void TestArrayGet() {
        List r = JobUtil.get(m, "d.x");
        assertEquals(r.size(), 2);
        assertEquals(r.get(0), 1);
        assertEquals(r.get(1), 3);
    }

    @Test
    public void TestGetList() {
        List r = JobUtil.get(m, "d.z");
        assertEquals(r.size(), 4);
        assertEquals(r.get(0), 4);
        assertEquals(r.get(3), 7);
    }
}
