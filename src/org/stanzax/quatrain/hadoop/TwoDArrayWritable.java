/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.stanzax.quatrain.hadoop;

import org.stanzax.quatrain.io.Writable;

/** A Writable for 2D arrays containing a matrix of instances of a class. */
public class TwoDArrayWritable extends org.apache.hadoop.io.TwoDArrayWritable
        implements Writable {

    public TwoDArrayWritable(Class<?> valueClass) {
        super(valueClass);
    }

    public TwoDArrayWritable(Class<?> valueClass,
            org.apache.hadoop.io.Writable[][] values) {
        super(valueClass, values);
    }

    @Override
    public Object getValue() {
        return get();
    }
}
