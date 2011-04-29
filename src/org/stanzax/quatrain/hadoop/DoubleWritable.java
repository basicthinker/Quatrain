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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.stanzax.quatrain.io.Writable;

/**
 * Writable for Double values.
 */
public class DoubleWritable extends org.apache.hadoop.io.DoubleWritable
        implements Writable {

    public DoubleWritable(double value) {
        super(value);
    }

    public DoubleWritable() {
        super();
    }

    @Override
    public Object getValue() {
        return super.get();
    }

    @Override
    public void setValue(Object value) {
        super.set((Double)value);
    }

    @Override
    public void readFields(DataInputStream in) throws IOException {
        super.readFields(in);
    }

    @Override
    public void write(DataOutputStream out) throws IOException {
        super.write(out);
    }

}
