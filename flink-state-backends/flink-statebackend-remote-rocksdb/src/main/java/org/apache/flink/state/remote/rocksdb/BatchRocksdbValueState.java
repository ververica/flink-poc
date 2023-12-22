/*
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

package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.api.common.state.batch.CommittedValue;
import org.apache.flink.api.common.state.batch.CommittedValue.CommittedValueType;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.internal.batch.InternalBatchValueState;

import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Iterator;

/**
 * BatchRocksdbValueState.
 */
public class BatchRocksdbValueState<K, N, V> extends AbstractBatchRocksdbState<K, N, V>
        implements InternalBatchValueState<K, N, V> {

    @Override
    public Iterable<V> values() throws IOException {
        return parallelIOExecutor.fetchValues(key -> {
            byte[] valueBytes = db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace(key));
            if (valueBytes == null) {
                return getDefaultValue();
            }
            DataInputDeserializer deserializeView = dataInputView.get();
            deserializeView.setBuffer(valueBytes);
            return valueSerializer.deserialize(deserializeView);
        });
    }

    @Override
    public void update(Iterable<CommittedValue<V>> values) throws IOException {
        try {
            Iterator<K> keyIter = backend.getCurrentKeys().iterator();
            Iterator<CommittedValue<V>> valueIter = values.iterator();
            while (valueIter.hasNext()) {
                K key = keyIter.next();
                CommittedValue<V> committedValue = valueIter.next();
                CommittedValueType type = committedValue.getValueType();
                if (type == CommittedValueType.UNMODIFIED) {
                    continue;
                }
                if (type == CommittedValueType.UPDATE) {
                    db.put(
                        columnFamily,
                        writeOptions,
                        serializeCurrentKeyWithGroupAndNamespace(key),
                        serializeValue(committedValue.getValue()));
                    continue;
                }
                if (type == CommittedValueType.DELETE) {
                    db.delete(columnFamily, writeOptions, serializeCurrentKeyWithGroupAndNamespace(key));
                }
            }
        } catch (RocksDBException e) {
            throw new IOException("Error while adding data to RocksDB", e);
        }

    }

    @Override
    public V value() throws IOException {
        try {
            byte[] valueBytes =
                    db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace(backend.getCurrentKey()));

            if (valueBytes == null) {
                return getDefaultValue();
            }
            DataInputDeserializer deserializeView = dataInputView.get();
            deserializeView.setBuffer(valueBytes);
            return valueSerializer.deserialize(deserializeView);
        } catch (RocksDBException e) {
            throw new IOException("Error while retrieving data from RocksDB.", e);
        }
    }

    @Override
    public void update(V value) throws IOException {
        if (value == null) {
            clear();
            return;
        }

        try {
            db.put(
                    columnFamily,
                    writeOptions,
                    serializeCurrentKeyWithGroupAndNamespace(backend.getCurrentKey()),
                    serializeValue(value));
        } catch (RocksDBException e) {
            throw new IOException("Error while adding data to RocksDB", e);
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TypeSerializer getKeySerializer() {
        throw new UnsupportedOperationException();
    }
}
