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

package org.apache.flink.state.forst.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;

import org.apache.flink.state.forst.ForStKeyedStateBackend;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.List;

@Internal
public class ForStValueState<K, N, V>
        extends AbstractForStState<K, N, V> implements RemoteValueState<K, N, V> {

    public ForStValueState(
            ForStKeyedStateBackend<?, K> backend,
            ColumnFamilyHandle columnFamily,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue) {
        super(
                backend,
                columnFamily,
                keySerializer,
                namespaceSerializer,
                valueSerializer,
                defaultValue);
    }

    @Override
    public V get(K key) throws IOException {
        try {
            byte[] valueBytes = db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace(key));
            if (valueBytes == null) {
                return getDefaultValue();
            }
            DataInputDeserializer deserializeView = dataInputView.get();
            deserializeView.setBuffer(valueBytes);
            return valueSerializer.deserialize(deserializeView);
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<V> multiGet(List<K> keys) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(K key, V value) throws IOException {
        try {
            db.put(
                    columnFamily,
                    writeOptions,
                    serializeCurrentKeyWithGroupAndNamespace(key),
                    serializeValue(value));
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
