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

package org.apache.flink.state.remote.rocksdb.fs.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * An object that can be reference counted.
 */
public abstract class ReferenceCounted {

    private static final Logger LOG = LoggerFactory.getLogger(ReferenceCounted.class);

	private static final Unsafe unsafe;
	private static final long referenceOffset;

	static {
		try {
			unsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    try {
                        Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);
                        return f.get(null);
                    } catch (Throwable e) {
                        LOG.warn("sun.misc.Unsafe is not accessible", e);
                    }
                    return null;
                }
            });
			referenceOffset = unsafe.objectFieldOffset
				(ReferenceCounted.class.getDeclaredField("referenceCount"));
		} catch (Exception ex) {
			throw new Error(ex);
		}
	}

	private volatile int referenceCount;

	public ReferenceCounted(int initReference) {
		this.referenceCount = initReference;
	}

	public int retain() {
		return unsafe.getAndAddInt(this, referenceOffset, 1) + 1;
	}

	/**
	 * Try to retain this object. Fail if reference count is already zero.
	 * @return zero if failed, otherwise current reference count.
	 */
	public int tryRetain() {
		int v;
		do {
			v = unsafe.getIntVolatile(this, referenceOffset);
		} while (v != 0 && !unsafe.compareAndSwapInt(this, referenceOffset, v, v + 1));
		return v == 0 ? 0 : v + 1;
	}

	public int release() {
		int r = unsafe.getAndAddInt(this, referenceOffset, -1) - 1;
		if (r == 0) {
			referenceCountReachedZero();
		}
		return r;
	}

	public int getReferenceCount() {
		return referenceCount;
	}

	/**
	 * A method called when the reference count reaches zero.
	 */
	protected abstract void referenceCountReachedZero();
}
