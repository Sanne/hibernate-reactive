/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A {@link ReactiveIdentifierGenerator} which uses the database to allocate
 * blocks of ids. A block is identified by its "hi" value (the first id in
 * the block). While a new block is being allocated, concurrent streams wait
 * without blocking.
 *
 * @author Gavin King
 */
public abstract class BlockingIdentifierGenerator implements ReactiveIdentifierGenerator<Long> {

	/**
	 * The block size (the number of "lo" values for each "hi" value)
	 */

	protected abstract int getBlockSize();

	/**
	 * Allocate a new block, by obtaining the next "hi" value from the database
	 */
	protected abstract CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session);

	private int loValue;
	private long hiValue;

//	private volatile List<Runnable> queue = null;

	protected synchronized long next() {
		return loValue > 0 && loValue < getBlockSize()
				? hiValue + loValue++
				: -1; //flag value indicating that we need to hit db
	}

	protected synchronized long next(long hi) {
		hiValue = hi;
		loValue = 1;
		return hi;
	}

	private static final AtomicLong al = new AtomicLong();

	@Override
	public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object entity) {
		return completedFuture(al.incrementAndGet());
	}

}
