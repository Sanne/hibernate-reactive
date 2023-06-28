/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.reactive.logging.impl.Log;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

public class StateTrackerUtil {

	private static final Log LOG = make( Log.class, lookup() );

	private static final AtomicInteger c = new AtomicInteger();
	private static final ArrayList<State> processedStates = new ArrayList<>( 2_000 );

	public static State injectTracker(MethodHandles.Lookup lookup) {
		final String className = lookup.lookupClass().getName();
		State s = new State( c.incrementAndGet(), className );
		synchronized ( processedStates ) {
			processedStates.add( s );
		}
		return s;
	}

	public static final void dumpSuspectStates(long timerId) {
		synchronized ( processedStates ) {
			final int size = processedStates.size();
			int okInstances = 0;
			for ( State p : processedStates ) {
				if ( !p.ended ) {
					LOG.errorf( "Context '%s' %d: not completed. %s", p.className, p.contextId, p.toString() );
				}
				else {
					okInstances++;
				}
			}
			final int failures = size - okInstances;
			if ( failures == 0 ) {
				LOG.errorf( "No problems detected among %d instances", size );
			}
			else {
				LOG.errorf( "Problems detected: %d instances out of a total of %d instances failed", failures, size );
			}
		}
	}

	public static State disabledTracker(MethodHandles.Lookup lookup) {
		return new State( 0, "disabled" );
	}

	public static class State {
		final int contextId;
		private final String className;
		final List<String> checkpoints = new ArrayList<>();
		private volatile boolean ended = false;

		public State(int contextId, String className) {
			this.contextId = contextId;
			this.className = className;
		}

		public void end() {
			ended = true;
		}

		public void end(Void ignored) {
			end();
		}

		@Override
		public String toString() {
			return className + "{" +
					"contextId=" + contextId +
					", checkpoints=" + checkpoints +
					'}';
		}

		public void reached(String label) {
			checkpoints.add( label );
		}

		public <T> T reached(T value, String label) {
			checkpoints.add( label );
			return value;
		}
	}
}
