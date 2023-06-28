/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.event.impl;

import java.util.ArrayList;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.FlushEvent;
import org.hibernate.event.spi.FlushEventListener;
import org.hibernate.reactive.event.ReactiveFlushEventListener;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.stat.spi.StatisticsImplementor;

import static java.lang.invoke.MethodHandles.lookup;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultFlushEventListener}.
 */
public class DefaultReactiveFlushEventListener extends AbstractReactiveFlushingEventListener
		implements ReactiveFlushEventListener, FlushEventListener {

	private static final Log LOG = make( Log.class, lookup() );

	static final AtomicInteger c = new AtomicInteger();
	static final ArrayList<State> processedStates = new ArrayList<>( 2_000 );

	@Override
	public CompletionStage<Void> reactiveOnFlush(FlushEvent event) throws HibernateException {
		State s = new State();
		storeState( s );
		final EventSource source = event.getSession();
		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();

		// State{flushBegin=true, flushEnd=false, postFlushStart=true, flushedEverythingToExecutions=true, performedExecutions=false, postFlushExecuted=false}
		if ( persistenceContext.getNumberOfManagedEntities() > 0
				|| persistenceContext.getCollectionEntriesSize() > 0 ) {
			s.flushStart();
			source.getEventListenerManager().flushStart();
			s.postFlushStart();

			return flushEverythingToExecutions( event )
					.thenAccept( v -> s.flushedEverythingToExecutions() ) // true
					.thenCompose( v -> performExecutions( source ) )
					.thenAccept( v -> s.performedExecutions() )//false
					.thenRun( () -> postFlush( source ) )
					.thenAccept( v -> s.postFlushExecuted() )
					.whenComplete( (v, x) -> source
							.getEventListenerManager()
							.flushEnd( event.getNumberOfEntitiesProcessed(), event.getNumberOfCollectionsProcessed() ) )
					.thenRun( () -> {
						postPostFlush( source );
						s.postPostFlush();

						final StatisticsImplementor statistics = source.getFactory().getStatistics();
						if ( statistics.isStatisticsEnabled() ) {
							statistics.flush();
						}
					} );
		}
		else if ( ((ReactiveSession) source).getReactiveActionQueue().hasAnyQueuedActions() ) {
			throw new IllegalStateException("Unexpected A");
			// execute any queued unloaded-entity deletions
//			return performExecutions( source );
		}
		else {
			throw new IllegalStateException("Unexpected B");
//			return voidFuture();
		}
	}

	private void storeState(State s) {
		synchronized ( processedStates ) {
			processedStates.add( s );
		}
	}

	@Override
	public void onFlush(FlushEvent event) throws HibernateException {
		throw LOG.nonReactiveMethodCall( "reactiveOnFlush" );
	}

	public static final void dumpSuspectStates(long timerId) {
		synchronized ( processedStates ) {
			final int size = processedStates.size();
			int okInstances = 0;
			for ( State p : processedStates ) {
				if ( p.flushBegin && (!p.flushEnd) ) {
					LOG.error( "Flush begun but not completed: " + p.contextId + " "  + p.toString() );
				}
				else {
					okInstances++;
				}
			}
			LOG.error( "No problems detected for N instances: " + okInstances + " out of a total of " + size + " instances" );
		}
	}

	private static final class State {
		private volatile boolean flushBegin = false;
		private volatile boolean flushEnd = false;
		final int contextId;
		private volatile boolean postFlushStart = false;
		private volatile boolean flushedEverythingToExecutions = false;
		private volatile boolean performedExecutions = false;
		private volatile boolean postFlushExecuted = false;

		private State() {
			this.contextId = c.incrementAndGet();
		}

		public void flushStart() {
			//LOG.info( "Session open, objects loaded and marked dirty "  + contextId );
			flushBegin = true;
		}

		public void postPostFlush() {
			//LOG.info( "Session closed " + contextId );
			flushEnd = true;
		}


		public void postFlushStart() {
			this.postFlushStart = true;
		}

		public void flushedEverythingToExecutions() {
			this.flushedEverythingToExecutions = true;
		}

		public void performedExecutions() {
			this.performedExecutions = true;
		}

		public void postFlushExecuted() {
			this.postFlushExecuted = true;
		}

		@Override
		public String toString() {
			return "State{" +
					"flushBegin=" + flushBegin +
					", flushEnd=" + flushEnd +
					", contextId=" + contextId +
					", postFlushStart=" + postFlushStart +
					", flushedEverythingToExecutions=" + flushedEverythingToExecutions +
					", performedExecutions=" + performedExecutions +
					", postFlushExecuted=" + postFlushExecuted +
					'}';
		}
	}
}
