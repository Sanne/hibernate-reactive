/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.verticle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.hibernate.reactive.it.verticle.utils.LocalRandom;
import org.hibernate.reactive.it.verticle.utils.Randomizer;
import org.hibernate.reactive.mutiny.Mutiny;

import org.jboss.logging.Logger;

import io.smallrye.mutiny.Uni;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import static io.vertx.core.CompositeFuture.all;
import static java.lang.Integer.parseInt;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

public class WorldVerticle extends AbstractVerticle {

	private static final Logger LOG = Logger.getLogger( WorldVerticle.class );

	private final Supplier<Mutiny.SessionFactory> emfSupplier;
	private Mutiny.SessionFactory emf;
	private HttpServer httpServer;

	static final AtomicInteger c = new AtomicInteger();
	static final ArrayList<State> processedStates = new ArrayList<>( 2_000 );

	/**
	 * The port to use to listen to requests
	 */
	public static final int HTTP_PORT = 8088;

	public WorldVerticle(Supplier<Mutiny.SessionFactory> emfSupplier) {
		this.emfSupplier = emfSupplier;
	}

	private void startHibernate(Promise<Object> p) {
		try {
			this.emf = emfSupplier.get();
			p.complete();
		}
		catch (Throwable t) {
			p.fail( t );
		}
	}

	@Override
	public void start(Promise<Void> startPromise) {
		final Future<Object> startHibernate = vertx.executeBlocking( this::startHibernate )
				.onSuccess( s -> LOG.infof( "✅ Hibernate Reactive is ready" ) );

		Router router = Router.router( vertx );
		BodyHandler bodyHandler = BodyHandler.create();
		router.post().handler( bodyHandler );

		router.get( "/worlds" ).respond( this::listWorlds );
		router.get( "/updates" ).respond( this::updateWorlds );
		router.get( "/createData" ).respond( this::createData );

		this.httpServer = vertx.createHttpServer();
		final Future<HttpServer> startHttpServer = httpServer
				.requestHandler( router )
				.listen( HTTP_PORT )
				.onSuccess( v -> LOG.infof( "✅ HTTP server listening on port %s", HTTP_PORT ) );

		all( startHibernate, startHttpServer )
				.onSuccess( s -> startPromise.complete() )
				.onFailure( startPromise::fail );
	}

	@Override
	public void stop(Promise<Void> stopPromise) {
		httpServer.close().onComplete( unused -> emf.close() )
				.onSuccess( s -> stopPromise.complete() )
				.onFailure( stopPromise::fail );
	}

	private Uni<List<World>> randomWorldsForWrite(Mutiny.Session session, int count) {
		final List<World> worlds = new ArrayList<>( count );
		//The rules require individual load: we can't use the Hibernate feature which allows load by multiple IDs
		// as one single operation as Hibernate is too smart and will switch to use batched loads.
		// But also, we can't use "Uni#join" as we did in the above method as managed entities shouldn't use pipelining -
		// so we also have to avoid Mutiny optimising things by establishing an explicit chain:
		final LocalRandom localRandom = Randomizer.current();
		Uni<Void> loopRoot = Uni.createFrom().voidItem();
		for ( int i = 0; i < count; i++ ) {
			loopRoot = loopRoot.call( () -> session
					.find( World.class, localRandom.getNextRandom() )
					.invoke( worlds::add ) );
		}
		return loopRoot.map( v -> worlds );
	}

	private Future<List<World>> listWorlds(RoutingContext ctx) {
		return toFuture( emf.withSession( session -> session
				.createQuery( "from World", World.class ).getResultList()
		) );
	}

	private int parseQueryCount(String textValue) {
		if ( textValue == null ) {
			return 1;
		}

		try {
			return Math.min( 500, Math.max( 1, parseInt( textValue ) ) );
		}
		catch (NumberFormatException e) {
			return 1;
		}
	}

	private Future<List<World>> updateWorlds(RoutingContext ctx) {
		String queries = ctx.request().getParam( "queries" );
		State s = new State();
		storeState( s );
		return toFuture( emf.withSession( session -> randomWorldsForWrite( session, parseQueryCount( queries ) )
				.flatMap( worldsCollection -> {
//					LOG.info( worldsCollection );
					final LocalRandom localRandom = Randomizer.current();
					worldsCollection.forEach( w -> {
						//Read the one field, as required by the following rule:
						// # vi. At least the randomNumber field must be read from the database result set.
						final int previousRead = w.getRandomNumber();
						//Update it, but make sure to exclude the current number as Hibernate optimisations would have us "fail"
						//the verification:
						w.setRandomNumber( localRandom.getNextRandomExcluding( previousRead ) );
					} );
					s.sessionOpened();
					return session
							.setBatchSize( worldsCollection.size() )
							.flush()
							.invoke( s::flushExecuted )
							.map( v -> worldsCollection );
				} )
		).invoke( s::sessionClosed ) );
	}

	private void storeState(State s) {
		synchronized ( processedStates ) {
			processedStates.add( s );
		}
	}

	private Future<Void> createData(RoutingContext ctx) {
		final LocalRandom random = Randomizer.current();
		return toFuture( emf.withSession( session -> Uni.createFrom()
				.completionStage( loop( 0, 10000, index -> {
					final World world = new World();
					world.setId( index + 1 );
					world.setRandomNumber( random.getNextRandom() );
					return session.persist( world ).subscribeAsCompletionStage();
				} ) ).call( session.setBatchSize( 1000 )::flush )
		) );
	}

	private static <U> Future<U> toFuture(Uni<U> uni) {
		return Future.fromCompletionStage( uni.convert().toCompletionStage() );
	}

	public static final void dumpSuspectStates(long timerId) {
		synchronized ( processedStates ) {
			for ( State processedState : processedStates ) {
				if ( processedState.sessionOpened && (!processedState.sessionClosed ) ) {
					LOG.error( "Session open but not closed: " + processedState.contextId + " flush state: " + processedState.sessionFlushed );
				}
				if ( ! processedState.sessionOpened ) {
					LOG.error( "Session was never opened successfully: " + processedState.contextId + " flush state: " + processedState.sessionFlushed );
				}
			}
		}
	}

	private static final class State {
		private volatile boolean sessionOpened = false;
		private volatile boolean sessionClosed = false;
		private volatile boolean sessionFlushed = false;
		final int contextId;

		private State() {
			this.contextId = c.incrementAndGet();
		}

		public void sessionOpened() {
			//LOG.info( "Session open, objects loaded and marked dirty "  + contextId );
			sessionOpened = true;
		}

		public void sessionClosed() {
			//LOG.info( "Session closed " + contextId );
			sessionClosed = true;
		}

		public void flushExecuted() {
			sessionFlushed = true;

		}
		//ERROR: Session open but not closed: 5
		//Jun 28, 2023 11:37:47 AM <unknown> <unknown>
		//ERROR: Session open but not closed: 24
		//Jun 28, 2023 11:37:47 AM <unknown> <unknown>
		//ERROR: Session open but not closed: 389

	}
}
