/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it;

import org.hibernate.reactive.it.verticle.StartVerticle;
import org.hibernate.reactive.it.verticle.WorldVerticle;
import org.hibernate.reactive.mutiny.Mutiny;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import junit.framework.AssertionFailedError;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.Timeout;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;


/**
 * Test that the Rest API is correct.
 * <p>
 *     <dl>
 *         <dt><a href="http://localhost:8088/createData">http://localhost:8080/createData</a></dt>
 *         <dd>Populate the database with an initial dataset</dd>
 *         <dt><a href="http://localhost:8088/updates?queries=20">http://localhost:8088/updates?queries=20</a></dt>
 *         <dd>Update 20 random entries</dd>
 *     </dl>
 * </p>
 * Example
 */
@RunWith(VertxUnitRunner.class)
public class RestAPITest {

	private static final int VERTICLE_INSTANCES = 10;

	@Rule
	public Timeout rule = Timeout.seconds( 5 * 60 );

	@Test
	public void testWorldRepository(TestContext context) {
		final Async async = context.async();
		final Vertx vertx = Vertx.vertx( StartVerticle.vertxOptions() );

		Mutiny.SessionFactory sf = StartVerticle
				.createHibernateSessionFactory( StartVerticle.USE_DOCKER, vertx )
				.unwrap( Mutiny.SessionFactory.class );

		final WebClient webClient = WebClient.create( vertx );

		final DeploymentOptions deploymentOptions = new DeploymentOptions();
		deploymentOptions.setInstances( VERTICLE_INSTANCES );

		vertx
				.deployVerticle( () -> new WorldVerticle( () -> sf ), deploymentOptions )
				.map( ignore -> webClient )
				.compose( this::createData )
				.map( ignore -> webClient )
				.compose( this::updates )
				.onSuccess( res -> async.complete() )
				.onFailure( context::fail )
				.eventually( unused -> vertx.close() );
	}

	/**
	 * Create several products using http requests
	 */
	private Future<?> createData(WebClient webClient) {
		return webClient
				.get( WorldVerticle.HTTP_PORT, "localhost", "/createData" )
				.send()
				.compose( this::handle );
	}

	/**
	 * Use http requests to find the products previously created and validate them
	 */
	private Future<?> updates(WebClient webClient) {
		// Send the request
		return webClient
				.get( WorldVerticle.HTTP_PORT, "localhost", "/updates?queries=20" )
				.send()
				.compose( this::handle );
	}

	private Future<Void> handle(HttpResponse<Buffer> response) {
		switch ( response.statusCode() ) {
			case 200:
			case 204:
				return succeededFuture();
			default:
				return failedFuture( new AssertionFailedError( "Expected status code 200 or 204, but was " + response.statusCode() ) );
		}
	}
}
