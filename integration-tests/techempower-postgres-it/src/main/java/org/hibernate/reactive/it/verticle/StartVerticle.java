/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.it.verticle;

import java.util.concurrent.TimeUnit;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.engine.impl.StateTrackerUtil;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.vertx.VertxInstance;

import org.jboss.logging.Logger;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.OutputFrame;

/**
 * Make it easier to run benchmarks with external tools like "wrk"
 */
public class StartVerticle {

	private static final Logger LOG = Logger.getLogger( WorldVerticle.class );

	// These properties are in DatabaseConfiguration in core
	public static final boolean USE_DOCKER = false;

	public static final String IMAGE_NAME = "postgres:15-bullseye";
	public static final String USERNAME = "benchmarkdbuser";
	public static final String PASSWORD = "benchmarkdbpass";
	public static final String DB_NAME = "hello_world";

	public static final int VERTICLE_INSTANCES = 10;

	private static final String POSGRESQL_CONF_PATH = "/home/ddalto/Workspace/FrameworkBenchmarks/toolset/databases/postgres/postgresql-min.conf";

	public static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>( IMAGE_NAME )
			.withUsername( USERNAME )
			.withPassword( PASSWORD )
			.withDatabaseName( DB_NAME )
			.withCommand( "postgres", "-c", "config_file=/ssd/postgresql/postgresql.conf" )
			.withEnv( "PG_DATA", "/ssd/postgresql" )
			.withFileSystemBind( POSGRESQL_CONF_PATH, "/ssd/postgresql/postgresql.conf" )
			.withReuse( true )
			.withLogConsumer( StartVerticle::log );

	private static void log(OutputFrame outputFrame) {
		System.out.println( outputFrame.getUtf8String() );
	}


	private static Configuration constructConfiguration(boolean enableDocker) {
		Configuration configuration = new Configuration();
		configuration.addAnnotatedClass( World.class );

		configuration.setProperty( Settings.HBM2DDL_AUTO, "validate" );
		configuration.setProperty( Settings.URL, dbConnectionUrl( enableDocker ) );
		configuration.setProperty( Settings.USER, USERNAME );
		configuration.setProperty( Settings.PASS, PASSWORD );

		//Use JAVA_TOOL_OPTIONS='-Dhibernate.show_sql=true'
		configuration.setProperty( Settings.SHOW_SQL, System.getProperty( Settings.SHOW_SQL, "false" ) );
		configuration.setProperty( Settings.FORMAT_SQL, System.getProperty( Settings.FORMAT_SQL, "false" ) );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, System.getProperty( Settings.HIGHLIGHT_SQL, "true" ) );
		return configuration;
	}

	private static String dbConnectionUrl(boolean enableDocker) {
		if ( enableDocker ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			postgresql.start();
			return postgresql.getJdbcUrl();
		}

		// When we don't use testcontainers we expect a database on the default url
		return "postgres://localhost:5432/" + DB_NAME;
	}

	public static SessionFactory createHibernateSessionFactory(boolean enableDocker, io.vertx.core.Vertx vertx) {
		final Configuration configuration = constructConfiguration( enableDocker );
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.addService( VertxInstance.class, (VertxInstance) () -> vertx )
				.applySettings( configuration.getProperties() );
		StandardServiceRegistry registry = builder.build();
		return configuration.buildSessionFactory( registry );
	}

	public static VertxOptions vertxOptions() {
		VertxOptions vertxOptions = new VertxOptions();
		vertxOptions.setBlockedThreadCheckInterval( 5 );
		vertxOptions.setBlockedThreadCheckIntervalUnit( TimeUnit.MINUTES );
		return vertxOptions;
	}

	public static void main(String[] args) {
		DeploymentOptions options = new DeploymentOptions();
		options.setInstances( VERTICLE_INSTANCES );

		Vertx vertx = Vertx.vertx( vertxOptions() );
		final Mutiny.SessionFactory sessionFactory = createHibernateSessionFactory( USE_DOCKER, vertx )
				.unwrap( Mutiny.SessionFactory.class );
		vertx.deployVerticle( () -> new WorldVerticle( () -> sessionFactory ), options )
				.onSuccess( s -> {
					LOG.info( "✅ Deployment success" );
					LOG.info( "💡 Vert.x app started" );
				} )
				.onFailure( err -> LOG.errorf( "🔥 Deployment failure %s", err ) );
//		vertx.setPeriodic( 5_000, 5_000, WorldVerticle::dumpSuspectStates );
//		vertx.setPeriodic( 5_000, 5_000, DefaultReactiveFlushEventListener::dumpSuspectStates );
		vertx.setPeriodic( 5_000, 5_000, StateTrackerUtil::dumpSuspectStates );
	}
	//ERROR: Session open but not closed: 66
	//Jun 28, 2023 11:33:43 AM <unknown> <unknown>
	//ERROR: Session open but not closed: 306
	//Jun 28, 2023 11:33:48 AM <unknown> <unknown>
}
