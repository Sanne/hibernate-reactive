/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.id.impl.ReactiveGeneratorWrapper;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.session.impl.ReactiveSessionFactoryImpl;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.stage.impl.StageSessionImpl;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.reactive.vertx.VertxInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(VertxUnitRunner.class)
public class MultithreadedIdentityGenerationTest {

    private static final int N_THREADS = 12;
    private static final int IDS_GENERATED_PER_THREAD = 5000;
    private static final int TIMEOUT_MINUTES = 10;
    private static final boolean LOG_SQL = false;
    private static final Latch startLatch = new Latch("start", N_THREADS);
    private static final Latch endLatch = new Latch("end", N_THREADS);

    private static Stage.SessionFactory stageSessionFactory;
    private static Vertx vertx;
    private static SessionFactory sessionFactory;

    @BeforeClass
    public static void setupSessionFactory() {
        final VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setEventLoopPoolSize(N_THREADS);
        vertxOptions.setBlockedThreadCheckInterval(TIMEOUT_MINUTES);
        vertxOptions.setBlockedThreadCheckIntervalUnit(TimeUnit.MINUTES);
        vertx = Vertx.vertx(vertxOptions);
        Configuration configuration = new Configuration();
        configuration.addAnnotatedClass( EntityWithGeneratedId.class );
        BaseReactiveTest.setDefaultProperties( configuration );
        configuration.setProperty( Settings.SHOW_SQL, String.valueOf(LOG_SQL) );
        StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
                .applySettings( configuration.getProperties() )
                .addService( VertxInstance.class, () -> vertx );
        StandardServiceRegistry registry = builder.build();
        sessionFactory = configuration.buildSessionFactory(registry);
        stageSessionFactory = sessionFactory.unwrap(Stage.SessionFactory.class);
    }

    @AfterClass
    public static void closeSessionFactory() {
        stageSessionFactory.close();
    }

    private ReactiveGeneratorWrapper getIdGenerator() {
        final ReactiveSessionFactoryImpl hibernateSessionFactory = (ReactiveSessionFactoryImpl) sessionFactory;
        final ReactiveGeneratorWrapper identifierGenerator = (ReactiveGeneratorWrapper) hibernateSessionFactory.getIdentifierGenerator("org.hibernate.reactive.MultithreadedIdentityGenerationTest$EntityWithGeneratedId");
        return identifierGenerator;
    }

    @Test(timeout = (1000*60*10))//10 minutes timeout
    public void testIdentityGenerator(TestContext context) {
        final Async async = context.async();
        final ReactiveGeneratorWrapper idGenerator = getIdGenerator();
        context.assertNotNull(idGenerator);

        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setInstances(N_THREADS);

        List<List<Long>> allResults = new ArrayList<>();

        vertx
                .deployVerticle( () -> new IdGenVerticle( idGenerator, allResults ), deploymentOptions )
                .onSuccess( res -> {
                    endLatch.waitForEveryone();
                    if (allunique(allResults)) {
                        async.complete();
                    }
                    else {
                        context.fail("Non unique numbers detected");
                    }
                } )
                .onFailure( context::fail )
                .eventually( unused -> vertx.close() );
    }

    private boolean allunique(List<List<Long>> allResults) {
        //Add 50 per thread to the total amount of generated ids to allow for gaps
        //in the hi/lo partitioning (not likely to be necessary)
        final int expectedSize = N_THREADS * (IDS_GENERATED_PER_THREAD + 50);
        BitSet resultsSeen = new BitSet(expectedSize);
        boolean failed = false;
        for (List<Long> partialResult : allResults) {
            for (Long aLong : partialResult) {
                final int intValue = aLong.intValue();
                final boolean existing = resultsSeen.get(intValue);
                if (existing) {
                    System.out.println("Duplicate detected: " + intValue);
                    failed=true;
                }
                resultsSeen.set(intValue);
            }
        }
        return !failed;
    }

    private static class IdGenVerticle extends AbstractVerticle {
        private final ReactiveGeneratorWrapper idGenerator;
        private final List<List<Long>> allResults;
        private final ArrayList<Long> generatedIds = new ArrayList<>(IDS_GENERATED_PER_THREAD);
        private final AtomicBoolean starting = new AtomicBoolean(false);
        private final AtomicBoolean started = new AtomicBoolean(false);
        private final AtomicBoolean stopped = new AtomicBoolean(false);


        public IdGenVerticle(ReactiveGeneratorWrapper idGenerator, List<List<Long>> allResults) {
            this.idGenerator = idGenerator;
            this.allResults = allResults;
        }

        @Override
        public void start(Promise<Void> startPromise) throws InterruptedException {
            assertBoolean(starting, false);
            assertBoolean(started, false);
            assertBoolean(stopped, false);
            starting.set(true);
            startLatch.reached();
            startLatch.waitForEveryone();
            stageSessionFactory.withSession(
                            s -> generateMultipleIds( idGenerator, s, generatedIds)
                    )
                    .whenComplete((o, throwable) -> {
                        assertBoolean(started, false);
                        started.set(true);
                        endLatch.reached();
                        if (throwable != null) {
                            startPromise.fail(throwable);
                        } else {
                            endLatch.waitForEveryone();
                            allResults.add(generatedIds);
                            final String name2 = Thread.currentThread().getName();
                            System.out.println("Thread [" + name2 + "] Deployed fine, content: " + generatedIds);
                            startPromise.complete();
                        }
                    });
        }

        @Override
        public void stop() throws Exception {
            assertBoolean(starting, true);
            assertBoolean(started, true);
            assertBoolean(stopped, false);
            stopped.set(true);
            System.out.println("Verticle stopped " + super.toString());
        }
    }

    private static void assertBoolean(AtomicBoolean bool, boolean expectedValue) {
        if ( bool.get() != expectedValue ) {
            throw new IllegalStateException();
        }
    }

    private static CompletionStage<Void> generateMultipleIds(ReactiveGeneratorWrapper idGenerator, Stage.Session s, ArrayList<Long> collector) {
        prettyOut("Preparing completionstage to gen ids");
        return CompletionStages.loop(0, IDS_GENERATED_PER_THREAD, index -> generateIds(idGenerator, s, collector) );
    }

    private static CompletionStage<Void> generateIds(ReactiveGeneratorWrapper idGenerator, Stage.Session s, ArrayList<Long> collector) {
        return idGenerator.generate(((StageSessionImpl) s)
                .unwrap(ReactiveConnectionSupplier.class), new EntityWithGeneratedId())
            .thenAccept( o -> collector.add((Long)o) );
    }

    @Entity
    private static class EntityWithGeneratedId {
        @Id @GeneratedValue
        Long id;

        String name;

        public EntityWithGeneratedId() {
        }
    }

    private static final class Latch {
        private final String label;
        private final CountDownLatch countDownLatch;

        public Latch(String label, int membersCount) {
            this.label = label;
            this.countDownLatch = new CountDownLatch(membersCount);
        }

        public void reached() {
            final long count = countDownLatch.getCount();
            countDownLatch.countDown();
            prettyOut("Reached latch '" + label +"', current countdown is " + (count-1));
        }

        public void waitForEveryone() {
            try {
                countDownLatch.await(TIMEOUT_MINUTES, TimeUnit.MINUTES);
                prettyOut("Everyone has now breached '"+label+"'");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private static void prettyOut(final String message) {
        final String threadName = Thread.currentThread().getName();
        System.out.println( threadName + ": " + message);
    }

}
