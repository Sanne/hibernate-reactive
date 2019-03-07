package org.hibernate.rx;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.persistence.EntityTransaction;

/**
 * Depending of what we want to test, this session can be configured using the {@link Builder} to
 * simulate the operations in {@link RxSession}.
 * <p>
 * By default, if no operation is specified the methods will return null or do nothing.
 * </p>
 */
public class MockRxSession implements RxSession {

		private BiFunction loadFunction = ( type, id ) -> { return null; };
		private Consumer persistFunction = obj -> {};
		private Consumer removeFunction = obj -> {};

		/**
		 * Assign the functions that simulate the access to a source for CRUD.
		 *
		 * @param <T> the type of the entity
		 */
		public static class Builder<T> {
			MockRxSession session = new MockRxSession();

			public Builder find(BiFunction<Class<T>, ?, T> load ) {
				session.loadFunction = load;
				return this;
			}

			public Builder persist(Consumer<T> persist) {
				session.persistFunction = persist;
				return this;
			}

			public Builder remove(Consumer<T> remove) {
				session.removeFunction = remove;
				return this;
			}

			public RxSession build() {
				return session;
			}
		}

	@Override
	public CompletionStage<Void> inTransaction(BiConsumer<RxSession, EntityTransaction> consumer) {
		return null;
	}

	@Override
	public <T> CompletionStage<Optional<T>> find(Class<T> entityClass, final Object id) {
		Supplier<Optional<T>> supplier = () -> {
			Object result = loadFunction.apply( entityClass, id );
			if ( result == null ) {
				return Optional.empty();
			}
			else {
				return Optional.of( (T) result );
			}
		};
		return CompletableFuture.supplyAsync( supplier );
	}

	@Override
	public CompletionStage<Void> persist(Object entity) {
		return CompletableFuture.runAsync( () -> persistFunction.accept( entity ) );
	}

	@Override
	public CompletionStage<Void> remove(Object entity) {
		return CompletableFuture.runAsync( () -> removeFunction.accept( entity ) );
	}

	@Override
	public <R> RxQuery<R> createQuery(Class<R> resultType, String jpql) {
		throw new UnsupportedOperationException( "not implemented" );
	}

	@Override
	public StateControl sessionState() {
		throw new UnsupportedOperationException( "not implemented" );
	}

}
