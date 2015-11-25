/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.id.enhanced;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.HibernateException;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.internal.CoreMessageLogger;
import org.jboss.logging.Logger;

/**
 * Variation of {@link PooledOptimizer} which interprets the incoming database value as the lo value, rather than
 * the hi value.
 *
 * @author Steve Ebersole
 *
 * @see PooledOptimizer
 */
public class PooledLoOptimizer extends AbstractOptimizer {
	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			PooledLoOptimizer.class.getName()
	);

	private static long THREAD_LOCAL_BLOCK_SIZE = Integer.getInteger("org.hibernate.thread-local-block-size", 5000);

	private static class GenerationState {
		// last value read from db source
		private IntegralDataTypeHolder lastSourceValue;
		// the current generator value
		private IntegralDataTypeHolder value;
		// the value at which we'll hit the db again
		private IntegralDataTypeHolder upperLimitValue;
	}

	/**
	 * Constructs a PooledLoOptimizer.
	 *
	 * @param returnClass The Java type of the values to be generated
	 * @param incrementSize The increment size.
	 */
	public PooledLoOptimizer(Class returnClass, int incrementSize) {
		super( returnClass, incrementSize );
		if ( incrementSize < 1 ) {
			throw new HibernateException( "increment size cannot be less than 1" );
		}
		LOG.creatingPooledLoOptimizer( incrementSize, returnClass.getName() );
	}

	@Override
	public Serializable generate(AccessCallback callback) {
		GenerationState local = null;
		if ( callback.getTenantIdentifier() == null ) {
		 	local = localAssignedIds.get();
			if ( local != null && local.value.lt( local.upperLimitValue ) ) {
				return local.value.makeValueThenIncrement();
			}
		}

		synchronized (this) {
			final GenerationState generationState = locateGenerationState(callback.getTenantIdentifier());

			if (generationState.lastSourceValue == null
					|| !generationState.value.lt(generationState.upperLimitValue)) {
				generationState.lastSourceValue = callback.getNextValue();
				generationState.upperLimitValue = generationState.lastSourceValue.copy().add(incrementSize);
				generationState.value = generationState.lastSourceValue.copy();
				// handle cases where initial-value is less that one (hsqldb for instance).
				while (generationState.value.lt(1)) {
					generationState.value.increment();
				}
			}
			if(callback.getTenantIdentifier() != null) {
				return generationState.value.makeValueThenIncrement();
			} else {
				if ( local == null ) {
					local = new GenerationState();
					localAssignedIds.set( local );
				}
				long toIncrement = THREAD_LOCAL_BLOCK_SIZE;
				local.value = generationState.value.copy();
				generationState.value.add(toIncrement);
				if(!generationState.value.lt(generationState.upperLimitValue)) {
					//IntegralDataTypeHolder is not great for this, for such a simple math operation there is no easy way to do it
					long subtract;
					Number val1 = generationState.value.makeValue();
					Number val2 = generationState.upperLimitValue.makeValue();
					if( val1 instanceof BigDecimal ) {
						BigDecimal b1 = (BigDecimal) val1;
						BigDecimal b2 = (BigDecimal) val2;
						subtract = b1.subtract(b2).longValue();
					} else if( val1 instanceof BigInteger ) {
						BigInteger b1 = (BigInteger) val1;
						BigInteger b2 = (BigInteger) val2;
						subtract = b1.subtract(b2).longValue();
					} else {
						subtract = val1.longValue() - val2.longValue();
					}
					generationState.value.subtract(subtract);
				}
				local.upperLimitValue = generationState.value.copy();

				return local.value.makeValueThenIncrement();
			}
		}
	}

	private GenerationState noTenantState;
	private Map<String,GenerationState> tenantSpecificState;
	private ThreadLocal<GenerationState> localAssignedIds = new ThreadLocal<GenerationState>();

	private GenerationState locateGenerationState(String tenantIdentifier) {
		if ( tenantIdentifier == null ) {
			if ( noTenantState == null ) {
				noTenantState = new GenerationState();
			}
			return noTenantState;
		}
		else {
			GenerationState state;
			if ( tenantSpecificState == null ) {
				tenantSpecificState = new ConcurrentHashMap<String, GenerationState>();
				state = new GenerationState();
				tenantSpecificState.put( tenantIdentifier, state );
			}
			else {
				state = tenantSpecificState.get( tenantIdentifier );
				if ( state == null ) {
					state = new GenerationState();
					tenantSpecificState.put( tenantIdentifier, state );
				}
			}
			return state;
		}
	}

	private GenerationState noTenantGenerationState() {
		if ( noTenantState == null ) {
			throw new IllegalStateException( "Could not locate previous generation state for no-tenant" );
		}
		return noTenantState;
	}

	@Override
	public IntegralDataTypeHolder getLastSourceValue() {
		return noTenantGenerationState().lastSourceValue;
	}

	@Override
	public boolean applyIncrementSizeToSourceValues() {
		return true;
	}
}
