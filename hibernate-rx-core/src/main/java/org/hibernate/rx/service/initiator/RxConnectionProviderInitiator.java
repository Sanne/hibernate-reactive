package org.hibernate.rx.service.initiator;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.rx.service.RxConnectionPoolProviderImpl;
import org.hibernate.service.spi.ServiceRegistryImplementor;

public class RxConnectionProviderInitiator implements StandardServiceInitiator<RxConnectionPoolProviderImpl> {

	public static final RxConnectionProviderInitiator INSTANCE = new RxConnectionProviderInitiator();

	private RxConnectionProviderInitiator() {
	}

	@Override
	public RxConnectionPoolProviderImpl initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		return new RxConnectionPoolProviderImpl( configurationValues );
	}

	@Override
	public Class<RxConnectionPoolProviderImpl> getServiceInitiated() {
		return RxConnectionPoolProviderImpl.class;
	}
}
