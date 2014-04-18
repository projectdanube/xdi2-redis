package xdi2.tests.core.impl.keyvalue.redis;

import xdi2.core.GraphFactory;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueGraphFactory;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueStore;
import xdi2.tests.core.impl.AbstractGraphTest;

public class RedisKeyValueGraphTest extends AbstractGraphTest {

	private static RedisKeyValueGraphFactory graphFactory = new RedisKeyValueGraphFactory();

	public static final String HOST = "localhost";

	static {
		
		graphFactory.setHost(HOST);
	}
	
	@Override
	protected void setUp() throws Exception {

		super.setUp();

		RedisKeyValueStore.cleanup(HOST);
	}

	@Override
	protected void tearDown() throws Exception {

		super.tearDown();

		RedisKeyValueStore.cleanup(HOST);
	}

	@Override
	protected GraphFactory getGraphFactory() {

		return graphFactory;
	}

	@Override
	protected boolean supportsPersistence() {

		return true;
	}
}
