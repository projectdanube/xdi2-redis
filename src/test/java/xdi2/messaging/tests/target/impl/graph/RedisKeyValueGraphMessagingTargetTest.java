package xdi2.messaging.tests.target.impl.graph;

import java.io.IOException;

import xdi2.core.Graph;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueGraphFactory;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueStore;
import xdi2.messaging.target.tests.impl.graph.AbstractGraphMessagingTargetTest;

public class RedisKeyValueGraphMessagingTargetTest extends AbstractGraphMessagingTargetTest {

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
	protected Graph openGraph(String identifier) throws IOException {

		return graphFactory.openGraph(identifier);
	}
}
