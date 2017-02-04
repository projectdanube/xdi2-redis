package xdi2.messaging.container.tests.impl.graph;

import java.io.IOException;

import xdi2.core.Graph;
import xdi2.core.impl.json.redis.RedisJSONGraphFactory;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueStore;
import xdi2.messaging.container.tests.impl.graph.AbstractGraphMessagingContainerTest;

public class RedisJSONGraphMessagingContainerTest extends AbstractGraphMessagingContainerTest {

	private static RedisJSONGraphFactory graphFactory = new RedisJSONGraphFactory();

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
