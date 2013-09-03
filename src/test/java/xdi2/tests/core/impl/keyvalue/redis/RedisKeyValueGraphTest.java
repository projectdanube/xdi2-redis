package xdi2.tests.core.impl.keyvalue.redis;

import java.io.IOException;

import xdi2.core.Graph;
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
	protected Graph openNewGraph(String identifier) throws IOException {

		return graphFactory.openGraph(identifier);
	}

	@Override
	protected Graph reopenGraph(Graph graph, String identifier) throws IOException {

		graph.close();

		return graphFactory.openGraph(identifier);
	}
}
