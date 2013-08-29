package xdi2.tests.core.impl;

import java.io.IOException;

import redis.clients.jedis.Jedis;
import xdi2.core.Graph;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueGraphFactory;
import xdi2.tests.core.graph.AbstractGraphTest;

public class RedisKeyValueGraphTest extends AbstractGraphTest {

	private static RedisKeyValueGraphFactory graphFactory = new RedisKeyValueGraphFactory();

	public static final String HOST = "localhost";

	static {

		cleanup();
	}

	public static void cleanup() {

		graphFactory.setHost(HOST);

		try {

			new Jedis(HOST).flushDB();
		} catch (Exception ex) {

			throw new RuntimeException(ex.getMessage(), ex);
		}
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
