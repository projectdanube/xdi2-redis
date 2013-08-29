package xdi2.messaging.tests.target.impl.graph;

import java.io.IOException;

import redis.clients.jedis.Jedis;

import xdi2.core.Graph;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueGraphFactory;

public class RedisKeyValueGraphMessagingTargetTest extends AbstractGraphMessagingTargetTest {

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
}
