package xdi2.messaging.tests.target.impl.graph;

import java.io.IOException;

import xdi2.core.Graph;
import xdi2.core.impl.redis.RedisGraphFactory;

public class RedisGraphMessagingTargetTest extends AbstractGraphMessagingTargetTest {

	private static RedisGraphFactory graphFactory = new RedisGraphFactory();

	public static final String PATH = "dummy";

	static {

		cleanup();
	}

	public static void cleanup() {

		graphFactory.setPath(PATH);

		try {

			// delete everything in redis
		} catch (Exception ex) {

			throw new RuntimeException(ex.getMessage(), ex);
		}
	}

	@Override
	protected Graph openNewGraph(String identifier) throws IOException {

		return graphFactory.openGraph(identifier);
	}
}
