package xdi2.tests.core.impl;

import java.io.IOException;

import xdi2.core.Graph;
import xdi2.core.impl.redis.RedisGraphFactory;
import xdi2.tests.core.graph.AbstractGraphTest;

public class RedisGraphTest extends AbstractGraphTest {

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

	@Override
	protected Graph reopenGraph(Graph graph, String identifier) throws IOException {

		graph.close();

		return graphFactory.openGraph(identifier);
	}
}
