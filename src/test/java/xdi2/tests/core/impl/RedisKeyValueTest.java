package xdi2.tests.core.impl;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

import redis.clients.jedis.Jedis;
import xdi2.core.impl.keyvalue.KeyValueStore;
import xdi2.core.impl.redis.RedisKeyValueStore;
import xdi2.tests.core.impl.keyvalue.AbstractKeyValueTest;

public class RedisKeyValueTest extends AbstractKeyValueTest {

	public static final String HOST = "localhost";

	@Override
	protected KeyValueStore getKeyValueStore(String id) throws IOException {

		// create jedis

		Jedis jedis = new Jedis(HOST);

		// create prefix

		String prefix = new String(Base64.encodeBase64(id.getBytes("UTF-8")), "UTF-8") + ".";

		// create the key/value store

		KeyValueStore keyValueStore = new RedisKeyValueStore(jedis, prefix);

		// done

		return keyValueStore;
	}
}
