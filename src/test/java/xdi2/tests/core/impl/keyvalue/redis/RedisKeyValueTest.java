package xdi2.tests.core.impl.keyvalue.redis;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;

import redis.clients.jedis.JedisPool;
import xdi2.core.impl.keyvalue.KeyValueStore;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueStore;
import xdi2.tests.core.impl.keyvalue.AbstractKeyValueTest;

public class RedisKeyValueTest extends AbstractKeyValueTest {

	public static final String HOST = "localhost";

	@Override
	protected KeyValueStore getKeyValueStore(String id) throws IOException {

		// create jedis

		JedisPool jedisPool = new JedisPool(HOST);

		// create prefix

		String prefix = new String(Base64.encodeBase64(id.getBytes(Charset.forName("UTF-8"))), Charset.forName("UTF-8")) + ".";

		// create the key/value store

		KeyValueStore keyValueStore = new RedisKeyValueStore(jedisPool, null, prefix);

		// done

		return keyValueStore;
	}
}
