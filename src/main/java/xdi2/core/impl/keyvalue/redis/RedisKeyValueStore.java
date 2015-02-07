package xdi2.core.impl.keyvalue.redis;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import xdi2.core.impl.keyvalue.AbstractKeyValueStore;
import xdi2.core.impl.keyvalue.KeyValueStore;
import xdi2.redis.util.JedisMonitorThread;

/**
 * This class defines access to a Redis key/value store. It is used by the
 * RedisKeyValueGraphFactory class to create graphs stored in Redis.
 * 
 * @author markus
 */
public class RedisKeyValueStore extends AbstractKeyValueStore implements KeyValueStore {

	private JedisPool jedisPool;
	private String prefix;

	private JedisMonitorThread jedisMonitorThread;

	public RedisKeyValueStore(JedisPool jedisPool, Jedis monitorJedis, String prefix) {

		this.jedisPool = jedisPool;
		this.prefix = prefix;

		if (monitorJedis != null) {

			this.jedisMonitorThread = new JedisMonitorThread(monitorJedis);
			this.jedisMonitorThread.start();
		} else {

			this.jedisMonitorThread = null;
		}
	}

	@Override
	public void init() throws IOException {

	}

	@Override
	public void close() {

		this.getJedisPool().destroy();

		if (this.getJedisMonitorThread() != null) {

			this.getJedisMonitorThread().getMonitorJedis().disconnect();

			try {

				this.getJedisMonitorThread().join();
			} catch (InterruptedException ex) { }
		}
	}

	@Override
	public void set(String key, String value) {

		Jedis jedis = null;

		try {

			jedis = this.getJedisPool().getResource();

			jedis.sadd(this.getPrefix() + key, value);
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}
	}

	@Override
	public String getOne(String key) {

		Jedis jedis = null;
		String result = null;

		try {

			jedis = this.getJedisPool().getResource();

			result = jedis.srandmember(this.getPrefix() + key);
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}

		return result;
	}

	@Override
	public Iterator<String> getAll(String key) {

		Jedis jedis = null;
		Iterator<String> result = null;

		try {

			jedis = this.getJedisPool().getResource();

			result = jedis.smembers(this.getPrefix() + key).iterator();
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}

		return result;
	}

	@Override
	public boolean contains(String key) {

		Jedis jedis = null;
		boolean result = false;

		try {

			jedis = this.getJedisPool().getResource();

			result = Boolean.TRUE.equals(jedis.exists(this.getPrefix() + key));
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}

		return result;
	}

	@Override
	public boolean contains(String key, String value) {

		Jedis jedis = null;
		boolean result = false;

		try {

			jedis = this.getJedisPool().getResource();

			result = Boolean.TRUE.equals(jedis.sismember(this.getPrefix() + key, value));
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}

		return result;
	}

	@Override
	public void delete(String key) {

		Jedis jedis = null;

		try {

			jedis = this.getJedisPool().getResource();

			jedis.del(this.getPrefix() + key);
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}
	}

	@Override
	public void delete(String key, String value) {

		Jedis jedis = null;

		try {

			jedis = this.getJedisPool().getResource();

			jedis.srem(this.getPrefix() + key, value);
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}
	}

	@Override
	public long count(String key) {

		Jedis jedis = null;
		long result = -1;

		try {

			jedis = this.getJedisPool().getResource();

			result = jedis.scard(this.getPrefix() + key).longValue();
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}

		return result;
	}

	@Override
	public void clear() {

		Jedis jedis = null;

		try {

			jedis = this.getJedisPool().getResource();

			Set<String> keys = jedis.keys(this.getPrefix() + "*");
			if (! keys.isEmpty()) jedis.del(keys.toArray(new String[keys.size()]));
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}
	}

	public JedisPool getJedisPool() {

		return this.jedisPool;
	}

	public String getPrefix() {

		return this.prefix;
	}

	public JedisMonitorThread getJedisMonitorThread() {

		return this.jedisMonitorThread;
	}

	/*
	 * Helper methods
	 */

	public static void cleanup(String host) {

		cleanup(host, null);
	}

	public static void cleanup(String host, Integer port) {

		Jedis jedis = null;
		
		try {

			jedis = port == null ? new Jedis(host) : new Jedis(host, port.intValue());

			jedis.flushDB();
		} catch (Exception ex) {

			throw new RuntimeException(ex.getMessage(), ex);
		} finally {

			if (jedis != null) jedis.close();
		}
	}
}
