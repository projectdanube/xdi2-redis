package xdi2.core.impl.redis;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import xdi2.core.impl.keyvalue.AbstractKeyValueStore;
import xdi2.core.impl.keyvalue.KeyValueStore;

/**
 * This class defines access to a Redis key/value store. It is used by the
 * RedisGraphFactory class to create graphs stored in Redis.
 * 
 * @author markus
 */
public class RedisKeyValueStore extends AbstractKeyValueStore implements KeyValueStore {

	private static final Logger log = LoggerFactory.getLogger(RedisKeyValueStore.class);

	private Jedis jedis;
	private String prefix;

	private MyJedisMonitorThread jedisMonitorThread;

	public RedisKeyValueStore(Jedis jedis, Jedis monitorJedis, String prefix) {

		this.jedis = jedis;
		this.prefix = prefix;

		if (monitorJedis != null) {

			this.jedisMonitorThread = new MyJedisMonitorThread(monitorJedis);
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

		this.getJedis().disconnect();

		if (this.getJedisMonitorThread() != null) {

			this.getJedisMonitorThread().getMonitorJedis().disconnect();

			try {

				this.getJedisMonitorThread().join();
			} catch (InterruptedException ex) { 

				ex.printStackTrace(System.err);
			}
		}
	}

	@Override
	public void set(String key, String value) {

		this.getJedis().sadd(this.getPrefix() + key, value);
	}

	@Override
	public String getOne(String key) {

		return this.getJedis().srandmember(this.getPrefix() + key);
	}

	@Override
	public Iterator<String> getAll(String key) {

		return this.getJedis().smembers(this.getPrefix() + key).iterator();
	}

	@Override
	public boolean contains(String key) {

		return Boolean.TRUE.equals(this.getJedis().exists(this.getPrefix() + key));
	}

	@Override
	public boolean contains(String key, String value) {

		return Boolean.TRUE.equals(this.getJedis().sismember(this.getPrefix() + key, value));
	}

	@Override
	public void delete(String key) {

		this.getJedis().del(this.getPrefix() + key);
	}

	@Override
	public void delete(String key, String value) {

		this.getJedis().srem(this.getPrefix() + key, value);
	}

	@Override
	public long count(String key) {

		return this.getJedis().scard(this.getPrefix() + key).longValue();
	}

	@Override
	public void clear() {

		Set<String> keys = this.getJedis().keys(this.getPrefix() + "*");
		if (keys.isEmpty()) return;

		this.getJedis().del(keys.toArray(new String[keys.size()]));
	}

	public Jedis getJedis() {

		return this.jedis;
	}

	public String getPrefix() {

		return this.prefix;
	}

	public MyJedisMonitorThread getJedisMonitorThread() {

		return this.jedisMonitorThread;
	}

	/*
	 * Helper classes
	 */

	public static class MyJedisMonitorThread extends Thread {

		private StringBuffer buffer;
		private int count;
		private Jedis monitorJedis;

		public MyJedisMonitorThread(Jedis monitorJedis) {

			this.buffer = new StringBuffer();
			this.count = 0;
			this.monitorJedis = monitorJedis;
		}

		@Override
		public void run() {

			if (log.isDebugEnabled()) log.debug("Starting monitor.");

			try {

				this.getMonitorJedis().monitor(new JedisMonitor() {

					@Override
					public void onCommand(String command) {

						if (log.isDebugEnabled()) log.debug(command);

						MyJedisMonitorThread.this.buffer.append(command + "\n");
					}
				});
			} catch (Exception ex) {

				if (log.isDebugEnabled()) log.debug("Stopping monitor.");
			}
		}

		public StringBuffer getBuffer() {

			return this.buffer;
		}

		public int getCount() {

			return this.count;
		}

		public Jedis getMonitorJedis() {

			return this.monitorJedis;
		}

		public void reset() {

			this.buffer = new StringBuffer();
			this.count = 0;
		}
	}
}
