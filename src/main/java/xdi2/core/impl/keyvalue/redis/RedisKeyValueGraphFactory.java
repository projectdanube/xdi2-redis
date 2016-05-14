package xdi2.core.impl.keyvalue.redis;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import xdi2.core.GraphFactory;
import xdi2.core.impl.keyvalue.AbstractKeyValueGraphFactory;
import xdi2.core.impl.keyvalue.KeyValueStore;

/**
 * GraphFactory that creates key/value graphs in Redis.
 * 
 * @author markus
 */
public class RedisKeyValueGraphFactory extends AbstractKeyValueGraphFactory implements GraphFactory {

	public static final boolean DEFAULT_SUPPORT_GET_CONTEXTNODES = true; 
	public static final boolean DEFAULT_SUPPORT_GET_RELATIONS = true; 

	public static final String DEFAULT_HOST = "localhost";
	public static final Integer DEFAULT_PORT = null;
	public static final boolean DEFAULT_MONITOR = false;

	private String host;
	private Integer port;
	private boolean monitor;

	public RedisKeyValueGraphFactory() {

		super(DEFAULT_SUPPORT_GET_CONTEXTNODES, DEFAULT_SUPPORT_GET_RELATIONS);

		this.host = DEFAULT_HOST;
		this.port = DEFAULT_PORT;
		this.monitor = DEFAULT_MONITOR;
	}

	@Override
	@SuppressWarnings("resource")
	protected KeyValueStore openKeyValueStore(String identifier) throws IOException {

		// check identifier

		String prefix = new String(Base64.encodeBase64(identifier.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8) + ".";

		// create jedis

		JedisPool jedisPool = this.getPort() == null ? new JedisPool(this.getHost()) : new JedisPool(this.getHost(), this.getPort().intValue());
		Jedis monitorJedis;

		if (this.isMonitor()) {

			monitorJedis = this.getPort() == null ? new Jedis(this.getHost()) : new Jedis(this.getHost(), this.getPort().intValue());
		} else {

			monitorJedis = null;
		}

		// open store

		KeyValueStore keyValueStore;

		keyValueStore = new RedisKeyValueStore(jedisPool, monitorJedis, prefix);
		keyValueStore.init();

		// done

		return keyValueStore;
	}

	/*
	 * Getters and setters
	 */
	
	public String getHost() {

		return this.host;
	}

	public void setHost(String host) {

		this.host = host;
	}

	public Integer getPort() {

		return this.port;
	}

	public void setPort(Integer port) {

		this.port = port;
	}

	public boolean isMonitor() {

		return this.monitor;
	}

	public void setMonitor(boolean monitor) {

		this.monitor = monitor;
	}
}
