package xdi2.core.impl.json.redis;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import xdi2.core.GraphFactory;
import xdi2.core.impl.json.AbstractJSONGraphFactory;
import xdi2.core.impl.json.JSONStore;

/**
 * GraphFactory that creates JSON graphs in Redis.
 * 
 * @author markus
 */
public class RedisJSONGraphFactory extends AbstractJSONGraphFactory implements GraphFactory {

	public static final String DEFAULT_HOST = "localhost";
	public static final Integer DEFAULT_PORT = null;
	public static final boolean DEFAULT_MONITOR = false;

	private String host;
	private Integer port;
	private boolean monitor;

	public RedisJSONGraphFactory() { 

		super();

		this.host = DEFAULT_HOST;
		this.port = DEFAULT_PORT;
		this.monitor = DEFAULT_MONITOR;
	}

	@Override
	protected JSONStore openJSONStore(String identifier) throws IOException {

		// check identifier

		String prefix = new String(Base64.encodeBase64(identifier.getBytes("UTF-8")), "UTF-8") + ".";

		// create jedis

		JedisPool jedisPool = this.getPort() == null ? new JedisPool(this.getHost()) : new JedisPool(this.getHost(), this.getPort().intValue());
		Jedis monitorJedis;

		if (this.isMonitor()) {

			monitorJedis = this.getPort() == null ? new Jedis(this.getHost()) : new Jedis(this.getHost(), this.getPort().intValue());
		} else {

			monitorJedis = null;
		}

		// open store

		JSONStore jsonStore;

		jsonStore = new RedisJSONStore(jedisPool, monitorJedis, prefix);
		jsonStore.init();

		// done

		return jsonStore;
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
