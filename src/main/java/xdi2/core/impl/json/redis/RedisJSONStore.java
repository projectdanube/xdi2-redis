package xdi2.core.impl.json.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import xdi2.core.impl.json.AbstractJSONStore;
import xdi2.core.impl.json.JSONStore;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueStore;
import xdi2.redis.util.JedisMonitorThread;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class RedisJSONStore extends AbstractJSONStore implements JSONStore {

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

	private static final Logger log = LoggerFactory.getLogger(RedisJSONStore.class);

	private JedisPool jedisPool;
	private String prefix;

	private JedisMonitorThread jedisMonitorThread;

	public RedisJSONStore(JedisPool jedisPool, Jedis monitorJedis, String prefix) {

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
	public void init() {

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
	public JsonObject load(String id) throws IOException {

		Jedis jedis = null;
		JsonObject jsonObject = null;

		try {

			jedis = this.getJedisPool().getResource();

			jsonObject = fromRedisString(jedis.get(this.getPrefix() + id));
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}

		return jsonObject;
	}

	@Override
	public Map<String, JsonObject> loadWithPrefix(String id) throws IOException {

		Jedis jedis = null;
		Map<String, JsonObject> map = null;

		try {

			jedis = this.getJedisPool().getResource();

			List<String> keys = new ArrayList<String> (jedis.keys(toRedisStartsWithPattern(this.getPrefix() + id)));

			Pipeline pipeline = jedis.pipelined();
			for (String key : keys) pipeline.get(key);
			List<Object> objects = pipeline.syncAndReturnAll();

			if (keys.size() != objects.size()) {

				log.warn("Unexpected list size " + keys.size() + " != " + objects.size() + ".");

				String string = jedis.get(this.getPrefix() + id);
				map = Collections.singletonMap(id, fromRedisString(string));
			} else {

				map = new HashMap<String, JsonObject> ();

				for (int i=0; i<keys.size(); i++) {

					String key = keys.get(i).substring(this.getPrefix().length());
					JsonObject jsonObject = fromRedisString((String) objects.get(i));

					if (key == null || jsonObject == null) {

						log.warn("Null key or object " + keys + " -> " + jsonObject + ".");
						continue;
					}

					map.put(key, jsonObject);
				}
			}
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}

		return map;
	}

	@Override
	public void save(String id, JsonObject jsonObject) throws IOException {

		Jedis jedis = null;

		try {

			jedis = this.getJedisPool().getResource();

			String string = toRedisString(jsonObject);

			jedis.set(this.getPrefix() + id, string);
		} finally {

			if (jedis != null) this.getJedisPool().returnResource(jedis);
		}
	}

	@Override
	public void delete(final String id) throws IOException {

		Jedis jedis = null;

		try {

			jedis = this.getJedisPool().getResource();

			Set<String> keys = jedis.keys(toRedisStartsWithPattern(this.getPrefix() + id));

			Pipeline pipeline = jedis.pipelined();
			for (String key : keys) pipeline.del(key);
			pipeline.sync();
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

	private String toRedisStartsWithPattern(String string) {

		StringBuilder buffer = new StringBuilder();

		buffer.append(string
				.replace("?", "\\?")
				.replace("[", "\\[")
				.replace("]", "\\]")
				.replace("*", "\\*"));

		buffer.append("*");

		return buffer.toString();
	}

	private JsonObject fromRedisString(String string) throws IOException {

		if (string == null) return null;

		return (JsonObject) gson.getAdapter(JsonArray.class).fromJson("[" + string + "]").get(0);
	}

	private String toRedisString(JsonElement jsonElement) {

		return gson.toJson(jsonElement);
	}

	/*
	 * Helper methods
	 */

	public static void cleanup(String host) {

		cleanup(host, null);
	}

	public static void cleanup(String host, Integer port) {

		try {

			Jedis jedis = port == null ? new Jedis(host) : new Jedis(host, port.intValue());

			jedis.flushDB();
		} catch (Exception ex) {

			throw new RuntimeException(ex.getMessage(), ex);
		}
	}
}
