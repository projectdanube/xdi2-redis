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
import redis.clients.jedis.Pipeline;
import xdi2.core.impl.json.AbstractJSONStore;
import xdi2.core.impl.json.JSONStore;
import xdi2.core.impl.keyvalue.redis.RedisKeyValueStore.MyJedisMonitorThread;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class RedisJSONStore extends AbstractJSONStore implements JSONStore {

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

	private static final Logger log = LoggerFactory.getLogger(RedisJSONStore.class);

	private Jedis jedis;
	private String prefix;

	private MyJedisMonitorThread jedisMonitorThread;

	public RedisJSONStore(Jedis jedis, Jedis monitorJedis, String prefix) {

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
	public void init() {

	}

	@Override
	public void close() {

	}

	@Override
	public JsonObject load(String id) throws IOException {

		String string = this.getJedis().get(this.getPrefix() + id);

		return fromRedisString(string);
	}

	@Override
	public Map<String, JsonObject> loadWithPrefix(String id) throws IOException {

		List<String> keys = new ArrayList<String> (this.getJedis().keys(toRedisStartsWithPattern(this.getPrefix() + id)));

		Pipeline pipeline = this.getJedis().pipelined();
		for (String key : keys) pipeline.get(key);
		List<Object> objects = pipeline.syncAndReturnAll();

		if (keys.size() != objects.size()) {

			log.warn("Unexpected list size " + keys.size() + " != " + objects.size() + ".");
			return Collections.singletonMap(id, this.load(id));
		}

		Map<String, JsonObject> map = new HashMap<String, JsonObject> ();

		for (int i=0; i<keys.size(); i++) {

			String key = keys.get(i).substring(this.getPrefix().length());
			JsonObject jsonObject = fromRedisString((String) objects.get(i));

			if (key == null || jsonObject == null) {

				log.warn("Null key or object " + keys + " -> " + jsonObject + ".");
				continue;
			}

			map.put(key, jsonObject);
		}

		return map;
	}

	@Override
	public void save(String id, JsonObject jsonObject) throws IOException {

		String string = toRedisString(jsonObject);

		this.getJedis().set(this.getPrefix() + id, string);
	}

	@Override
	public void delete(final String id) throws IOException {

		Set<String> keys = this.getJedis().keys(toRedisStartsWithPattern(this.getPrefix() + id));

		Pipeline pipeline = this.getJedis().pipelined();
		for (String key : keys) pipeline.del(key);
		pipeline.sync();
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
}
