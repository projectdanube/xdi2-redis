package xdi2.core.impl.json.redis;

import java.io.IOException;
import java.util.Set;

import redis.clients.jedis.Jedis;
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
	public void save(String id, JsonObject jsonObject) throws IOException {

		String string = toRedisString(jsonObject);

		this.getJedis().set(this.getPrefix() + id, string);
	}

	@Override
	public void delete(final String id) throws IOException {

		Set<String> keys = this.getJedis().keys(toRedisStartsWithPattern(this.getPrefix() + id));

		for (String key : keys) {
			
			System.out.println(key);
			this.getJedis().del(key);
		}
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
