package xdi2.core.impl.redis;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private String path;

	public RedisKeyValueStore(String path) {

		this.path = path;

	}

	@Override
	public void init() throws IOException {

	}

	@Override
	public void close() {

	}

	@Override
	public void put(String key, String value) {
		// TODO Auto-generated method stub

	}

	@Override
	public Iterator<String> getAll(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(String key, String value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	public String getPath() {

		return this.path;
	}
}
