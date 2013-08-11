package xdi2.core.impl.redis;

import java.io.IOException;

import xdi2.core.GraphFactory;
import xdi2.core.impl.keyvalue.AbstractKeyValueGraphFactory;
import xdi2.core.impl.keyvalue.KeyValueStore;

/**
 * GraphFactory that creates graphs in Redis.
 * 
 * @author markus
 */
public class RedisGraphFactory extends AbstractKeyValueGraphFactory implements GraphFactory {

	public static final boolean DEFAULT_SUPPORT_GET_CONTEXTNODES = true; 
	public static final boolean DEFAULT_SUPPORT_GET_RELATIONS = true; 

	public static final String DEFAULT_PROPERTIES_PATH = "xdi2-graph.properties";

	private String path;

	public RedisGraphFactory() {

		super(DEFAULT_SUPPORT_GET_CONTEXTNODES, DEFAULT_SUPPORT_GET_RELATIONS);

		this.path = DEFAULT_PROPERTIES_PATH;
	}

	@Override
	protected KeyValueStore openKeyValueStore(String identifier) throws IOException {

		// check identifier

		if (identifier != null) {

			this.setPath("xdi2-graph." + identifier + ".properties");
		}

		// open store

		KeyValueStore keyValueStore;

		keyValueStore = new RedisKeyValueStore(this.path);
		keyValueStore.init();

		// done

		return keyValueStore;
	}

	public String getPath() {

		return this.path;
	}

	public void setPath(String path) {

		this.path = path;
	}
}
