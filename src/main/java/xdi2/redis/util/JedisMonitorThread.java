package xdi2.redis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;

public class JedisMonitorThread extends Thread {

	private static final Logger log = LoggerFactory.getLogger(JedisMonitorThread.class);

	private StringBuffer logBuffer;
	private Jedis monitorJedis;

	public JedisMonitorThread(Jedis monitorJedis) {

		this.logBuffer = new StringBuffer();
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

					JedisMonitorThread.this.logBuffer.append(command + "\n");
				}
			});
		} catch (Exception ex) {

			if (log.isDebugEnabled()) log.debug("Stopping monitor.");
		}
	}

	public StringBuffer getLogBuffer() {

		return this.logBuffer;
	}

	public Jedis getMonitorJedis() {

		return this.monitorJedis;
	}

	public void resetLogBuffer() {

		this.logBuffer = new StringBuffer();
	}
}