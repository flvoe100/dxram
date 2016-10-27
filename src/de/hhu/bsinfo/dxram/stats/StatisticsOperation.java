package de.hhu.bsinfo.dxram.stats;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A single operation tracks time, counters, averages etc
 * of one task/method call, for example: memory management
 * -> alloc operation
 * Each operation is part of a recorder.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 */
public final class StatisticsOperation {
	public static final int INVALID_ID = -1;

	private String m_name;
	private boolean m_enabled = true;

	// stats per thread, avoids having locks
	private static int ms_blockSizeStatsMap = 100;
	private Stats[][] m_statsMap = new Stats[ms_blockSizeStatsMap][];
	private int m_statsMapBlockPos;
	private Lock m_mapLock = new ReentrantLock(false);

	/**
	 * Constructor
	 *
	 * @param p_name Name of the operation.
	 */
	StatisticsOperation(final String p_name) {
		m_name = p_name;

		m_statsMap[0] = new Stats[ms_blockSizeStatsMap];
		m_statsMapBlockPos = 1;
	}

	/**
	 * Get the name of the operation
	 *
	 * @return Name
	 */
	public String getName() {
		return m_name;
	}

	/**
	 * Check if the operation is enabled
	 *
	 * @return True if enabled, false otherwise
	 */
	public boolean isEnabled() {
		return m_enabled;
	}

	/**
	 * Enable/disable recording of the operation
	 *
	 * @param p_val True to enable, false to disable
	 */
	public void setEnabled(final boolean p_val) {
		m_enabled = p_val;
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 */
	public void enter() {
		if (!m_enabled) {
			return;
		}

		long threadId = Thread.currentThread().getId();
		if (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
			m_mapLock.lock();
			while (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
				m_statsMap[m_statsMapBlockPos++] = new Stats[ms_blockSizeStatsMap];
			}
			m_mapLock.unlock();
		}

		Stats stats = m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];
		if (stats == null) {
			stats = new Stats(Thread.currentThread().getName());
			m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)] = stats;
		}

		stats.m_opCount++;
		stats.m_timeNsStart = System.nanoTime();
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 *
	 * @param p_val Value to added to the long counter.
	 */
	public void enter(final long p_val) {
		if (!m_enabled) {
			return;
		}

		long threadId = Thread.currentThread().getId();
		if (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
			m_mapLock.lock();
			while (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
				m_statsMap[m_statsMapBlockPos++] = new Stats[ms_blockSizeStatsMap];
			}
			m_mapLock.unlock();
		}

		Stats stats = m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];
		if (stats == null) {
			stats = new Stats(Thread.currentThread().getName());
			m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)] =
					stats;
		}

		stats.m_opCount++;
		stats.m_timeNsStart = System.nanoTime();
		stats.m_counter += p_val;
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 *
	 * @param p_val Value to added to the double counter.
	 */
	public void enter(final double p_val) {
		if (!m_enabled) {
			return;
		}

		long threadId = Thread.currentThread().getId();
		if (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
			m_mapLock.lock();
			while (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
				m_statsMap[m_statsMapBlockPos++] = new Stats[ms_blockSizeStatsMap];
			}
			m_mapLock.unlock();
		}

		Stats stats = m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];
		if (stats == null) {
			stats = new Stats(Thread.currentThread().getName());
			m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)] = stats;
		}

		stats.m_opCount++;
		stats.m_timeNsStart = System.nanoTime();
		stats.m_counter2 += p_val;
	}

	/**
	 * Call this when/after you ended/left the call/operation.
	 */
	public void leave() {
		if (!m_enabled) {
			return;
		}

		long threadId = Thread.currentThread().getId();
		if (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
			m_mapLock.lock();
			while (threadId >= m_statsMapBlockPos * ms_blockSizeStatsMap) {
				m_statsMap[m_statsMapBlockPos++] = new Stats[ms_blockSizeStatsMap];
			}
			m_mapLock.unlock();
		}

		Stats stats = m_statsMap[(int) (threadId / ms_blockSizeStatsMap)][(int) (threadId % ms_blockSizeStatsMap)];

		long duration = System.nanoTime() - stats.m_timeNsStart;
		stats.m_totalTimeNs += duration;
		if (duration < stats.m_shortestTimeNs) {
			stats.m_shortestTimeNs = duration;
		}
		if (duration > stats.m_longestTimeNs) {
			stats.m_longestTimeNs = duration;
		}
	}

	@Override
	public String toString() {
		String str = "[" + m_name + " (enabled "
				+ m_enabled + "): ";
		for (int i = 0; i < m_statsMapBlockPos; i++) {
			for (int j = 0; j < ms_blockSizeStatsMap; j++) {
				if (m_statsMap[i][j] != null) {
					str += "\n\t\t" + m_statsMap[i][j];
				}
			}
		}

		return str;
	}

	/**
	 * Internal state for an operation for statistics.
	 *
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
	 */
	public static final class Stats {
		private String m_threadName = "";

		private long m_opCount;
		private long m_totalTimeNs;
		private long m_shortestTimeNs = Long.MAX_VALUE;
		private long m_longestTimeNs = Long.MIN_VALUE;
		private long m_counter;
		private double m_counter2;

		// temporary stuff
		private long m_timeNsStart;

		/**
		 * Constructor
		 *
		 * @param p_threadName Name of the thread
		 */
		private Stats(final String p_threadName) {
			m_threadName = p_threadName;
		}

		/**
		 * Get the operation count recorded (i.e. how often was enter called).
		 *
		 * @return Operation count.
		 */
		public long getOpCount() {
			return m_opCount;
		}

		/**
		 * Get the total amount of time we were in the enter/leave section in ns.
		 *
		 * @return Total time in ns.
		 */
		public long getTotalTimeNs() {
			return m_totalTimeNs;
		}

		/**
		 * Get the shortest time we spent in the enter/leave section in ns.
		 *
		 * @return Shortest time in ns.
		 */
		public long getShortestTimeNs() {
			return m_shortestTimeNs;
		}

		/**
		 * Get the longest time we spent in the enter/leave section in ns.
		 *
		 * @return Longest time in ns.
		 */
		public long getLongestTimeNs() {
			return m_longestTimeNs;
		}

		/**
		 * Get the avarage time we spent in the enter/leave section in ns.
		 *
		 * @return Avarage time in ns.
		 */
		public long getAvarageTimeNs() {
			return m_totalTimeNs / m_opCount;
		}

		/**
		 * Get the long counter. Depending on the operation, this is used for tracking different things.
		 *
		 * @return Long counter value.
		 */
		public long getCounter() {
			return m_counter;
		}

		/**
		 * Get the double counter. Depending on the operation, this is used for tracking different things.
		 *
		 * @return Double counter value.
		 */
		public double getCounter2() {
			return m_counter2;
		}

		/**
		 * Calculate the number of operations per second.
		 *
		 * @return Number of operations per second.
		 */
		public float getOpsPerSecond() {

			return (float) ((1000.0 * 1000.0 * 1000.0) / (((double) m_totalTimeNs) / m_opCount));
		}

		@Override
		public String toString() {
			return "Stats[" + m_threadName + "](m_opCount, " + m_opCount
					+ ")(m_totalTimeNs, " + m_totalTimeNs
					+ ")(m_shortestTimeNs, " + m_shortestTimeNs
					+ ")(m_longestTimeNs, " + m_longestTimeNs
					+ ")(avgTimeNs, " + getAvarageTimeNs()
					+ ")(opsPerSecond, " + getOpsPerSecond()
					+ ")(m_counter, " + m_counter
					+ ")(m_counter2, " + m_counter2 + ")";
		}
	}
}
