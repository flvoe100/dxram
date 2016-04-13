
package de.hhu.bsinfo.dxcompute.ms;

public enum ComputeRole {
	// Constants
	MASTER('M'), SLAVE('S'), NONE('N');

	public static final String MASTER_STR = "master";
	public static final String SLAVE_STR = "slave";
	public static final String NONE_STR = "none";

	// Attributes
	private char m_acronym;

	// Constructors
	/**
	 * Creates an instance of Role
	 * @param p_acronym
	 *            the role's acronym
	 */
	ComputeRole(final char p_acronym) {
		m_acronym = p_acronym;
	}

	/**
	 * Get the node role from a full string.
	 * @param p_str
	 *            String to parse.
	 * @return Role node of string.
	 */
	public static ComputeRole toComputeRole(final String p_str) {
		String str = p_str.toLowerCase();
		if (str.equals(MASTER_STR) || str.equals("m")) {
			return ComputeRole.MASTER;
		} else if (str.equals(SLAVE_STR) || str.equals("s")) {
			return ComputeRole.SLAVE;
		} else {
			return ComputeRole.NONE;
		}
	}

	// Getters
	/**
	 * Gets the acronym of the role
	 * @return the acronym
	 */
	public char getAcronym() {
		return m_acronym;
	}

	@Override
	public String toString() {
		if (equals(MASTER)) {
			return MASTER_STR;
		} else if (equals(SLAVE)) {
			return SLAVE_STR;
		} else {
			return NONE_STR;
		}
	}

	// Methods
	/**
	 * Gets the role for the given acronym
	 * @param p_acronym
	 *            the acronym
	 * @return the corresponding role
	 */
	public static ComputeRole getRoleByAcronym(final char p_acronym) {
		ComputeRole ret = null;

		for (ComputeRole role : values()) {
			if (role.m_acronym == p_acronym) {
				ret = role;

				break;
			}
		}

		return ret;
	}
}