package de.hhu.bsinfo.dxram.loading.util;

public class Util {

    public static long getIntervalSize(long p_wholeSize, int p_numberOfIntervals, int p_forWhom) {
        if (p_wholeSize % p_numberOfIntervals == 0) {
            return p_wholeSize / p_numberOfIntervals;
        }

        //    p_wholeSize % p_numberOfIntervals
        return 0;
    }
}
