package com.ehborisov.udf;


import com.google.common.net.InetAddresses;

import java.net.InetAddress;

class UdfUtil {

    static Integer convertIPv4AddressToLong(final String address) {
        InetAddress addr = InetAddresses.forString(address);
        return InetAddresses.coerceToInteger(addr);
    }
}
