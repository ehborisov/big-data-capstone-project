package com.ehborisov.udf;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/**
 * SubnetRangeEnd returns a long numerical form of a given subnet in CIDR notation.
 */
@Description(
        name = "subnet_range_start",
        value = "_FUNC_(str) -  Attempts to extract start of range in numerical decimal form from the given IPv4 " +
                "subnet string in CIDR notation.")
public class SubnetRangeStart extends UDF {

    public LongWritable evaluate(final Text subnet) {

        if (subnet == null || "".equals(subnet.toString())){
            return null;
        }

        SubnetUtils utils = new SubnetUtils(subnet.toString());
        Integer value = UdfUtil.convertIPv4AddressToLong(utils.getInfo().getLowAddress());
        return new LongWritable(value);
    }
}
