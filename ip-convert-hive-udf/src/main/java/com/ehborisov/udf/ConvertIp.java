package com.ehborisov.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;



/**
 * ConvertIp returns a long numerical form of a given IPv4 address string.
 */
@Description(
        name = "convert_ip",
        value = "_FUNC_(str) -  Attempts to extract a numerical decimal form of the given IPv4" +
                " address string.")
public class ConvertIp extends UDF {

    public LongWritable evaluate(final Text address) {

        if (address == null  || "".equals(address.toString())){
            return null;
        }
        return new LongWritable(UdfUtil.convertIPv4AddressToLong(address.toString()));
    }
}
