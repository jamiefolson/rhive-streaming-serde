package com.sonamine.hive.serde;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;

public class RStreamingSerDe extends RBaseSerDe {

	  @Override
	  public void initialize(Configuration conf, Properties tbl)
	      throws SerDeException {
		  super.initialize(conf, tbl);
		  this.wrapKeys = false;
		  this.numKeys = 0;
	  }
}
