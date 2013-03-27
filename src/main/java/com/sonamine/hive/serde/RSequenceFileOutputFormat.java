package com.sonamine.hive.serde;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.util.Progressable;

public class RSequenceFileOutputFormat 
	extends SequenceFileOutputFormat<TypedBytesWritable, TypedBytesWritable>     
	implements HiveOutputFormat<TypedBytesWritable, TypedBytesWritable> {

	  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
		      Class<? extends Writable> valueClass, boolean isCompressed,
		      Properties tableProperties, Progressable progress) throws IOException {

		    FileSystem fs = finalOutPath.getFileSystem(jc);
		    final SequenceFile.Writer outStream = Utilities.createSequenceWriter(jc,
		        fs, finalOutPath, BytesWritable.class, valueClass, isCompressed);

		    return new RecordWriter() {
		      public void write(Writable r) throws IOException {
		        outStream.append(new BytesWritable(), r);
		      }

		      public void close(boolean abort) throws IOException {
		        outStream.close();
		      }
		    };
		  }


}
