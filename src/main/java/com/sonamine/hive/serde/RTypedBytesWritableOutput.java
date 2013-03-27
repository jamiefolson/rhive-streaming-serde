/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sonamine.hive.serde;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Record;

/**
 * Provides functionality for writing Writable objects as typed bytes.
 *
 * @see TypedBytesOutput
 */
public class RTypedBytesWritableOutput {
public static final Log LOG = LogFactory.getLog("com.sonamine.hive.serde.RTypedBytesWritableOutput");

  private RTypedBytesOutput out;

  private RTypedBytesWritableOutput() {
  }

  private void setTypedBytesOutput(RTypedBytesOutput out) {
    this.out = out;
  }

  private static ThreadLocal tbOut = new ThreadLocal() {
    @Override
    protected synchronized Object initialValue() {
      return new RTypedBytesWritableOutput();
    }
  };

  /**
   * Get a thread-local typed bytes writable input for the supplied
   * {@link TypedBytesOutput}.
   *
   * @param out
   *          typed bytes output object
   * @return typed bytes writable output corresponding to the supplied
   *         {@link TypedBytesOutput}.
   */
  public static RTypedBytesWritableOutput get(RTypedBytesOutput out) {
    RTypedBytesWritableOutput bout = (RTypedBytesWritableOutput) tbOut.get();
    bout.setTypedBytesOutput(out);
    return bout;
  }
  
  public RTypedBytesOutput getOutput() {
	  return out;
  }

  /**
   * Get a thread-local typed bytes writable output for the supplied
   * {@link DataOutput}.
   *
   * @param out
   *          data output object
   * @return typed bytes writable output corresponding to the supplied
   *         {@link DataOutput}.
   */
  public static RTypedBytesWritableOutput get(DataOutput out) {
    return get(RTypedBytesOutput.get(out));
  }

  /** Creates a new instance of TypedBytesWritableOutput. */
  public RTypedBytesWritableOutput(RTypedBytesOutput out) {
    this();
    this.out = out;
  }

  /** Creates a new instance of TypedBytesWritableOutput. */
  public RTypedBytesWritableOutput(DataOutput dout) {
    this(new RTypedBytesOutput(dout));
  }
  
  public void writeRaw(Writable w, RType type) throws IOException {
    if (type == RType.BYTES) {
      //LOG.info("Writing as regular bytes: RTypedBytesWritableOutput.writeRaw");
      writeRawBytes((BytesWritable) w);
    } else if (type == RType.BYTE) {
      writeRawByte((ByteWritable) w);
    } else if (type == RType.BOOL) {
      writeRawBoolean((BooleanWritable) w);
    } else if (type == RType.INT){ 
    	if(w instanceof IntWritable) {
    		writeRawInt((IntWritable) w);
    	} else if (w instanceof VIntWritable) {
    		writeRawVInt((VIntWritable) w);
    	}else {
    		throw new RuntimeException("Cannot write raw INT stored in type: "+w.getClass());
    	}
    } else if (type == RType.LONG){ 
    	if (w instanceof LongWritable) {
      writeRawLong((LongWritable) w);
    } else if (w instanceof VLongWritable) {
      writeRawVLong((VLongWritable) w);
    }else {
		throw new RuntimeException("Cannot write raw LONG stored in type: "+w.getClass());
	}
    } else if (type == RType.FLOAT) {
      writeRawFloat((FloatWritable) w);
    } else if (type == RType.DOUBLE) {
      writeRawDouble((DoubleWritable) w);
    } else if (type == RType.STRING) {
      writeRawText((Text) w);
    } else if (type == RType.SHORT) {
      writeRawShort((ShortWritable) w);
    }else {
    	throw new RuntimeException("Cannot write raw object stored in type: "+w.getClass()+" as rtype: "+type.code); // last resort
    }
  }
 
  public void writeRawBytes(BytesWritable bw) throws IOException {
	//LOG.info("Writing a plain BytesWritable");
	  
    byte[] bytes = Arrays.copyOfRange(bw.getBytes(), 0, bw.getLength());
    out.writeRawBytes(bytes);
  }

  public void writeRawByte(ByteWritable bw) throws IOException {
    out.writeRawByte(bw.get());
  }

  public void writeRawBoolean(BooleanWritable bw) throws IOException {
    out.writeRawBool(bw.get());
  }

  public void writeRawInt(IntWritable iw) throws IOException {
    out.writeRawInt(iw.get());
  }

  public void writeRawVInt(VIntWritable viw) throws IOException {
    out.writeRawInt(viw.get());
  }

  public void writeRawLong(LongWritable lw) throws IOException {
    out.writeRawLong(lw.get());
  }

  public void writeRawVLong(VLongWritable vlw) throws IOException {
    out.writeRawLong(vlw.get());
  }

  public void writeRawFloat(FloatWritable fw) throws IOException {
    out.writeRawFloat(fw.get());
  }

  public void writeRawDouble(DoubleWritable dw) throws IOException {
    out.writeRawDouble(dw.get());
  }

  public void writeRawShort(ShortWritable sw) throws IOException {
    out.writeRawShort(sw.get());
  }

  public void writeRawText(Text t) throws IOException {
    out.writeRawString(t.toString());
  }
  
  public void write(Writable w) throws IOException {
    if (w instanceof TypedBytesWritable) {
      //LOG.info("Writing as typedbytes: RTypedBytesWritableOutput.write");
      writeTypedBytes((TypedBytesWritable) w);
    } else if (w instanceof BytesWritable) {
      //LOG.info("Writing as regular bytes: RTypedBytesWritableOutput.write");
      writeBytes((BytesWritable) w);
    } else if (w instanceof ByteWritable) {
      writeByte((ByteWritable) w);
    } else if (w instanceof BooleanWritable) {
      writeBoolean((BooleanWritable) w);
    } else if (w instanceof IntWritable) {
      writeInt((IntWritable) w);
    } else if (w instanceof VIntWritable) {
      writeVInt((VIntWritable) w);
    } else if (w instanceof LongWritable) {
      writeLong((LongWritable) w);
    } else if (w instanceof VLongWritable) {
      writeVLong((VLongWritable) w);
    } else if (w instanceof FloatWritable) {
      writeFloat((FloatWritable) w);
    } else if (w instanceof DoubleWritable) {
      writeDouble((DoubleWritable) w);
    } else if (w instanceof Text) {
      writeText((Text) w);
    } else if (w instanceof ShortWritable) {
      writeShort((ShortWritable) w);
    } else if (w instanceof ArrayWritable) {
      writeVector((ArrayWritable) w);
    } else if (w instanceof MapWritable) {
      writeMap((MapWritable) w);
    } else if (w instanceof SortedMapWritable) {
      writeSortedMap((SortedMapWritable) w);
    } else if (w instanceof Record) {
      writeRecord((Record) w);
    } else if (w instanceof NullWritable || w == null) {
      writeNull();
    } else {
      writeWritable(w); // last resort
    }
  }
  public void writeList(List l, PrimitiveObjectInspector elemOI) throws IOException {
	  out.writeListHeader();
	  for (Object o : l){
		  write((Writable) elemOI.getPrimitiveWritableObject(o));
	  }
	  out.writeListFooter();
  }
  public void writeList(List l) throws IOException {
	  out.writeListHeader();
	  for (Object o : l){
		  write((Writable) o);
	  }
	  out.writeListFooter();
  }

  public void writeVector(List l, PrimitiveObjectInspector elemOI) throws IOException {
	  out.writeVectorHeader(l.size());
	  for (Object o : l){
		  write((Writable) elemOI.getPrimitiveWritableObject(o));
	  }
  }
  public void writeVector(List l) throws IOException {
	  out.writeVectorHeader(l.size());
	  for (Object o : l){
		  write((Writable) o);
	  }
  }

  public void writeTypedBytes(TypedBytesWritable tbw) throws IOException {
	  //LOG.info("Writing a raw TypedBytesWritable");
     out.writeTypedBytes(tbw);
  }

  public void writeBytes(BytesWritable bw) throws IOException {
	//LOG.info("Writing a plain BytesWritable");
	  
    byte[] bytes = Arrays.copyOfRange(bw.getBytes(), 0, bw.getLength());
    out.writeBytes(bytes);
  }

  public void writeByte(ByteWritable bw) throws IOException {
    out.writeByte(bw.get());
  }

  public void writeBoolean(BooleanWritable bw) throws IOException {
    out.writeBool(bw.get());
  }

  public void writeInt(IntWritable iw) throws IOException {
    out.writeInt(iw.get());
  }

  public void writeVInt(VIntWritable viw) throws IOException {
    out.writeInt(viw.get());
  }

  public void writeLong(LongWritable lw) throws IOException {
    out.writeLong(lw.get());
  }

  public void writeVLong(VLongWritable vlw) throws IOException {
    out.writeLong(vlw.get());
  }

  public void writeFloat(FloatWritable fw) throws IOException {
    out.writeFloat(fw.get());
  }

  public void writeDouble(DoubleWritable dw) throws IOException {
    out.writeDouble(dw.get());
  }

  public void writeShort(ShortWritable sw) throws IOException {
    out.writeShort(sw.get());
  }

  public void writeText(Text t) throws IOException {
    out.writeString(t.toString());
  }

  public void writeVector(ArrayWritable aw) throws IOException {
    Writable[] writables = aw.get();
    out.writeVectorHeader(writables.length);
    for (Writable writable : writables) {
      write(writable);
    }
  }

  public void writeMap(MapWritable mw) throws IOException {
    out.writeMapHeader(mw.size());
    for (Map.Entry<Writable, Writable> entry : mw.entrySet()) {
      write(entry.getKey());
      write(entry.getValue());
    }
  }

  public void writeSortedMap(SortedMapWritable smw) throws IOException {
    out.writeMapHeader(smw.size());
    for (Map.Entry<WritableComparable, Writable> entry : smw.entrySet()) {
      write(entry.getKey());
      write(entry.getValue());
    }
  }

  public void writeRecord(Record r) throws IOException {
    r.serialize(RTypedBytesRecordOutput.get(out));
  }

  public void writeNull() throws IOException {
    out.writeNull();
  }

  public void writeWritable(Writable w) throws IOException {
    DataOutputStream dos = null;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      dos = new DataOutputStream(baos);
      WritableUtils.writeString(dos, w.getClass().getName());
      w.write(dos);
      out.writeBytes(baos.toByteArray(), RType.WRITABLE.code);
      dos.close();
      dos = null;
    } finally {
      IOUtils.closeStream(dos);
    }
  }
  
  /*private boolean writeArray(ArrayWritable o){
	  return writeArray(o,o.getValueClass());
  }*/
  public void writeArray(List l, PrimitiveObjectInspector elemOI) throws IOException {
	  RType type = RType.valueOf(((PrimitiveObjectInspector)elemOI).getPrimitiveCategory());
	  int typecode = type.code;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
      RTypedBytesWritableOutput rtbo = RTypedBytesWritableOutput.get(new DataOutputStream(baos));
       
      for (Object o : l){
        rtbo.writeRaw((Writable) elemOI.getPrimitiveWritableObject(o), type);
      }
      out.writeArray(typecode, baos.toByteArray());
  }
  public void writeArray(ArrayWritable o, RType type) throws IOException{
	  
	  	int typecode = type.code;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RTypedBytesWritableOutput rtbo = RTypedBytesWritableOutput.get(new DataOutputStream(baos));
         
        for (Writable w : o.get()){
          rtbo.writeRaw(w, type);
         }
		out.writeArray(typecode, baos.toByteArray());
	}		

/*  public void writeArray(ArrayWritable o, RType type) throws IOException{
	  
	  	int length = o.get().length;
		int rawlength = -1;
		int typecode = -1;
		switch (type) {
	        case BYTE:{
	          rawlength = length;
	          typecode = 1;}
	        break;
	        case BOOL:{
	          rawlength = length;
	          typecode = 2;}
	        break;
	        case INT:{
	          rawlength = (length)*4;
	          typecode = 3;}
	        break;
	        case LONG:{
	          rawlength = (length)*8;
	          typecode = 4;}
	        break;
	        case FLOAT:
	        case DOUBLE:
	          rawlength = (length)*8;
	          typecode = 6;
	          break;
	        case STRING:
	        	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	            RTypedBytesWritableOutput rtbo = RTypedBytesWritableOutput.get(new DataOutputStream(baos));
	            
	            for (Writable w : o.get()){
	          	  rtbo.write(w);
	            }
	            rawlength = baos.size();
	            typecode = 8;
	        break;
	        default:
	        	throw new UnsupportedOperationException("Cannot make typedbytes array(145) out of: "+type);
	    	}
			out.writeArrayHeader(rawlength, typecode);
			for (Writable elem : o.get()){
				out.write(elem);
			}
			out.writeArray(o.get());
	}		
*/
  public void writeEndOfRecord() throws IOException {
    ;//out.writeEndOfRecord();
  }
}
