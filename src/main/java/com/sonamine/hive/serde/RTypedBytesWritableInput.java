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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
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
import org.apache.hadoop.util.ReflectionUtils;
import static com.sonamine.hive.serde.RType.*;

/**
 * Provides functionality for reading typed bytes as Writable objects.
 *
 * @see TypedBytesInput
 */
public class RTypedBytesWritableInput implements Configurable {

  private RTypedBytesInput in;
  private Configuration conf;

  private RTypedBytesWritableInput() {
    conf = new Configuration();
  }

  private void setTypedBytesInput(RTypedBytesInput in) {
    this.in = in;
  }

  private static ThreadLocal tbIn = new ThreadLocal() {
    @Override
    protected synchronized Object initialValue() {
      return new RTypedBytesWritableInput();
    }
  };

  /**
   * Get a thread-local typed bytes writable input for the supplied
   * {@link TypedBytesInput}.
   *
   * @param in
   *          typed bytes input object
   * @return typed bytes writable input corresponding to the supplied
   *         {@link TypedBytesInput}.
   */
  public static RTypedBytesWritableInput get(RTypedBytesInput in) {
    RTypedBytesWritableInput bin = (RTypedBytesWritableInput) tbIn.get();
    bin.setTypedBytesInput(in);
    return bin;
  }
  
  public RTypedBytesInput getInput() {
	  return in;
  }

  /**
   * Get a thread-local typed bytes writable input for the supplied
   * {@link DataInput}.
   *
   * @param in
   *          data input object
   * @return typed bytes writable input corresponding to the supplied
   *         {@link DataInput}.
   */
  public static RTypedBytesWritableInput get(DataInput in) {
    return get(RTypedBytesInput.get(in));
  }

  /** Creates a new instance of TypedBytesWritableInput. */
  public RTypedBytesWritableInput(RTypedBytesInput in) {
    this();
    this.in = in;
  }

  /** Creates a new instance of TypedBytesWritableInput. */
  public RTypedBytesWritableInput(DataInput din) {
    this(new RTypedBytesInput(din));
  }
  public Writable read() throws IOException {
	  return read(null);
  }

  public Writable read(Writable w) throws IOException {
    final RType type = in.readType();
    if (type == null) {
      return null;
    }
    return read(type,w);
  }
  public Writable readRaw() throws IOException {
	  return this.readRaw(in.readType(), null);
  }
  public Writable readRaw(RType type) throws IOException {
	  return this.readRaw(type, null);
  }
  public Writable readRaw(RType type, Writable w) throws IOException {
	  byte[] bytes = this.in.readRaw(type.code);
      bytes[0] = (byte)type.code;
      if (w == null){
		  w = new TypedBytesWritable(bytes);
	  }else {
		  ((TypedBytesWritable)w).set(bytes, 0, bytes.length);
	  }
      return w;
  }

  public Writable read(RType type, Writable w) throws IOException {
    // can't use switch because not final??
    if(type.code==BYTES.code){
      return readBytes((BytesWritable) w);
    }else if(type.code ==  BYTE.code){
      return readByte((ByteWritable) w);
    }else if(type.code ==  BOOL.code){
      return readBoolean((BooleanWritable) w);
    }else if(type.code ==  INT.code){
      return readInt((IntWritable) w);
    }else if(type.code ==  SHORT.code){
      return readShort((ShortWritable) w);
    }else if(type.code ==  LONG.code){
      return readLong((LongWritable) w);
    }else if(type.code ==  FLOAT.code){
      return readFloat((FloatWritable) w);
    }else if(type.code ==  DOUBLE.code){
      return readDouble((DoubleWritable) w);
    }else if(type.code ==  STRING.code){
      return readText((Text) w);
    }else if(type.code ==  VECTOR.code){
      return readVector((ArrayWritable) w);
    }else if(type.code ==  MAP.code){
      return readMap((MapWritable) w);
    }else if(type.code ==  WRITABLE.code){
      return readWritable(w);
    }else if(type.code ==  ENDOFRECORD.code){
      return null;
    }else if(type.code ==  NULL.code){
      return NullWritable.get();
    }else if (type.code >= 50 && type.code <= 200){
      byte[] bytes = this.in.readRaw(type.code);
      bytes[0] = (byte)type.code;
      ((TypedBytesWritable)w).set(bytes, 0, bytes.length);
      return w;
    }else {
      throw new RuntimeException("unknown type");
    }
  }

  public RType readTypeCode() throws IOException {
    return in.readType();
  }

  /*public Class<? extends Writable> getType(RType type) throws IOException {
    	  
    if (type == null) {
      return null;
    }
    // can't use switch because java thinks they're not final
    if(type.code ==  BYTES.code){
      return BytesWritable.class;
    }else if(type.code ==  BYTE.code){
      return ByteWritable.class;
    }else if(type.code ==  BOOL.code){
      return BooleanWritable.class;
    }else if(type.code ==  INT.code){
      return VIntWritable.class;
    }else if(type.code ==  LONG.code){
      return VLongWritable.class;
    }else if(type.code ==  FLOAT.code){
      return FloatWritable.class;
    }else if(type.code ==  SHORT.code){
      return ShortWritable.class;
    }else if(type.code ==  DOUBLE.code){
      return DoubleWritable.class;
    }else if(type.code ==  STRING.code){
      return Text.class;
    }else if(type.code ==  VECTOR.code){
      return ArrayWritable.class;
    }else if(type.code ==  MAP.code){
      return MapWritable.class;
    }else if(type.code ==  WRITABLE.code){
      return Writable.class;
    }else if(type.code ==  ENDOFRECORD.code){
      return null;
    }else if(type.code ==  NULL.code){
      return NullWritable.class;
    }else if (type.code >=50 && type.code <= 200){
    	return BytesWritable.class;
    }else {
      throw new RuntimeException("unknown type");
    }
  }*/

  public BytesWritable readBytes(BytesWritable bw) throws IOException {
    byte[] bytes = in.readBytes();
    if (bw == null) {
      bw = new BytesWritable(bytes);
    } else {
      bw.set(bytes, 0, bytes.length);
    }
    return bw;
  }

  public BytesWritable readBytes() throws IOException {
    return readBytes(null);
  }

  public ByteWritable readByte(ByteWritable bw) throws IOException {
    if (bw == null) {
      bw = new ByteWritable();
    }
    bw.set(in.readByte());
    return bw;
  }

  public ByteWritable readByte() throws IOException {
    return readByte(null);
  }

  public BooleanWritable readBoolean(BooleanWritable bw) throws IOException {
    if (bw == null) {
      bw = new BooleanWritable();
    }
    bw.set(in.readBool());
    return bw;
  }

  public BooleanWritable readBoolean() throws IOException {
    return readBoolean(null);
  }

  public IntWritable readInt(IntWritable iw) throws IOException {
    if (iw == null) {
      iw = new IntWritable();
    }
    int val = in.readInt();
    if (val == RType.NA_INTEGER){
    	return null;
    }
    iw.set(val);
    return iw;
  }

  public IntWritable readInt() throws IOException {
    return readInt(null);
  }

  public ShortWritable readShort(ShortWritable sw) throws IOException {
    if (sw == null) {
      sw = new ShortWritable();
    }
    sw.set(in.readShort());
    return sw;
  }

  public ShortWritable readShort() throws IOException {
    return readShort(null);
  }

  public VIntWritable readVInt(VIntWritable iw) throws IOException {
    if (iw == null) {
      iw = new VIntWritable();
    }
    iw.set(in.readInt());
    return iw;
  }

  public VIntWritable readVInt() throws IOException {
    return readVInt(null);
  }

  public LongWritable readLong(LongWritable lw) throws IOException {
    if (lw == null) {
      lw = new LongWritable();
    }
    lw.set(in.readLong());
    return lw;
  }

  public LongWritable readLong() throws IOException {
    return readLong(null);
  }

  public VLongWritable readVLong(VLongWritable lw) throws IOException {
    if (lw == null) {
      lw = new VLongWritable();
    }
    lw.set(in.readLong());
    return lw;
  }

  public VLongWritable readVLong() throws IOException {
    return readVLong(null);
  }

  public FloatWritable readFloat(FloatWritable fw) throws IOException {
    if (fw == null) {
      fw = new FloatWritable();
    }
    fw.set(in.readFloat());
    return fw;
  }

  public FloatWritable readFloat() throws IOException {
    return readFloat(null);
  }

  public DoubleWritable readDouble(DoubleWritable dw) throws IOException {
    if (dw == null) {
      dw = new DoubleWritable();
    }
    long val =in.readLong();
    if (val == RType.NA_REAL){
    	return null;
    }
    dw.set(Double.longBitsToDouble(val));
    return dw;
  }

  public DoubleWritable readDouble() throws IOException {
    return readDouble(null);
  }

  public Text readText(Text t) throws IOException {
    if (t == null) {
      t = new Text();
    }
    t.set(in.readString());
    return t;
  }

  public Text readText() throws IOException {
    return readText(null);
  }
  
/*
  public ArrayWritable readVector(ArrayWritable aw) throws IOException {
	  if (aw == null) {
	        aw = new ArrayWritable(RTypedBytesWritable.class);
	  } else if (!aw.getValueClass().equals(RTypedBytesWritable.class)) {
	        throw new RuntimeException("value class has to be "+RTypedBytesWritable.class.getCanonicalName());
	  }
    int length = in.readVectorHeader();
    Writable[] writables = new Writable[length];
    for (int i = 0; i < length; i++) {
      Writable w = new RTypedBytesWritable(in.readRaw());
      writables[i] = w;
    }
    
    aw.set(writables);
    return aw;
  }
*/

  public ArrayWritable readVector(ArrayWritable aw) throws IOException {
	  if (aw == null) {
	        aw = new ArrayWritable(Writable.class);
	  }/* else if (!aw.getValueClass().equals(clazz)) {
	        throw new RuntimeException("value class has to be "+clazz.getCanonicalName());
	      }*/
    int length = in.readVectorHeader();
    Writable[] writables = new Writable[length];
    for (int i = 0; i < length; i++) {
      Writable w = null;
      if (aw.getValueClass() == TypedBytesWritable.class){
    	  w = readRaw();
      }else {
      w = this.read();
      if (w instanceof NullWritable){
    	  w = null;
      }
      }
      writables[i] = w;
    }
    
    aw.set(writables);
    return aw;
  }

  public ArrayWritable readVector() throws IOException {
    return readVector(null);
  }

  public MapWritable readMap(MapWritable mw) throws IOException {
    if (mw == null) {
      mw = new MapWritable();
    }
    int length = in.readMapHeader();
    for (int i = 0; i < length; i++) {
      Writable key = read();
      Writable value = read();
      mw.put(key, value);
    }
    return mw;
  }

  public MapWritable readMap() throws IOException {
    return readMap(null);
  }

  public SortedMapWritable readSortedMap(SortedMapWritable mw)
      throws IOException {
    if (mw == null) {
      mw = new SortedMapWritable();
    }
    int length = in.readMapHeader();
    for (int i = 0; i < length; i++) {
      WritableComparable key = (WritableComparable) read();
      Writable value = read();
      mw.put(key, value);
    }
    return mw;
  }

  public SortedMapWritable readSortedMap() throws IOException {
    return readSortedMap(null);
  }

  public Writable readWritable(Writable writable) throws IOException {
    DataInputStream dis = null;
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(in.readBytes());
      dis = new DataInputStream(bais);
      String className = WritableUtils.readString(dis);
      if (writable == null) {
        try {
          Class<? extends Writable> cls = conf.getClassByName(className)
              .asSubclass(Writable.class);
          writable = (Writable) ReflectionUtils.newInstance(cls, conf);
        } catch (ClassNotFoundException e) {
          throw new IOException(e);
        }
      } else if (!writable.getClass().getName().equals(className)) {
        throw new IOException("wrong Writable class given");
      }
      writable.readFields(dis);
      dis.close();
      dis = null;
      return writable;
    } finally {
      IOUtils.closeStream(dis);
    }
  }

  public Writable readWritable() throws IOException {
    return readWritable(null);
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  public NullWritable readNull() throws IOException {
	  int val = in.readInt();
	  if (val != RType.NA_INTEGER){
		  throw new RuntimeException("Non-null value in readNull()");
	  }
	  return null;
  }

}
