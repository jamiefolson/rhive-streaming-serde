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

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

/**
 * Provides functionality for writing typed bytes.
 */
public class RTypedBytesOutput {
	public static final Log LOG = LogFactory.getLog("com.sonamine.hive.serde.RTypedBytesOutput");

  private DataOutput out;

  private RTypedBytesOutput() {
  }

  private void setDataOutput(DataOutput out) {
    this.out = out;
  }

  private static ThreadLocal tbOut = new ThreadLocal() {
    @Override
    protected synchronized Object initialValue() {
      return new RTypedBytesOutput();
    }
  };

  /**
   * Get a thread-local typed bytes output for the supplied {@link DataOutput}.
   *
   * @param out
   *          data output object
   * @return typed bytes output corresponding to the supplied {@link DataOutput}
   *         .
   */
  public static RTypedBytesOutput get(DataOutput out) {
    RTypedBytesOutput bout = (RTypedBytesOutput) tbOut.get();
    bout.setDataOutput(out);
    return bout;
  }

  /** Creates a new instance of TypedBytesOutput. */
  public RTypedBytesOutput(DataOutput out) {
    this.out = out;
  }
  /**
   * Writes a Java object as a typed bytes sequence.
   *
   * @param obj
   *          the object to be written
   * @throws IOException
   */
  public void writeRaw(Object obj,int typecode) throws IOException {
	 if (typecode == RType.BYTES.code) {
	  writeRawBytes(((Buffer) obj).get());
    } else if (typecode == RType.BYTE.code) {
      writeRawByte((Byte) obj);
    } else if (typecode == RType.BOOL.code) {
      writeRawBool((Boolean) obj);
    } else if (typecode == RType.INT.code) {
      writeRawInt((Integer) obj);
    } else if (typecode == RType.LONG.code) {
      writeRawLong((Long) obj);
    } else if (typecode == RType.FLOAT.code) {
      writeRawFloat((Float) obj);
    } else if (typecode == RType.FLOAT.code) {
      writeRawDouble((Double) obj);
    } else if (typecode == RType.STRING.code) {
      writeRawString((String) obj);
    } else {
      throw new RuntimeException("cannot write raw objects of this type");
    }
  }

   /**
   * Writes a bytes array as a typed bytes sequence, using a given typecode.
   *
   * @param bytes
   *          the bytes array to be written
   * @param code
   *          the typecode to use
   * @throws IOException
   */
  public void writeRawBytes(byte[] bytes, int code) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  /**
   * Writes a bytes array as a typed bytes sequence.
   *
   * @param bytes
   *          the bytes array to be written
   * @throws IOException
   */
  public void writeRawBytes(byte[] bytes) throws IOException {
	//LOG.info("Writing as plain bytes");
    writeRawBytes(bytes, RType.BYTES.code);
  }

  /**
   * Writes a byte as a typed bytes sequence.
   *
   * @param b
   *          the byte to be written
   * @throws IOException
   */
  public void writeRawByte(byte b) throws IOException {
    out.write(b);
  }

  /**
   * Writes a boolean as a typed bytes sequence.
   *
   * @param b
   *          the boolean to be written
   * @throws IOException
   */
  public void writeRawBool(boolean b) throws IOException {
    out.writeBoolean(b);
  }

  /**
   * Writes an integer as a typed bytes sequence.
   *
   * @param i
   *          the integer to be written
   * @throws IOException
   */
  public void writeRawInt(int i) throws IOException {
    out.writeInt(i);
  }

  /**
   * Writes a long as a typed bytes sequence.
   *
   * @param l
   *          the long to be written
   * @throws IOException
   */
  public void writeRawLong(long l) throws IOException {
    out.writeLong(l);
  }

  /**
   * Writes a float as a typed bytes sequence.
   *
   * @param f
   *          the float to be written
   * @throws IOException
   */
  public void writeRawFloat(float f) throws IOException {
    out.writeFloat(f);
  }

  /**
   * Writes a double as a typed bytes sequence.
   *
   * @param d
   *          the double to be written
   * @throws IOException
   */
  public void writeRawDouble(double d) throws IOException {
    out.writeLong(Double.doubleToRawLongBits(d));
  }

  /**
   * Writes a short as a typed bytes sequence.
   *
   * @param s
   *          the short to be written
   * @throws IOException
   */
  public void writeRawShort(short s) throws IOException {
    out.writeShort(s);
  }

  /**
   * Writes a string as a typed bytes sequence.
   *
   * @param s
   *          the string to be written
   * @throws IOException
   */
  public void writeRawString(String s) throws IOException {
    WritableUtils.writeString(out, s);
  }
  /**
   * Writes a Java object as a typed bytes sequence.
   *
   * @param obj
   *          the object to be written
   * @throws IOException
   */
  public void write(Object obj) throws IOException {
	 if (obj == null){
	  this.writeNull();
	 }else if (obj instanceof TypedBytesWritable){
	  writeTypedBytes((TypedBytesWritable)obj);
	 }else if (obj instanceof Buffer) {
	  //LOG.info("Writing as a bytes because obj is a Buffer");
      writeBytes(((Buffer) obj).get());
    } else if (obj instanceof Byte) {
      writeByte((Byte) obj);
    } else if (obj instanceof Boolean) {
      writeBool((Boolean) obj);
    } else if (obj instanceof Integer) {
      writeInt((Integer) obj);
    } else if (obj instanceof Long) {
      writeLong((Long) obj);
    } else if (obj instanceof Float) {
      writeFloat((Float) obj);
    } else if (obj instanceof Double) {
      writeDouble((Double) obj);
    } else if (obj instanceof String) {
      writeString((String) obj);
    } else if (obj instanceof ArrayList) {
      writeVector((ArrayList) obj);
    } else if (obj instanceof List) {
      writeList((List) obj);
    } else if (obj instanceof Map) {
      writeMap((Map) obj);
    } else {
      throw new RuntimeException("cannot write objects of this type: "+obj.getClass());
    }
  }

  /**
   * Writes a raw sequence of typed bytes.
   *
   * @param bytes
   *          the bytes to be written
   * @throws IOException
   */
  public void writeRaw(byte[] bytes) throws IOException {
    out.write(bytes);
  }

  /**
   * Writes a raw sequence of typed bytes.
   *
   * @param bytes
   *          the bytes to be written
   * @param offset
   *          an offset in the given array
   * @param length
   *          number of bytes from the given array to write
   * @throws IOException
   */
  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    out.write(bytes, offset, length);
  }

  /**
   * Writes a bytes array as a typed bytes sequence, using a given typecode.
   *
   * @param bytes
   *          the bytes array to be written
   * @param code
   *          the typecode to use
   * @throws IOException
   */
  public void writeBytes(byte[] bytes, int code) throws IOException {
    out.write(code);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  /**
   * Writes a bytes array as a typed bytes sequence.
   *
   * @param bytes
   *          the bytes array to be written
   * @throws IOException
   */
  public void writeBytes(byte[] bytes) throws IOException {
	//LOG.info("Writing as plain bytes");
    writeBytes(bytes, RType.BYTES.code);
  }
  
  public void writeTypedBytes(TypedBytesWritable tb) throws IOException{
	  //LOG.info("Writing as typedbytes bytes");
	  writeRaw(new Buffer(tb.getBytes(),0,tb.getLength()).get());
  }

  /**
   * Writes a byte as a typed bytes sequence.
   *
   * @param b
   *          the byte to be written
   * @throws IOException
   */
  public void writeByte(byte b) throws IOException {
    out.write(RType.BYTE.code);
    out.write(b);
  }

  /**
   * Writes a boolean as a typed bytes sequence.
   *
   * @param b
   *          the boolean to be written
   * @throws IOException
   */
  public void writeBool(boolean b) throws IOException {
    out.write(RType.BOOL.code);
    out.writeBoolean(b);
  }

  /**
   * Writes an integer as a typed bytes sequence.
   *
   * @param i
   *          the integer to be written
   * @throws IOException
   */
  public void writeInt(int i) throws IOException {
    out.write(RType.INT.code);
    out.writeInt(i);
  }

  /**
   * Writes a long as a typed bytes sequence.
   *
   * @param l
   *          the long to be written
   * @throws IOException
   */
  public void writeLong(long l) throws IOException {
    out.write(RType.LONG.code);
    out.writeLong(l);
  }

  /**
   * Writes a float as a typed bytes sequence.
   *
   * @param f
   *          the float to be written
   * @throws IOException
   */
  public void writeFloat(float f) throws IOException {
    out.write(RType.FLOAT.code);
    out.writeFloat(f);
  }

  /**
   * Writes a double as a typed bytes sequence.
   *
   * @param d
   *          the double to be written
   * @throws IOException
   */
  public void writeDouble(double d) throws IOException {
    out.write(RType.DOUBLE.code);
    out.writeLong(Double.doubleToRawLongBits(d));
  }

  /**
   * Writes a short as a typed bytes sequence.
   *
   * @param s
   *          the short to be written
   * @throws IOException
   */
  public void writeShort(short s) throws IOException {
    out.write(RType.SHORT.code);
    out.writeShort(s);
  }

  /**
   * Writes a string as a typed bytes sequence.
   *
   * @param s
   *          the string to be written
   * @throws IOException
   */
  public void writeString(String s) throws IOException {
    out.write(RType.STRING.code);
    WritableUtils.writeString(out, s);
  }

  /**
   * Writes a vector as a typed bytes sequence.
   *
   * @param vector
   *          the vector to be written
   * @throws IOException
   */
  public void writeVector(List vector) throws IOException {
    writeVectorHeader(vector.size());
    for (Object obj : vector) {
      write(obj);
    }
  }

  /**
   * Writes a vector header.
   *
   * @param length
   *          the number of elements in the vector
   * @throws IOException
   */
  public void writeVectorHeader(int length) throws IOException {
    out.write(RType.VECTOR.code);
    out.writeInt(length);
  }

  /**
   * Writes a list as a typed bytes sequence.
   *
   * @param list
   *          the list to be written
   * @throws IOException
   */
  public void writeList(List list) throws IOException {
    writeListHeader();
    for (Object obj : list) {
      write(obj);
    }
    writeListFooter();
  }

  /**
   * Writes a list header.
   *
   * @throws IOException
   */
  public void writeListHeader() throws IOException {
    out.write(RType.LIST.code);
  }

  /**
   * Writes a list footer.
   *
   * @throws IOException
   */
  public void writeListFooter() throws IOException {
    out.write(RType.MARKER.code);
  }

  /**
   * Writes a map as a typed bytes sequence.
   *
   * @param map
   *          the map to be written
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public void writeMap(Map map) throws IOException {
    writeMapHeader(map.size());
    Set<Entry> entries = map.entrySet();
    for (Entry entry : entries) {
      write(entry.getKey());
      write(entry.getValue());
    }
  }

  /**
   * Writes a map header.
   *
   * @param length
   *          the number of key-value pairs in the map
   * @throws IOException
   */
  public void writeMapHeader(int length) throws IOException {
    out.write(RType.MAP.code);
    out.writeInt(length);
  }
  
  public void writeArrayHeader(int bytelength, int typecode) throws IOException{
	  out.write(RType.ARRAY.code);
	  out.writeInt(bytelength+1);
	  out.write(typecode);
  }
  
  public void writeArray(int typecode, byte[] data) throws IOException{
	  writeArrayHeader(data.length,typecode);
	  this.writeRaw(data);
  }

  public void writeEndOfRecord() throws IOException {
    ;//out.write(Type.ENDOFRECORD.code);
  }

  /**
   * Writes a <tt>NULL</tt> type marker to the output.
   *
   * @throws IOException
   */
  public void writeNull() throws IOException {
    //out.write(RType.NULL.code);
	writeInt(RType.NA_INTEGER);
  }
}
