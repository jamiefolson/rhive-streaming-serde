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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

/**
 * Provides functionality for reading typed bytes.
 */
public class RTypedBytesInput {
	public static final Log LOG = LogFactory.getLog("com.sonamine.hive.serde.RTypedBytesInput");

  private DataInput in;

  private RTypedBytesInput() {
  }

  private void setDataInput(DataInput in) {
    this.in = in;
  }

  private static ThreadLocal tbIn = new ThreadLocal() {
    @Override
    protected synchronized Object initialValue() {
      return new RTypedBytesInput();
    }
  };

  /**
   * Get a thread-local typed bytes input for the supplied {@link DataInput}.
   * 
   * @param in
   *          data input object
   * @return typed bytes input corresponding to the supplied {@link DataInput}.
   */
  public static RTypedBytesInput get(DataInput in) {
    RTypedBytesInput bin = (RTypedBytesInput) tbIn.get();
    bin.setDataInput(in);
    return bin;
  }

  /** Creates a new instance of TypedBytesInput. */
  public RTypedBytesInput(DataInput in) {
    this.in = in;
  }

  /**
   * Reads a typed bytes sequence and converts it to a Java object. The first
   * byte is interpreted as a type code, and then the right number of subsequent
   * bytes are read depending on the obtained type.
   * 
   * @return the obtained object or null when the end of the file is reached
   * @throws IOException
   */
  public Object read() throws IOException {
    int code = -1;
    try {
      code = in.readUnsignedByte();
      return read(code);
    } catch (EOFException eof) {
      return null;
    }
  }
  public Object read(int code) throws IOException {
    if (code == RType.BYTES.code) {
      return new Buffer(readBytes());
    } else if (code == RType.BYTE.code) {
      return readByte();
    } else if (code == RType.BOOL.code) {
      return readBool();
    } else if (code == RType.INT.code) {
      return readInt();
    } else if (code == RType.SHORT.code) {
      return readShort();
    } else if (code == RType.LONG.code) {
      return readLong();
    } else if (code == RType.FLOAT.code) {
      return readFloat();
    } else if (code == RType.DOUBLE.code) {
      return readDouble();
    } else if (code == RType.STRING.code) {
      return readString();
    } else if (code == RType.VECTOR.code) {
      return readVector();
    } else if (code == RType.LIST.code) {
      return readList();
    } else if (code == RType.MAP.code) {
      return readMap();
    }else if (code == RType.NULL.code) {
      return null;
    } else if (code == RType.RNATIVE.code) {
       return readRNative();
    } else if (code == RType.MARKER.code) {
      return null;
    } else if (50 <= code && code <= 200) { // application-specific typecodes
    	byte[] bytes = readRawBytes();
        bytes[0] = (byte)code;
      return new TypedBytesWritable(bytes);
    } else {
      throw new RuntimeException("unknown type");
    }
  }

  /**
   * Reads a typed bytes sequence. The first byte is interpreted as a type code,
   * and then the right number of subsequent bytes are read depending on the
   * obtained type.
   * 
   * @return the obtained typed bytes sequence or null when the end of the file
   *         is reached
   * @throws IOException
   */
  public byte[] readRaw() throws IOException {
    int code = -1;
    try {
        code = in.readUnsignedByte();
        return readRaw(code);
      } catch (EOFException eof) {
        return null;
     }
  }
  public byte[] readRaw(int code) throws IOException {
    
    if (code == RType.BYTES.code) {
      return readRawBytes();
    } else if (code == RType.BYTE.code) {
      return readRawByte();
    } else if (code == RType.BOOL.code) {
      return readRawBool();
    } else if (code == RType.INT.code) {
      return readRawInt();
    } else if (code == RType.LONG.code) {
      return readRawLong();
    } else if (code == RType.FLOAT.code) {
      return readRawFloat();
    } else if (code == RType.DOUBLE.code) {
      return readRawDouble();
    } else if (code == RType.STRING.code) {
      return readRawString();
    } else if (code == RType.VECTOR.code) {
      return readRawVector();
    } else if (code == RType.LIST.code) {
      return readRawList();
    } else if (code == RType.MAP.code) {
      return readRawMap();
    } else if (code == RType.RNATIVE.code) {
      return readRawRNative();
    } else if (code == RType.MARKER.code) {
      return null;
    }else if (code == RType.NULL.code) {
      byte[] bytes = new byte[1];
      bytes[0] = (byte)code;
      return bytes;
    } else if (50 <= code && code <= 200) { // application-specific typecodes
      byte[] bytes = readRawBytes();
      bytes[0] = (byte)code;
      return bytes;
    } else {
      throw new RuntimeException("unknown type");
    }
  }
  
  public byte[] readRawRNative() throws IOException {
	  byte[] bytes = readRawBytes();
	  bytes[0] = (byte) RType.RNATIVE.code;
	  return bytes;
  }
  
  public Object readRNative() throws IOException {
	  return new TypedBytesWritable(readRawRNative());
  }

  /**
   * Reads a type byte and returns the corresponding {@link RType}.
   * 
   * @return the obtained Type or null when the end of the file is reached
   * @throws IOException
   */
  public RType readType() throws IOException {
    int code = -1;
    try {
      code = in.readUnsignedByte();
    } catch (EOFException eof) {
      return null;
    }
    RType type = RType.valueOf(code);
    if (type == null){
    	LOG.info("Warning: null RType for typecode: "+code);
    }
    return type;
  }

  /**
   * Skips a type byte.
   * 
   * @return true iff the end of the file was not reached
   * @throws IOException
   */
  public boolean skipType() throws IOException {
    try {
      in.readByte();
      return true;
    } catch (EOFException eof) {
      return false;
    }
  }

  /**
   * Reads the bytes following a <code>Type.BYTES</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readBytes() throws IOException {
    int length = in.readInt();
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return bytes;
  }

  /**
   * Reads the raw bytes following a <code>Type.BYTES</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawBytes() throws IOException {
    int length = in.readInt();
    byte[] bytes = new byte[5 + length];
    bytes[0] = (byte) RType.BYTES.code;
    bytes[1] = (byte) (0xff & (length >> 24));
    bytes[2] = (byte) (0xff & (length >> 16));
    bytes[3] = (byte) (0xff & (length >> 8));
    bytes[4] = (byte) (0xff & length);
    in.readFully(bytes, 5, length);
    return bytes;
  }

  /**
   * Reads the byte following a <code>Type.BYTE</code> code.
   * 
   * @return the obtained byte
   * @throws IOException
   */
  public byte readByte() throws IOException {
    return in.readByte();
  }

  /**
   * Reads the raw byte following a <code>Type.BYTE</code> code.
   * 
   * @return the obtained byte
   * @throws IOException
   */
  public byte[] readRawByte() throws IOException {
    byte[] bytes = new byte[2];
    bytes[0] = (byte) RType.BYTE.code;
    in.readFully(bytes, 1, 1);
    return bytes;
  }

  /**
   * Reads the boolean following a <code>Type.BOOL</code> code.
   * 
   * @return the obtained boolean
   * @throws IOException
   */
  public boolean readBool() throws IOException {
    return in.readBoolean();
  }

  /**
   * Reads the raw bytes following a <code>Type.BOOL</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawBool() throws IOException {
    byte[] bytes = new byte[2];
    bytes[0] = (byte) RType.BOOL.code;
    in.readFully(bytes, 1, 1);
    return bytes;
  }

  /**
   * Reads the integer following a <code>Type.INT</code> code.
   * 
   * @return the obtained integer
   * @throws IOException
   */
  public int readInt() throws IOException {
    return in.readInt();
  }

  /**
   * Reads the short following a <code>Type.SHORT</code> code.
   * 
   * @return the obtained short
   * @throws IOException
   */
  public short readShort() throws IOException {
    return in.readShort();
  }

  /**
   * Reads the raw bytes following a <code>Type.INT</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawInt() throws IOException {
    byte[] bytes = new byte[5];
    bytes[0] = (byte) RType.INT.code;
    in.readFully(bytes, 1, 4);
    return bytes;
  }

  /**
   * Reads the long following a <code>Type.LONG</code> code.
   * 
   * @return the obtained long
   * @throws IOException
   */
  public long readLong() throws IOException {
    return in.readLong();
  }

  /**
   * Reads the raw bytes following a <code>Type.LONG</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawLong() throws IOException {
    byte[] bytes = new byte[9];
    bytes[0] = (byte) RType.LONG.code;
    in.readFully(bytes, 1, 8);
    return bytes;
  }

  /**
   * Reads the float following a <code>Type.FLOAT</code> code.
   * 
   * @return the obtained float
   * @throws IOException
   */
  public float readFloat() throws IOException {
    return in.readFloat();
  }

  /**
   * Reads the raw bytes following a <code>Type.FLOAT</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawFloat() throws IOException {
    byte[] bytes = new byte[5];
    bytes[0] = (byte) RType.FLOAT.code;
    in.readFully(bytes, 1, 4);
    return bytes;
  }

  /**
   * Reads the double following a <code>Type.DOUBLE</code> code.
   * 
   * @return the obtained double
   * @throws IOException
   */
  public double readDouble() throws IOException {
    return in.readDouble();
  }

  /**
   * Reads the raw bytes following a <code>Type.DOUBLE</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawDouble() throws IOException {
    byte[] bytes = new byte[9];
    bytes[0] = (byte) RType.DOUBLE.code;
    in.readFully(bytes, 1, 8);
    return bytes;
  }

  /**
   * Reads the string following a <code>Type.STRING</code> code.
   * 
   * @return the obtained string
   * @throws IOException
   */
  public String readString() throws IOException {
    return WritableUtils.readString(in);
  }

  /**
   * Reads the raw bytes following a <code>Type.STRING</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawString() throws IOException {
    int length = in.readInt();
    byte[] bytes = new byte[5 + length];
    bytes[0] = (byte) RType.STRING.code;
    bytes[1] = (byte) (0xff & (length >> 24));
    bytes[2] = (byte) (0xff & (length >> 16));
    bytes[3] = (byte) (0xff & (length >> 8));
    bytes[4] = (byte) (0xff & length);
    in.readFully(bytes, 5, length);
    return bytes;
  }

  /**
   * Reads the vector following a <code>Type.VECTOR</code> code.
   * 
   * @return the obtained vector
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public ArrayList readVector() throws IOException {
    int length = readVectorHeader();
    ArrayList result = new ArrayList(length);
    for (int i = 0; i < length; i++) {
      result.add(read());
    }
    return result;
  }

  /**
   * Reads the raw bytes following a <code>Type.VECTOR</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawVector() throws IOException {
    Buffer buffer = new Buffer();
    int length = readVectorHeader();
    buffer.append(new byte[] {(byte) RType.VECTOR.code,
        (byte) (0xff & (length >> 24)), (byte) (0xff & (length >> 16)),
        (byte) (0xff & (length >> 8)), (byte) (0xff & length)});
    for (int i = 0; i < length; i++) {
      buffer.append(readRaw());
    }
    return buffer.get();
  }

  /**
   * Reads the header following a <code>Type.VECTOR</code> code.
   * 
   * @return the number of elements in the vector
   * @throws IOException
   */
  public int readVectorHeader() throws IOException {
    return in.readInt();
  }

  /**
   * Reads the list following a <code>Type.LIST</code> code.
   * 
   * @return the obtained list
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public List readList() throws IOException {
    List list = new ArrayList();
    Object obj = read();
    while (obj != null) {
      list.add(obj);
      obj = read();
    }
    return list;
  }

  /**
   * Reads the raw bytes following a <code>Type.LIST</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawList() throws IOException {
    Buffer buffer = new Buffer(new byte[] {(byte) RType.LIST.code});
    byte[] bytes = readRaw();
    while (bytes != null) {
      buffer.append(bytes);
      bytes = readRaw();
    }
    buffer.append(new byte[] {(byte) RType.MARKER.code});
    return buffer.get();
  }

  /**
   * Reads the map following a <code>Type.MAP</code> code.
   * 
   * @return the obtained map
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public TreeMap readMap() throws IOException {
    int length = readMapHeader();
    TreeMap result = new TreeMap();
    for (int i = 0; i < length; i++) {
      Object key = read();
      Object value = read();
      result.put(key, value);
    }
    return result;
  }

  /**
   * Reads the raw bytes following a <code>Type.MAP</code> code.
   * 
   * @return the obtained bytes sequence
   * @throws IOException
   */
  public byte[] readRawMap() throws IOException {
    Buffer buffer = new Buffer();
    int length = readMapHeader();
    buffer.append(new byte[] {(byte) RType.MAP.code,
        (byte) (0xff & (length >> 24)), (byte) (0xff & (length >> 16)),
        (byte) (0xff & (length >> 8)), (byte) (0xff & length)});
    for (int i = 0; i < length; i++) {
      buffer.append(readRaw());
      buffer.append(readRaw());
    }
    return buffer.get();
  }

  /**
   * Reads the header following a <code>Type.MAP</code> code.
   * 
   * @return the number of key-value pairs in the map
   * @throws IOException
   */
  public int readMapHeader() throws IOException {
    return in.readInt();
  }

}
