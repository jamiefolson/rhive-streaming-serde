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

import static com.sonamine.hive.serde.RType.BOOL;
import static com.sonamine.hive.serde.RType.BYTE;
import static com.sonamine.hive.serde.RType.BYTES;
import static com.sonamine.hive.serde.RType.DOUBLE;
import static com.sonamine.hive.serde.RType.ENDOFRECORD;
import static com.sonamine.hive.serde.RType.FLOAT;
import static com.sonamine.hive.serde.RType.INT;
import static com.sonamine.hive.serde.RType.LONG;
import static com.sonamine.hive.serde.RType.MAP;
import static com.sonamine.hive.serde.RType.NULL;
import static com.sonamine.hive.serde.RType.SHORT;
import static com.sonamine.hive.serde.RType.STRING;
import static com.sonamine.hive.serde.RType.VECTOR;
import static com.sonamine.hive.serde.RType.WRITABLE;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

/**
 * The possible type codes.
 */
public enum RType {
  BYTES(0, BytesWritable.class),
  BYTE(1, ByteWritable.class),
  BOOL(2, BooleanWritable.class),
  INT(3, IntWritable.class),
  LONG(4, LongWritable.class),
  FLOAT(5, FloatWritable.class),
  DOUBLE(6, DoubleWritable.class),
  STRING(7, Text.class),
  VECTOR(8, ArrayWritable.class),
  LIST(9, ArrayWritable.class),
  MAP(10, MapWritable.class),
  SHORT(11, ShortWritable.class),
  NULL(12, NullWritable.class),
  WRITABLE(50, Writable.class),
  RNATIVE(144, TypedBytesWritable.class),
  ARRAY(145, ArrayWritable.class),
  ENDOFRECORD(177,null),
  MARKER(255,null);
  // codes for supported types (< 50):
  
  static final long NA_REAL = Long.decode("0x7ff00000000007a2"); 
  static final int NA_INTEGER = Integer.MIN_VALUE;
  
  public static RType valueOf(int code){
	  for (RType rtype : RType.values()){
		  if (rtype.code == code){
			  return rtype;
		  }
	  }
	  
	  return null;
  }
  
  final int code;
  Class<? extends Writable> writableClass;

  private RType(int code, Class<? extends Writable> writableClass){
	  this.code = code;
	  this.writableClass = writableClass;
  }
  
  private Writable getWritable() {
	  if (writableClass!=null){
		  try {
			  Constructor<? extends Writable> construct = writableClass.getConstructor();
			  if (construct != null)
				  return construct.newInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	  }
	  return null;
  }
  
  public static Class<? extends Writable> getWritableType(RType type){
	  
	  if(type !=  null){
	      return type.writableClass;
	    }else {
	      throw new RuntimeException("unknown type");
	    }
  }
  public static Writable getWritable(RType type){
	  if (type != null){
		  return type.getWritable();
	  }
	  return null;
  }
  public static TypeInfo getHiveType(Writable writable){
	  Class clazz = writable.getClass();
	  if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(clazz)){
		  return getHiveType(clazz);
	  }else if (clazz == ArrayWritable.class){
		  Class valueClazz = ((ArrayWritable) writable).getValueClass();
		  if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(valueClazz)){
			  TypeInfo valueType = getHiveType(valueClazz);
			  return TypeInfoFactory.getListTypeInfo(valueType);
		  }else if (valueClazz.equals(TypedBytesWritable.class)){
			  return TypeInfoFactory.getListTypeInfo(TypeInfoFactory.binaryTypeInfo);
		  }else {//if (valueClazz.equals(Writable.class)){
			  return TypeInfoFactory.getListTypeInfo(TypeInfoFactory.voidTypeInfo);
		  }
	  }
	  return null;
  }
  private static TypeInfo getHiveType(Class clazz){
	  
	  String typeName = PrimitiveObjectInspectorUtils.getTypeNameFromPrimitiveWritable(clazz);
	  return TypeInfoFactory.getPrimitiveTypeInfo(typeName);
  }
  
  public static RType valueOf(PrimitiveCategory hiveType){
	  return RType.valueOf(typecode(hiveType));
  }

  private static int typecode(PrimitiveCategory hiveType){
	  switch (hiveType) {
	  case BINARY:{
		  return 0;
	  }
      case BYTE:{
        return 1;
        }
      case BOOLEAN:{
        return 2;
        }
      case INT:{
        return 3;
        }
      case LONG:{
        return 4;
        }
      case FLOAT: {
    	  return 5;
      }
      case DOUBLE: {
        return 6;
      }
      case STRING: {
          return 7;
      }
      default:
  	}
	return -1;
  }
  
  /** Get the type code embedded in the first byte. */
  public static RType getType(TypedBytesWritable writable) {
    byte[] bytes = writable.getBytes();
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    RType type = RType.valueOf((int)bytes[0] & 0xff);
    return type;
  }
  public static boolean isValid(TypedBytesWritable writable) {
	  	try {
	  	  byte[] bytes = writable.getBytes();
	  	ByteArrayInputStream bais = new ByteArrayInputStream(bytes,0,writable.getLength());
        RTypedBytesInput rtbi = RTypedBytesInput.get(new DataInputStream(bais));
  	  if (bytes == null || bytes.length <= 5) {
  		    return false;
	  	  }
	  	  RType rtype = rtbi.readType();
		  if (rtype==null) {
			  return false;
		  }
		  byte[] obj = rtbi.readRaw(rtype.code);
		   if(obj==null) {
			  return false;
		  }
		   int obj_length = obj.length;
		  obj = rtbi.readRaw();
		  if (obj!=null){
			  return false;
		  }
		  return true;
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	 return false;
  }

}
