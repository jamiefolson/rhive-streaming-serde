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
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.NonSyncDataInputBuffer;
import org.apache.hadoop.hive.ql.io.NonSyncDataOutputBuffer;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.typedbytes.TypedBytesWritable;

/**
 * TypedBytesSerDe uses typed bytes to serialize/deserialize.
 *
 * More info on the typedbytes stuff that Dumbo uses.
 * http://issues.apache.org/jira/browse/HADOOP-1722 A fast python decoder for
 * this, which is apparently 25% faster than the python version is available at
 * http://github.com/klbostee/ctypedbytes/tree/master
 */
public abstract class RBaseSerDe implements SerDe {
	public static final Log LOG = LogFactory.getLog("com.sonamine.hive.serde.RTypedBytesSerDe");
protected boolean wrapKeys = false;
protected boolean wrapValues = true;
protected boolean unwrapKeys = false;
protected boolean unwrapValues = true;
public static final String WRAP_VALUE_PROPERTY = "wrap.value"; 
public static final String WRAP_KEY_PROPERTY = "wrap.key"; 
public static final String NATIVE_PROPERTY = "native";
public static final String KEYLENGTH_PROPERTY = "keylength";


  protected int numColumns;
  protected int numKeys = 0;
  protected boolean keepAsBytes;
  protected StructObjectInspector rowOI;
  protected ArrayList<Object> row;

  protected TypedBytesWritable serializeBytesWritable;
  protected NonSyncDataOutputBuffer barrStr;
  protected RTypedBytesWritableOutput tbOut;

  protected NonSyncDataInputBuffer inBarrStr;
  protected RTypedBytesWritableInput tbIn;

  protected List<String> columnNames;
  protected List<TypeInfo> columnTypes;
 
  
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    // We can get the table definition from tbl.
    serializeBytesWritable = new TypedBytesWritable();
    barrStr = new NonSyncDataOutputBuffer();
    tbOut = new RTypedBytesWritableOutput(barrStr);

    inBarrStr = new NonSyncDataInputBuffer();
    tbIn = new RTypedBytesWritableInput(inBarrStr);

    // Read the configuration parameters
    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);

    columnNames = Arrays.asList(columnNameProperty.split(","));
    columnTypes = null;
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils
          .getTypeInfosFromTypeString(columnTypeProperty);
    }

    assert columnNames.size() == columnTypes.size();
    numColumns = columnNames.size();

    // All columns have to be primitive.
    /*for (int c = 0; c < numColumns; c++) {
      if (columnTypes.get(c).getCategory() != Category.PRIMITIVE) {
        throw new SerDeException(getClass().getName()
            + " only accepts primitive columns, but column[" + c + "] named "
            + columnNames.get(c) + " has category "
            + columnTypes.get(c).getCategory());
      }
    }*/

    // Constructing the row ObjectInspector:
    // The row consists of some string columns, each column will be a java
    // String object.
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
        columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      columnOIs.add(TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(columnTypes.get(c)));
    }

    // StandardStruct uses ArrayList to store the row.
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, columnOIs);

    // Constructing the row object, etc, which will be reused for all rows.
    row = new ArrayList<Object>(numColumns);
    for (int c = 0; c < numColumns; c++) {
      row.add(null);
    }
    String numStr = tbl.getProperty(KEYLENGTH_PROPERTY);
    if (numStr != null){
    	numKeys = Integer.parseInt(numStr);
    }
    
    wrapKeys = Boolean.parseBoolean(tbl.getProperty(WRAP_KEY_PROPERTY, "true"));
    unwrapKeys = wrapKeys;
    wrapValues = Boolean.parseBoolean(tbl.getProperty(WRAP_VALUE_PROPERTY, "true"));
    unwrapValues = wrapValues;
    
    keepAsBytes = Boolean.parseBoolean(tbl.getProperty(NATIVE_PROPERTY,"false"));
    if (keepAsBytes){
    	if (numKeys>1){
    		throw new RuntimeException("using native R serialization will only produce at most 1 key, not: "+numKeys);
    	}
    	if (numColumns>2){
    		throw new RuntimeException("using native R serialization will only produce at most 2 columns, not: "+numColumns);
    	}
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return TypedBytesWritable.class;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {

    BytesWritable data = (BytesWritable) blob;
    inBarrStr.reset(data.getBytes(), 0, data.getLength());

    try {
        //Read the key
        if (!keepAsBytes && unwrapKeys ){
      	// It's wrapped as a list TWICE, because R will try to c() things once deserialized
        	RType rtype = tbIn.getInput().readType(); 
        	if (rtype==null){
        		 throw new RuntimeException("End of stream");
        	}
        	if (rtype!=RType.VECTOR) {
        		throw new RuntimeException("Error: expecting vector<vector<key>>, instead of "+rtype.name());
        	}
        	int vectorLength = tbIn.getInput().readVectorHeader();
        	if (vectorLength!=1){
        		throw new RuntimeException("Hive cannot support multiple keys");
        	}
        	
        	rtype = tbIn.getInput().readType(); 
        	if (rtype==null){
        		 throw new RuntimeException("Unexpected end of stream");
        	}
        	if (rtype!=RType.VECTOR) {
        		throw new RuntimeException("Error: expecting vector<key>, instead of "+rtype.name());
        	}
        	vectorLength = tbIn.getInput().readVectorHeader();
        	if (vectorLength != numKeys){
        		throw new RuntimeException("Error: expecting "+numKeys+
        				" values, but list only has: "+vectorLength);
        	}
        }
        for (int i=0;i<numKeys;i++){
        	//LOG.info("Deserializing column: "+i);
            row.set(i, deserializeField(tbIn, columnTypes.get(i), row.get(i)));
        }
        //Read the value
        if (!keepAsBytes && unwrapValues ) {
        	// It's wrapped as a list TWICE, because R will try to c() things once deserialized
        	RType rtype = tbIn.getInput().readType(); 
        	if (rtype==null){
        		 throw new RuntimeException("End of stream");
        	}
        	if (rtype!=RType.VECTOR) {
        		throw new RuntimeException("Error: expecting vector<vector<value>>, instead of "+rtype.name());
        	}
        	int vectorLength = tbIn.getInput().readVectorHeader();
        	if (vectorLength!=1){
        		throw new RuntimeException("Hive cannot support multiple values");
        	}
        	
        	rtype = tbIn.getInput().readType(); 
        	if (rtype==null){
        		 throw new RuntimeException("Unexpected end of stream");
        	}
        	if (rtype!=RType.VECTOR) {
        		throw new RuntimeException("Error: expecting vector<key>, instead of "+rtype.name());
        	}
        	vectorLength = tbIn.getInput().readVectorHeader();
        	if (vectorLength != (numColumns - numKeys)){
        		throw new RuntimeException("Error: expecting "+(numColumns-numKeys)+
        				" values, but list only has: "+vectorLength);
        	}
        }
        for (int i = numKeys; i < numColumns; i++) {
        	//LOG.info("Deserializing column: "+i);
            row.set(i, deserializeField(tbIn, columnTypes.get(i), row.get(i)));
        }

      // The next byte should be the marker
      // R doesn't want this
      //assert tbIn.readTypeCode() == Type.ENDOFRECORD;

    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return row;
  }

  protected Object deserializeField(RTypedBytesWritableInput in, TypeInfo type,
      Object reuse) throws IOException {

	 RType rtype = in.readTypeCode();
	 if (rtype == null){
		 throw new RuntimeException("End of stream");
	 }
     
    // read the type
    Class<? extends Writable> writableType = RType.getWritableType(rtype);
    if (writableType == null){
    	LOG.info("Warning: null Writable type for rtype: "+rtype);
    }
    if (writableType != null &&
        writableType.isAssignableFrom(NullWritable.class)) {
      // indicates that the recorded value is null
      return null;
    }
    //LOG.info("RType should be instantiated as: "+writableType.getSimpleName());
    

    switch (type.getCategory()) {
    case PRIMITIVE: {
      PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
      switch (ptype.getPrimitiveCategory()) {

      case VOID: {
        return null;
      }

      case BINARY: {
    	  TypedBytesWritable r = reuse == null ? new TypedBytesWritable()
          : (TypedBytesWritable) reuse;
    	  byte[] bytes = in.getInput().readRaw(rtype.code);
    	  // rewrite the type code
    	  r.set(bytes,0,bytes.length);
    	  return r;
      }

      case BOOLEAN: {
    	//TODO Fix this hack:
    	if (rtype != RType.BOOL){
    		in.readNull();
    		return null;
    	}
        BooleanWritable r = reuse == null ? new BooleanWritable()
            : (BooleanWritable) reuse;
        return in.readBoolean(r);
      }
      /*case BYTE: {
        ByteWritable r = reuse == null ? new ByteWritable()
            : (ByteWritable) reuse;
        r = in.readByte(r);
        return r;
      }*/
      /*case SHORT: {
        ShortWritable r = reuse == null ? new ShortWritable()
            : (ShortWritable) reuse;
        r = in.readShort(r);
        return r;
      }*/
      case INT: {
    	  if (rtype != RType.INT){
    		  in.readNull();
    		  return null;
    	  }
        IntWritable r = reuse == null ? null : (IntWritable) reuse;
        return in.readInt(r);
      }
      /*case LONG: {
        LongWritable r = reuse == null ? new LongWritable()
            : (LongWritable) reuse;
        r = in.readLong(r);
        return r;
      }*/
      /*case FLOAT: {
        FloatWritable r = reuse == null ? new FloatWritable()
            : (FloatWritable) reuse;
        r = in.readFloat(r);
        return r;
      }*/
      case DOUBLE: {
    	if (rtype != RType.DOUBLE){
      		in.readNull();
      		return null;
      	}
        DoubleWritable r = reuse == null ? null
            : (DoubleWritable) reuse;
        return in.readDouble(r);
      }
      case STRING: {
    	  // TODO fix this hack
    	  if (rtype != RType.STRING){
    		  in.readNull();
    		  return null;
    	  }
        Text r = reuse == null ? null : (Text) reuse;
        return in.readText(r);
      }
      default: {
        throw new RuntimeException("Unrecognized type: "
            + ptype.getPrimitiveCategory());
      }
      }
    }
      // Currently, deserialization of complex types is not supported
    case LIST: {
    	if (rtype != RType.VECTOR){
   		  in.readNull();
   		  return null;
   	  	}
    	ObjectInspector elemOI = ((ListObjectInspector)
    			TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(type)).
    			getListElementObjectInspector();
    	
    	PrimitiveObjectInspector elemPOI= (PrimitiveObjectInspector)elemOI;
    	
    	Class<? extends Writable> elemClass = (Class<? extends Writable>) elemPOI.getPrimitiveWritableClass();
    	ArrayWritable l = reuse == null ? 
    			new ArrayWritable(elemClass) : 
    				new ArrayWritable(elemClass,(Writable[]) reuse);
    	in.readVector(l);
    	return l.get();
    }
    case MAP:
    case STRUCT:
    default: {
      throw new RuntimeException("Unsupported category: " + type.getCategory());
    }
    }
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    try {
      barrStr.reset();
      StructObjectInspector soi = (StructObjectInspector) objInspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      
      //Write the key
      if (!keepAsBytes && wrapKeys ){
    	// Wrap it as a list TWICE, because R will try to c() things once deserialized
      	tbOut.getOutput().writeVectorHeader(1); 
    	tbOut.getOutput().writeVectorHeader(numKeys); 
      }
      for (int i=0;i<numKeys;i++){
    	//LOG.info("Serializing key column: "+i);
      	Object o = soi.getStructFieldData(obj, fields.get(i));
      	ObjectInspector oi = fields.get(i).getFieldObjectInspector();
      	serializeField(o, oi, row.get(i));
      }
      //Write the value
      // write it as a list if more than one element
      if (!keepAsBytes && wrapValues) {
      	// Wrap it as a list TWICE, because R will try to c() things once deserialized
      	tbOut.getOutput().writeVectorHeader(1); 
      	tbOut.getOutput().writeVectorHeader(numColumns-numKeys); 
      }
      for (int i = numKeys; i < numColumns; i++) {
    	//LOG.info("Serializing column: "+i);
        Object o = soi.getStructFieldData(obj, fields.get(i));
        ObjectInspector oi = fields.get(i).getFieldObjectInspector();
        serializeField(o, oi, row.get(i));
      }

      // End of the record is part of the data
      //tbOut.writeEndOfRecord();

      serializeBytesWritable.set(barrStr.getData(), 0, barrStr.getLength());
    } catch (IOException e) {
      throw new SerDeException(e.getMessage());
    }
    return serializeBytesWritable;
  }

  protected void serializeField(Object o, ObjectInspector oi, Object reuse)
      throws IOException {
	  //LOG.info("Serializing hive type: "+oi.getTypeName());
	  //LOG.info("Serializing category: "+oi.getCategory().toString());
	if (o==null){
		tbOut.writeNull();
		return;
	}
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
	  //LOG.info("Serializing primitive: "+poi.getPrimitiveCategory().toString());
      switch (poi.getPrimitiveCategory()) {
      case VOID: {
        return;
      }
      case BINARY: {
    	  BinaryObjectInspector boi = (BinaryObjectInspector) poi;
    	  TypedBytesWritable bytes = reuse == null ? new TypedBytesWritable()
          : (TypedBytesWritable) reuse;
    	  BytesWritable bytesWrite = boi.getPrimitiveWritableObject(o); 
    	  if (bytesWrite!=null){
    		  bytes.set(bytesWrite);
    		  if (!RType.isValid(bytes)){
        		  LOG.error("Invalid typedbytes detected with type: "+RType.getType(bytes).code);
        		  bytes.setValue(new Buffer(bytesWrite.getBytes(),0,bytesWrite.getLength()));
        	  }
    		  //LOG.info("Writing binary primitive with class: "+bytes.getClass().getName());
        	  tbOut.write(bytes);
    	  }
    	  
    	  return;
      }
      case BOOLEAN: {
        BooleanObjectInspector boi = (BooleanObjectInspector) poi;
        BooleanWritable r = reuse == null ? new BooleanWritable()
            : (BooleanWritable) reuse;
        r.set(boi.get(o));
        tbOut.write(r);
        return;
      }
      case BYTE: {
        ByteObjectInspector boi = (ByteObjectInspector) poi;
        ByteWritable r = reuse == null ? new ByteWritable()
            : (ByteWritable) reuse;
        r.set(boi.get(o));
        tbOut.write(r);
        return;
      }
      case SHORT: {
        ShortObjectInspector spoi = (ShortObjectInspector) poi;
        ShortWritable r = reuse == null ? new ShortWritable()
            : (ShortWritable) reuse;
        r.set(spoi.get(o));
        tbOut.write(r);
        return;
      }
      case INT: {
        IntObjectInspector ioi = (IntObjectInspector) poi;
        IntWritable r = reuse == null ? new IntWritable() : (IntWritable) reuse;
        r.set(ioi.get(o));
        tbOut.write(r);
        return;
      }
      case LONG: {
        LongObjectInspector loi = (LongObjectInspector) poi;
        LongWritable r = reuse == null ? new LongWritable()
            : (LongWritable) reuse;
        r.set(loi.get(o));
        tbOut.write(r);
        return;
      }
      case FLOAT: {
        FloatObjectInspector foi = (FloatObjectInspector) poi;
        FloatWritable r = reuse == null ? new FloatWritable()
            : (FloatWritable) reuse;
        r.set(foi.get(o));
        tbOut.write(r);
        return;
      }
      case DOUBLE: 
        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
        DoubleWritable r = reuse == null ? new DoubleWritable()
            : (DoubleWritable) reuse;
        r.set(doi.get(o));
        tbOut.write(r);
        return;
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector) poi;
        Text t = soi.getPrimitiveWritableObject(o);
        tbOut.write(t);
        return;
      }
      default: {
        throw new RuntimeException("Unrecognized type: "
            + poi.getPrimitiveCategory());
      }
      }
    }
    case LIST: {
    	ListObjectInspector loi = (ListObjectInspector)oi;
    	ObjectInspector elemOI = loi.getListElementObjectInspector();
    	List l = loi.getList(o);
    	// Don't use array (typecode: 144) until everything supports NA values in typedbytes
    	if (false){//(elemOI.getCategory()==ObjectInspector.Category.PRIMITIVE){
    		tbOut.writeArray(l,(PrimitiveObjectInspector) elemOI);
    	}else {
    		tbOut.writeVector(l,(PrimitiveObjectInspector) elemOI);
    	}
    	return;
    }
    case MAP:
    case STRUCT: {
      // For complex object, serialize to JSON format
      String s = SerDeUtils.getJSONString(o, oi);
      Text t = reuse == null ? new Text() : (Text) reuse;

      // convert to Text and write it
      t.set(s);
      tbOut.write(t);
      return;
    }
    default: {
      throw new RuntimeException("Unrecognized type: " + oi.getCategory());
    }
    }
  }

  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
