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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.io.NonSyncDataOutputBuffer;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import org.apache.log4j.Logger;

import com.sonamine.hive.serde.*;
import static com.sonamine.hive.serde.RType.*;

/**
 * TypedBytesRecordReader.
 *
 */
public class RTypedBytesRecordReader implements RecordReader {
	public static final Log LOG = LogFactory.getLog("com.sonamine.hive.serde.RTypedBytesRecordReader");
  private DataInputStream din;
  private RTypedBytesWritableInput tbIn;

  private NonSyncDataOutputBuffer barrStr = new NonSyncDataOutputBuffer();
  private NonSyncDataOutputBuffer keysBarrStr = new NonSyncDataOutputBuffer();

  private RTypedBytesWritableOutput tbOut;

  private ArrayList<Writable> row = new ArrayList<Writable>(0);
  private ArrayList<Integer> rowType = new ArrayList<Integer>(0);
  private List<TypeInfo> columnTypes;
  private boolean[] isBinary;

  private ArrayList<ObjectInspector> dstOIns = new ArrayList<ObjectInspector>();
  private ArrayList<Converter> converters = new ArrayList<Converter>();
private int numColumns;
private int numKeys;
private int listValuesLeftCount = 0;
private boolean useNative;

  public void initialize(InputStream in, Configuration conf, Properties tbl) throws IOException {
    din = new DataInputStream(in);
    tbIn = new RTypedBytesWritableInput(din);
    tbOut = new RTypedBytesWritableOutput(barrStr);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
    //columnTypes = Arrays.asList(columnTypeProperty.split(","));
    

    // Read the configuration parameters
    columnTypes = null;
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils
          .getTypeInfosFromTypeString(columnTypeProperty);
    }

    numColumns = columnTypes.size();
    
    // All columns have to be primitive.
    /*for (int c = 0; c < numColumns; c++) {
      if (columnTypes.get(c).getCategory() != Category.PRIMITIVE) {
        throw new RuntimeException(getClass().getName()
            + " only accepts primitive columns, but column[" + c + "] "
            + " has category "
            + columnTypes.get(c).getCategory());
      }
    }*/

    isBinary = new boolean[numColumns];
    useNative = Boolean.parseBoolean(tbl.getProperty(RTypedBytesSerDe.NATIVE_PROPERTY,
    		RTypedBytesSerDe.NATIVE_DEFAULT));

    // Constructing the row ObjectInspector:
    // The row consists of some string columns, each column will be a java
    // String object.
    dstOIns.ensureCapacity(columnTypes.size());
    for (int c = 0; c < numColumns; c++) {
    	ObjectInspector oi = TypeInfoUtils
    	          .getStandardWritableObjectInspectorFromTypeInfo(columnTypes.get(c)); 
    	dstOIns.add(oi);
      isBinary[c] = false;
      if (oi instanceof PrimitiveObjectInspector) {
    	  PrimitiveObjectInspector poi = ((PrimitiveObjectInspector)oi);
    	  if (poi.getPrimitiveCategory() == PrimitiveCategory.BINARY){
    		  isBinary[c]=true;
    	  }
      }
      if (!isBinary[c] && useNative){
    	  throw new RuntimeException("Cannot only load native R serialized objects as Binary type, not: "+
    			  oi.getTypeName());
      }
    }
    /*for (String columnType : columnTypes) {
      PrimitiveTypeEntry dstTypeEntry = PrimitiveObjectInspectorUtils
          .getTypeEntryFromTypeName(columnType);
      dstOIns.add(PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(dstTypeEntry.primitiveCategory));
    }*/
    
  }

  public Writable createRow() throws IOException {
    BytesWritable retWrit = new BytesWritable();
    //LOG.info("Should have created a row");
    return retWrit;
  }
  public boolean nextElement(int pos) throws IOException {
	  RType type = tbIn.readTypeCode();
	  
	  // it was a empty stream
      if (type == null) {
    	  LOG.info("End of stream");
        return false;
      }
      //LOG.info("Found type: "+type.name());
	  boolean newColumn = (pos >= row.size()); 
      
      if (newColumn) {
    	  // This will automatically deal with custom typedbytes to binary
        /*Writable wrt = RType.getWritable(type);
        
        if (isBinary[pos]){
        	wrt = new RTypedBytesWritable();
        }
        row.add(wrt);
        */
    	row.add(null);
        assert pos == row.size();
        assert pos == rowType.size();
        rowType.add(type.code);
      } else {
        if (!rowType.get(pos).equals(type.code)) {
          throw new RuntimeException("datatype of row changed from "
              + rowType.get(pos) + " to " + type.code);
        }
      }
      // Get it first so we don't waste time instantiating objects
      Writable w = row.get(pos);
      if (isBinary[pos]){
    	  w = tbIn.readRaw(type,w);
      }else {
    	  w = tbIn.read(type, w);
      }
      
      if (newColumn) {
    	 // Set it here so we don't waste time instantiating objects
    	row.set(pos, w);
        ObjectInspector oi = null;
        if (isBinary[pos]){
        	//oi = dstOIns.get(pos);
        	//can't do this, since converter checks for == identity?  Pretty sure this is wrong,
        	// primitive object inspectors are instantiated as static fields and cached
        	oi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
        			PrimitiveCategory.BINARY);
        }else {
        	TypeInfo typeinfo = RType.getHiveType(w);
        	if (typeinfo==null){
        		throw new RuntimeException("Unsupported Writable "
        	              + w.getClass().getCanonicalName()+", typecode: "+type.code);
        	}
        	LOG.info("Translating typedbytes: "+type.code+", writable: "+w.getClass()+
        			", into hive type: "+typeinfo.toString());
        	
        	oi = TypeInfoUtils
                    .getStandardWritableObjectInspectorFromTypeInfo(typeinfo);
        }
        //LOG.info("Using OI: "+oi.getTypeName());
        
        LOG.info("Converting typedbytes: "+type.code+", loaded as: "+oi.getTypeName()+
        		", into hive type: "+dstOIns.get(pos).getTypeName());

        converters.add(
        		getConverter(oi,dstOIns.get(pos)));
        //LOG.info("Identical OIs? "+(oi==dstOIns.get(pos)));
        //LOG.info("OI converter: "+converters.get(pos).toString());

      }

      write(pos, w);
      return true;
  }


private Converter getConverter(ObjectInspector inputOI, ObjectInspector outputOI) {
	
	// TODO Auto-generated method stub
	if (outputOI.getCategory() == ObjectInspector.Category.PRIMITIVE){
		return ObjectInspectorConverters.getConverter(inputOI,
	            outputOI);
	}else if (outputOI.getCategory() == ObjectInspector.Category.LIST){
		return new HeterogeneousArrayWritableConverter((ListObjectInspector)inputOI,
				(SettableListObjectInspector)outputOI);
	}
	return null;
}

public static class HeterogeneousArrayWritableConverter implements Converter {
	public static final Log LOG = LogFactory.getLog("com.sonamine.hive.serde.RTypedBytesRecordReader.HeterogeneousArrayWritableConverter");

	private ListObjectInspector inputOI;
	private SettableListObjectInspector outputOI;
	private ObjectInspector elemOI;
	private Object output;
	public HeterogeneousArrayWritableConverter(ListObjectInspector inputOI,
			SettableListObjectInspector outputOI){
		this.inputOI = inputOI;
		this.outputOI = outputOI;
		this.elemOI = outputOI.getListElementObjectInspector();
	    output = outputOI.create(0);
	}
	@Override
	public Object convert(Object input) {
		if (input == null) {
			return null;
		}
		// Create enough elementConverters
		// NOTE: we have to have a separate elementConverter for each element,
		// because the elementConverters can reuse the internal object.
		// So it's not safe to use the same elementConverter to convert multiple
		// elements.
		int size = inputOI.getListLength(input);
		List inputList = inputOI.getList(input);
		/*LOG.info("Converting input "+input.toString()+
				"\n\t as list: "+inputList.toString()+
				"\n\t length: "+size
				);*/

		// Convert the elements
		outputOI.resize(output, size);
		for (int index = 0; index < size; index++) {
			Object inputElement = inputList.get(index);
			Object outputElement = null;
			if (inputElement!=null){
			TypeInfo wType = RType.getHiveType((Writable) inputElement);
			Converter converter = ObjectInspectorConverters.getConverter(
					TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(wType),
					elemOI);
			/*LOG.info("Converting element type "+wType+" at index "+index+
					"\n\tusing"+converter.getClass().getSimpleName()+
					"\n\tfrom: "+inputElement.toString()+
					"\n\t into: "+outputElement.toString()
					);*/
			outputElement = converter.convert(
					inputElement);
			}/*else {
				LOG.info("inputElement at: "+index+" is null");
			}*/
			outputOI.set(output, index, outputElement);
		}
		return output;
	}
}
public int next(Writable data) throws IOException {
    barrStr.reset();
    
/* R returns things as a list of lists of keys and a list of lists of values
 * This is necessary for things to work out.  Otherwise, R will flatten the compound keys
 * into multiple single-value keys.
 * 
 * There will always only be one key, but R will return multiple values mapped to a single key.
 * For 'native' format, this is ok, but if we're actually writing to a table, we want to 
 * write a row for each value returned.  So we read a key, then read the FIRST list of values, 
 * but keep track of how many lists of values are left in listValuesLeftCount.
 *  
*/    if (listValuesLeftCount==0){

    //LOG.info("Reading new key");
    numKeys = 1;
    
    if (!useNative && RTypedBytesSerDe.WRAP_KEYS){
    	// Wrap it as a list TWICE, because R will try to c() things once deserialized
    	RType rtype = tbIn.getInput().readType(); 
    	if (rtype==null){
    		return -1;
    	}
    	if (rtype!=RType.VECTOR) {
    		throw new RuntimeException("Error: expecting vector<vector<keys>>, instead of "+rtype.name());
    	}
    	int n = tbIn.getInput().readVectorHeader(); 
    	if (n != 1) {
    		throw new RuntimeException("Error: the vector<vector<keys>> must have one entry, instead of "+n);
    	}
    	rtype = tbIn.getInput().readType(); 
    	if (rtype!=RType.VECTOR) {
    		throw new RuntimeException("Error: expecting vector<keys>, instead of "+rtype.name());
    	}
		numKeys = tbIn.getInput().readVectorHeader();
	}
    for (int i=0;i<numKeys;i++){
    //LOG.info("Reading key(s) "+(i+1) +" of "+numKeys);
     boolean status = this.nextElement(0);
     if (!status){
    	 return -1;
     }
    }
    keysBarrStr.reset();
    keysBarrStr.write(barrStr.getData(), 0, barrStr.getLength());
   
    }else {
    	/*LOG.info("Reusing old key: "+listValuesLeftCount +" values left");
        	barrStr.write(keysBarrStr.getData(), 0, keysBarrStr.getLength());*/
    }
	//row.set(0, deserializeField(tbIn, columnTypes.get(0), row.get(0)));

	int numColumns = this.dstOIns.size();
	if (!useNative && RTypedBytesSerDe.WRAP_VALUES){
		// Wrap it as a list TWICE, because R will try to c() things once deserialized
		if (listValuesLeftCount==0){
    	RType rtype = tbIn.getInput().readType(); 
    	if (rtype!=RType.VECTOR) {
    		throw new RuntimeException("Error: expecting vector<vector<keys>>, instead of "+rtype.name());
    	}
    	this.listValuesLeftCount = tbIn.getInput().readVectorHeader();
		}
    	RType rtype = tbIn.getInput().readType(); 
    	if (rtype!=RType.VECTOR) {
    		throw new RuntimeException("Error: expecting vector of keys, instead of "+rtype.name());
    	}
		int numValues = tbIn.getInput().readVectorHeader();
		if (numValues+numKeys != numColumns){
			throw new RuntimeException("Error: number of keys: "+numKeys+" + number of values: "+numValues+
					"\n\tis not equal to total number of columns: "+numColumns);
		}
	}else {
		// If reading as native, treat the whole thing as one value
		listValuesLeftCount = 1;
	}
    for (int i = numKeys; i < numColumns; i++) {
        //LOG.info("Reading values, column "+(i+1)+ " of "+numColumns);
    	boolean status = nextElement(i);
    	if (!status) {
    		throw new RuntimeException("No value for value column "
    	              + (i+1));
    	}
    }
    this.listValuesLeftCount = listValuesLeftCount - 1;
    
    if (barrStr.getLength() > 0) {
        ((BytesWritable) data).set(barrStr.getData(), 0, barrStr.getLength());
    }
    return barrStr.getLength();
  }

  private void write(int pos, Writable inpw) throws IOException {
    String typ = columnTypes.get(pos).getTypeName();

    Object inpo = inpw;
    if (inpw instanceof ArrayWritable){
    	inpo = ((ArrayWritable)inpw).get();  // You must convert the array, not the arraywritable
    	//LOG.info("First element: "+((Writable[])inpo)[0]);
    }
    Object w = inpo;
    Converter converter = converters.get(pos);
    if (converter!=null){
    	w = converter.convert(inpo);
    }
    /*LOG.info("Write column: "+pos+" with type: "+typ+"\n\tvalue: "+inpo.toString()+
    		"\n\tconverted with: "+converter.getClass().getSimpleName()+
    		"\n\tinto value"+w.toString());*/
    if (typ.equalsIgnoreCase(Constants.BINARY_TYPE_NAME)) {
      if (w instanceof TypedBytesWritable){
          //LOG.info("Writing as typebytes, column: "+pos+", length: "+((RTypedBytesWritable)w).getLength());
    	  tbOut.writeTypedBytes((TypedBytesWritable)w);
      }else {
    	  //LOG.info("Writing as bytes, column "+pos);
    	  tbOut.writeBytes((BytesWritable)w);
      }
    }else if (typ.equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)) {
      tbOut.writeBoolean((BooleanWritable) w);
    } else if (typ.equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)) {
      tbOut.writeByte((ByteWritable) w);
    } else if (typ.equalsIgnoreCase(Constants.SMALLINT_TYPE_NAME)) {
      tbOut.writeShort((ShortWritable) w);
    } else if (typ.equalsIgnoreCase(Constants.INT_TYPE_NAME)) {
      tbOut.writeInt((IntWritable) w);
    } else if (typ.equalsIgnoreCase(Constants.BIGINT_TYPE_NAME)) {
      tbOut.writeLong((LongWritable) w);
    } else if (typ.equalsIgnoreCase(Constants.FLOAT_TYPE_NAME)) {
      tbOut.writeFloat((FloatWritable) w);
    } else if (typ.equalsIgnoreCase(Constants.DOUBLE_TYPE_NAME)) {
      tbOut.writeDouble((DoubleWritable) w);
    } else if (typ.equalsIgnoreCase(Constants.STRING_TYPE_NAME)) {
      tbOut.writeText((Text) w);
    } else if (columnTypes.get(pos).getCategory() == ObjectInspector.Category.LIST) {
    	/*PrimitiveCategory pcat = ((PrimitiveTypeInfo)
				((ListTypeInfo)columnTypes.get(pos)).getListElementTypeInfo()).
				getPrimitiveCategory();
    	tbOut.writeArray((ArrayWritable)w,RType.valueOf(pcat));*/
    	tbOut.writeVector((List)w);
    } else {
      assert false;
    }
  }

  public void close() throws IOException {
    if (din != null) {
      din.close();
    }
  }

  public static RType getType(int code) {
    return RType.valueOf(code);
  }
}
