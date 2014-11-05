package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * VMDB client binding for YCSB.
 *
 * Scanning operations are not supported.
 */
public class VMDBClient extends DB {
  static {
    try {
      System.loadLibrary("vmdb");
    } catch (UnsatisfiedLinkError e) {
      System.err.println("Native code library failed to load. " + e);
      System.exit(1);
    }
  }

  private static final VMDatabase vmdb = new VMDatabase();

  public static final int OK = 0;
  public static final int ERROR = -1;

  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    if (fields == null) {
      String[] resultArray = vmdb.Read(table + key, null);
      int len = resultArray.length / 2;
      for (int i = 0; i < len; ++i) {
        result.put(resultArray[i],
            new StringByteIterator(resultArray[len + i]));
      }
    } else {
      String[] fieldArray = new String[fields.size()];
      fields.toArray(fieldArray);

      String[] valueArray = vmdb.Read(table + key, fieldArray);

      assert fieldArray.length == valueArray.length;
      for (int i = 0; i < fieldArray.length; ++i) {
        result.put(fieldArray[i],
            new StringByteIterator(valueArray[i]));
      }
    }
    return result.isEmpty() ? ERROR : OK;
  }

  @Override
  public int insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    String[] fieldArray = new String[values.size()];
    String[] valueArray = new String[values.size()];
    int i = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldArray[i] = entry.getKey();
      valueArray[i] = entry.getValue().toString();
      ++i;
    }
    int n = vmdb.Insert(table + key, fieldArray, valueArray);
    return n > 0 ? OK : ERROR;
  }

  @Override
  public int delete(String table, String key) {
    int n = vmdb.Delete(table + key);
    return n > 0 ? OK : ERROR;
  }

  @Override
  public int update(String table, String key,
      HashMap<String, ByteIterator> values) {
    String[] fieldArray = new String[values.size()];
    String[] valueArray = new String[values.size()];
    int i = 0;
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      fieldArray[i] = entry.getKey();
      valueArray[i] = entry.getValue().toString();
      ++i;
    }
    int n = vmdb.Update(table + key, fieldArray, valueArray);
    return n > 0 ? OK : ERROR;
  }

  @Override
  public int scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("VMDatabase does not support scan semantics.");
    return ERROR;
  }
}

