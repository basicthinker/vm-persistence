package com.yahoo.ycsb.db;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

/**
 * VMDB client binding for YCSB.
 *
 * Scanning operations are not supported.
 */
public class VMDBClient extends DB {
  public static final int ERROR = -1;

  public void init() throws DBException {
  }

  public void cleanup() throws DBException {
  }

  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    return 1;
  }

  @Override
  public int insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    HashMap<String, String> valueMap = StringByteIterator.getStringMap(values);
    return 1;
  }

  @Override
  public int delete(String table, String key) {
    return 1;
  }

  @Override
  public int update(String table, String key,
      HashMap<String, ByteIterator> values) {
    HashMap<String, String> valueMap = StringByteIterator.getStringMap(values);
    return 1;
  }

  @Override
  public int scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("VMDatabase does not support scan semantics.");
    return ERROR;
  }
}

