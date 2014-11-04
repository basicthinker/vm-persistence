package test;

class Test {
  static {
    try {
      System.loadLibrary("vmdb");
    } catch (UnsatisfiedLinkError e) {
      System.err.println("Native code library failed to load. " + e);
      System.exit(1);
    }
  }

  public static void main(String argv[]) {
    VMDatabase vmdb = new VMDatabase();

    String table_key = "table+key";
    String[] results;
    results = vmdb.Read(table_key, new String[]{"1", "3"});
    if (results.length == 0) {
      System.out.println("Pass Test 0: read void");
    }

    String[] fields = {"1", "2", "3"};
    String[] values = {"one", "two", "three"};

    if (vmdb.Insert(table_key, fields, values) != 1) {
      System.out.println("Insert failed!");
    }

    results = vmdb.Read(table_key, new String[]{"1", "3"});
    if (results.length == 2 &&
        results[0].equals("one") && results[1].equals("three")) {
      System.out.println("Pass Test 1: insert and read");
    }

    if (vmdb.Update(table_key, new String[]{"2"}, new String[]{"Two"}) != 1) {
      System.out.println("Update 2 failed!");
    }

    if (vmdb.Update(table_key, new String[]{"4"}, new String[]{"Err"}) != 0) {
      System.out.println("Update 4 wrong return value!");
    }

    results = vmdb.Read(table_key, fields);
    System.out.println("Test 2: update and read");
    for (int i = 0; i < results.length; ++i) {
      System.out.println("\t" + results[i]);
    }
    System.out.println("End of Test 2");

    if (vmdb.Delete(table_key) != 1) {
      System.out.println("Delete failed!");
    }

    results = vmdb.Read(table_key, fields);
    if (results.length == 0) {
      System.out.println("Pass Test 3: delete and read");
    }
  }
}

