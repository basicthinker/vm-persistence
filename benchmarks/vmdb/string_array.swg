
%typemap(in) char * {
  if (!$input) {
    $1 = NULL;
  } else {
    const char *c_string = JCALL2(GetStringUTFChars, jenv, (jstring)$input, 0);
    $1 = NewZeroHashString(c_string);
    JCALL2(ReleaseStringUTFChars, jenv, $input, c_string);
  }
}

%typemap(freearg) char * {
  if ($1) free($1);
}

%typemap(out) char * {
  jresult = JCALL1(NewStringUTF, jenv, LoadString($1));
  free($1);
}

/* This tells SWIG to treat char ** as a special case when used as a parameter
   in a function call */
%typemap(in) char ** (jint size) {
  int i = 0;
  if (!$input) {
    $1 = NULL;
  } else {
    size = JCALL1(GetArrayLength, jenv, $input);
    $1 = (char **)malloc((size + 1) * sizeof(char *));
    /* make a copy of each string */
    for (i = 0; i < size; i++) {
      jstring j_string =
          (jstring)JCALL2(GetObjectArrayElement, jenv, $input, i);
      const char * c_string = JCALL2(GetStringUTFChars, jenv, j_string, 0);
      $1[i] = NewZeroHashString(c_string);
      JCALL2(ReleaseStringUTFChars, jenv, j_string, c_string);
      JCALL1(DeleteLocalRef, jenv, j_string);
    }
    $1[i] = 0;
  }
}

/* This cleans up the memory we malloc'd before the function call */
%typemap(freearg) char ** {
  if ($1) free($1);
}

/* This allows a C function to return a char ** as a Java String array */
%typemap(out) char ** {
  int i;
  int len = 0;
  jstring temp_string;
  const jclass clazz = JCALL1(FindClass, jenv, "java/lang/String");

  while ($1[len]) len++;
  jresult = JCALL3(NewObjectArray, jenv, len, clazz, NULL);
  /* exception checking omitted */

  for (i = 0; i < len; i++) {
    temp_string = JCALL1(NewStringUTF, jenv, LoadString($1[i]));
    JCALL3(SetObjectArrayElement, jenv, jresult, i, temp_string);
    JCALL1(DeleteLocalRef, jenv, temp_string);
  }
  free($1);
}

/* These 3 typemaps tell SWIG what JNI and Java types to use */
%typemap(jni) char ** "jobjectArray"
%typemap(jtype) char ** "String[]"
%typemap(jstype) char ** "String[]"

/* These 2 typemaps handle the conversion of the jtype to jstype typemap type
   and vice versa */
%typemap(javain) char ** "$javainput"
%typemap(javaout) char ** {
  return $jnicall;
}

