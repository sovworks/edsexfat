#include <stdint.h>
#include <sys/types.h>

#include "../util/jniutil.h"
#include "raio.h"


#define RAIO_CLASSNAME "com/sovworks/eds/fs/RandomAccessIO"
#define UTIL_CLASSNAME "com/sovworks/eds/fs/util/Util"

#define READ_SIG "read","([BII)I"
#define WRITE_SIG "write","([BII)V"
#define FLUSH_SIG "flush","()V"
#define SEEK_SIG "seek","(J)V"
#define GET_FILE_POINTER_SIG "getFilePointer","()J"
#define LENGTH_SIG "length","()J"

#define PREAD_SIG "pread","(Lcom/sovworks/eds/fs/RandomAccessIO;[BIIJ)I"
#define PWRITE_SIG "pwrite","(Lcom/sovworks/eds/fs/RandomAccessIO;[BIIJ)I"

extern JavaVM *Jvm;

jclass RAIOClass;
jclass UtilClass;

jmethodID readMethodId;
jmethodID writeMethodId;
jmethodID flushMethodId;
jmethodID seekMethodId;
jmethodID getFilePointerMethodId;
jmethodID lengthMethodId;

jmethodID preadMethodId;
jmethodID pwriteMethodId;


static jint cache_classes(JNIEnv *env)
{
    jclass cls = (*env)->FindClass(env, RAIO_CLASSNAME);
    if (cls == NULL)
        return JNI_ERR;

    RAIOClass = (*env)->NewGlobalRef(env,cls);
    (*env)->DeleteLocalRef(env, cls);
    if (RAIOClass == NULL)
        return JNI_ERR;

    cls = (*env)->FindClass(env, UTIL_CLASSNAME);
    if (cls == NULL)
        return JNI_ERR;

    UtilClass = (*env)->NewGlobalRef(env,cls);
    (*env)->DeleteLocalRef(env, cls);
    if (UtilClass == NULL)
        return JNI_ERR;

    return JNI_OK;
}


static jint cache_methods(JNIEnv *env)
{
    CACHE_METHOD(readMethodId, RAIOClass, READ_SIG);
    CACHE_METHOD(writeMethodId, RAIOClass, WRITE_SIG);
    CACHE_METHOD(flushMethodId, RAIOClass, FLUSH_SIG);
    CACHE_METHOD(seekMethodId, RAIOClass, SEEK_SIG);
    CACHE_METHOD(getFilePointerMethodId, RAIOClass, GET_FILE_POINTER_SIG);
    CACHE_METHOD(lengthMethodId, RAIOClass, LENGTH_SIG);

    CACHE_STATIC_METHOD(preadMethodId, UtilClass, PREAD_SIG);
    CACHE_STATIC_METHOD(pwriteMethodId, UtilClass, PWRITE_SIG);
    return JNI_OK;
}

static void clean_classes_cache(JNIEnv *env)
{
    (*env)->DeleteGlobalRef(env, RAIOClass);
    (*env)->DeleteGlobalRef(env, UtilClass);
}

int init_raio(JNIEnv *env)
{
    if(cache_classes(env)==JNI_ERR)
        return JNI_ERR;

    if(cache_methods(env)==JNI_ERR)
        return JNI_ERR;

    return JNI_OK;
}

void clear_raio(JNIEnv *env)
{
    clean_classes_cache(env);
}

ssize_t raio_read(JNIEnv *env, jobject raio, uint8_t *buf, size_t count)
{
    int rc;
    ssize_t res = 0;
    jbyteArray byteArray;
    jint bytesRead;

    byteArray = (*env)->NewByteArray(env,count);
    if(byteArray == NULL)
    {
        res = (ssize_t)-1;
        return res;
    }

    rc = call_jni_int_func(env, raio, readMethodId, &bytesRead,
                            byteArray, (jint)0, (jint)count);
    if(rc)
    {
        res = (ssize_t)-1;
        (*env)->DeleteLocalRef(env, byteArray);
        return res;
    }
    (*env)->GetByteArrayRegion(env, byteArray, 0, bytesRead, (jbyte *) buf);
    res = bytesRead;
    (*env)->DeleteLocalRef(env, byteArray);
    return res;
}

ssize_t raio_pread(JNIEnv *env, jobject raio, uint8_t *buf, size_t count, off64_t pos)
{
    int rc;
    ssize_t res = 0;
    jbyteArray byteArray;
    jint bytesRead;

    byteArray = (*env)->NewByteArray(env,count);
    if(byteArray == NULL)
    {
        res = (ssize_t)-1;
        return res;
    }

    rc = call_jni_static_int_func(env, UtilClass, preadMethodId, &bytesRead,
                                  raio, byteArray, (jint)0, (jint)count, (jlong)pos);
    if(rc)
    {
        res = (ssize_t)-1;
        (*env)->DeleteLocalRef(env, byteArray);
        return res;
    }
    (*env)->GetByteArrayRegion(env, byteArray, 0, bytesRead, (jbyte *) buf);
    res = bytesRead;
    (*env)->DeleteLocalRef(env, byteArray);
    return res;
}

ssize_t raio_write(JNIEnv *env, jobject raio, const uint8_t *buf, size_t count)
{
    int rc;
    ssize_t res = 0;
    jbyteArray byteArray;

    byteArray = (*env)->NewByteArray(env,count);
    if(byteArray == NULL)
    {
        res = (ssize_t)-1;
        return res;
    }
    (*env)->SetByteArrayRegion(env, byteArray, 0, count, (const jbyte *) buf);

    rc = call_jni_void_func(env, raio, writeMethodId,
                                  byteArray, (jint)0, (jint) count);
    if(rc)
        res = (ssize_t)-1;
    else
        res = count;
    (*env)->DeleteLocalRef(env, byteArray);

    return res;
}

ssize_t raio_pwrite(JNIEnv *env, jobject raio, const uint8_t *buf, size_t count, off64_t pos)
{
    int rc;
    ssize_t res = 0;
    jbyteArray byteArray;

    byteArray = (*env)->NewByteArray(env,count);
    if(byteArray == NULL)
    {
        res = (ssize_t)-1;
        return res;
    }
    (*env)->SetByteArrayRegion(env, byteArray, 0, count, (const jbyte *) buf);

    rc = call_jni_static_int_func(env, UtilClass, pwriteMethodId, &res,
                                  raio, byteArray, (jint)0, (jint) count, (jlong)pos);
    if(rc)
        res = (ssize_t)-1;
    else
        res = count;
    (*env)->DeleteLocalRef(env, byteArray);

    return res;
}

int raio_seek(JNIEnv *env, jobject raio, off64_t pos)
{
    return call_jni_void_func(env, raio, seekMethodId,
                             (jlong)pos);
}

off64_t raio_get_pos(JNIEnv *env, jobject raio)
{
    int res = 0;
    jlong pos;

    res = call_jni_long_func(env, raio, getFilePointerMethodId, &pos);
    if(res)
        pos = -1;
    return (off64_t) pos;
}

int raio_flush(JNIEnv *env, jobject raio)
{
    return call_jni_void_func(env, raio, flushMethodId);
}

off64_t raio_get_size(JNIEnv *env, jobject raio)
{
    int res = 0;
    jlong size;

    res = call_jni_long_func(env, raio, lengthMethodId, &size);
    if(res)
        size = -1;
    return (off64_t) size;
}