#include <jni.h>
#include <stdint.h>

#ifndef EDS_RAIO_H
#define EDS_RAIO_H

int init_raio(JNIEnv *env);
void clear_raio(JNIEnv *env);
ssize_t raio_read(JNIEnv *env, jobject raio, uint8_t *buf, size_t count);
ssize_t raio_pread(JNIEnv *env, jobject raio, uint8_t *buf, size_t count, off64_t pos);
ssize_t raio_write(JNIEnv *env, jobject raio, const uint8_t *buf, size_t count);
ssize_t raio_pwrite(JNIEnv *env, jobject raio, const uint8_t *buf, size_t count, off64_t pos);
int raio_seek(JNIEnv *env, jobject raio, off64_t pos);
off64_t raio_get_pos(JNIEnv *env, jobject raio);
int raio_flush(JNIEnv *env, jobject raio);
off64_t raio_get_size(JNIEnv *env, jobject raio);

#endif //EDS_RAIO_H
