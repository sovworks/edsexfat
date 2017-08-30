#include <inttypes.h>
#include <asm/errno.h>
#include <android/log.h>
#include <exfat.h>
#include "com_sovworks_eds_fs_exfat_ExFat.h"
#include "libexfat/exfat.h"
#include "../util/jniutil.h"
#include "raio.h"
#include "mkfs/mkexfat.h"

#define EDSEXFAT_VERSION 1001

#define LOGI(...) __android_log_print(ANDROID_LOG_INFO, "EDS (native code edsexfat)", __VA_ARGS__);

#define EXFAT_CLASSNAME "com/sovworks/eds/fs/exfat/ExFat"
#define FILE_STAT_CLASSNAME "com/sovworks/eds/fs/util/FileStat"
#define COLLECTION_CLASSNAME "java/util/Collection"

#define EXFAT_PTR_SIG "_exfatPtr","J"
#define EXFAT_IMAGE_SIG "_exfatImageFile","Lcom/sovworks/eds/fs/RandomAccessIO;"
#define IS_DIR_SIG "isDir","Z"
#define SIZE_SIG "size","J"
#define MOD_DATE_SIG "modTime","J"
#define FILENAME_SIG "fileName","Ljava/lang/String;"

#define COLLECTION_ADD_SIG "add","(Ljava/lang/Object;)Z"

static jclass ExFatClass;
static jclass FileStatClass;
static jclass CollectionClass;

static jfieldID exfatPtr;
static jfieldID exfatImage;
static jfieldID IsDir;
static jfieldID Size;
static jfieldID ModDate;
static jfieldID FileName;

static jmethodID CollectionAdd;


static jint cache_classes(JNIEnv *env)
{
    jclass cls = (*env)->FindClass(env, EXFAT_CLASSNAME);
    if (cls == NULL)
        return JNI_ERR;

    ExFatClass = (*env)->NewGlobalRef(env,cls);
    (*env)->DeleteLocalRef(env, cls);
    if (ExFatClass == NULL)
        return JNI_ERR;

    cls = (*env)->FindClass(env,FILE_STAT_CLASSNAME);
    if (cls == NULL)
        return JNI_ERR;

    FileStatClass = (*env)->NewGlobalRef(env,cls);
    (*env)->DeleteLocalRef(env, cls);
    if (FileStatClass == NULL)
        return JNI_ERR;

    cls = (*env)->FindClass(env,COLLECTION_CLASSNAME);
    if (cls == NULL)
        return JNI_ERR;

    CollectionClass = (*env)->NewGlobalRef(env,cls);
    (*env)->DeleteLocalRef(env, cls);
    if (CollectionClass == NULL)
        return JNI_ERR;

    return JNI_OK;
}


static jint cache_methods(JNIEnv *env)
{
    CACHE_FIELD(exfatPtr, ExFatClass, EXFAT_PTR_SIG);
    CACHE_FIELD(exfatImage, ExFatClass, EXFAT_IMAGE_SIG);

    CACHE_FIELD(IsDir, FileStatClass, IS_DIR_SIG);
    CACHE_FIELD(Size, FileStatClass, SIZE_SIG);
    CACHE_FIELD(FileName, FileStatClass, FILENAME_SIG);
    CACHE_FIELD(ModDate, FileStatClass, MOD_DATE_SIG);

    CACHE_METHOD(CollectionAdd, CollectionClass, COLLECTION_ADD_SIG);

    return JNI_OK;
}

static void clean_classes_cache(JNIEnv *env)
{
    (*env)->DeleteGlobalRef(env, ExFatClass);
    (*env)->DeleteGlobalRef(env,FileStatClass);
    (*env)->DeleteGlobalRef(env,CollectionClass);
}

static struct exfat *get_exfat(JNIEnv *env, jobject instance)
{
    struct exfat *ef = (struct exfat *) (*env)->GetLongField(env, instance, exfatPtr);
    if(ef && ef->dev)
        ef->dev->env = env;

    return ef;

}

static bool verify_vbr_checksum(struct exfat_dev* dev, void* sector,
                                off64_t sector_size)
{
    uint32_t vbr_checksum;
    int i;

    if (exfat_pread(dev, sector, sector_size, 0) < 0)
    {
        exfat_error("failed to read boot sector");
        return false;
    }
    vbr_checksum = exfat_vbr_start_checksum(sector, sector_size);
    for (i = 1; i < 11; i++)
    {
        if (exfat_pread(dev, sector, sector_size, i * sector_size) < 0)
        {
            exfat_error("failed to read VBR sector");
            return false;
        }
        vbr_checksum = exfat_vbr_add_checksum(sector, sector_size,
                                              vbr_checksum);
    }
    if (exfat_pread(dev, sector, sector_size, i * sector_size) < 0)
    {
        exfat_error("failed to read VBR checksum sector");
        return false;
    }
    for (i = 0; i < sector_size / sizeof(vbr_checksum); i++)
        if (le32_to_cpu(((const le32_t*) sector)[i]) != vbr_checksum)
        {
            exfat_error("invalid VBR checksum 0x%x (expected 0x%x)",
                        le32_to_cpu(((const le32_t*) sector)[i]), vbr_checksum);
            return false;
        }
    return true;
}

static int commit_super_block(const struct exfat* ef)
{
    if (exfat_pwrite(ef->dev, ef->sb, sizeof(struct exfat_super_block), 0) < 0)
    {
        exfat_error("failed to write super block");
        return 1;
    }
    return exfat_fsync(ef->dev);
}

static int prepare_super_block(const struct exfat* ef)
{
    if (le16_to_cpu(ef->sb->volume_state) & EXFAT_STATE_MOUNTED)
        exfat_warn("volume was not unmounted cleanly");

    if (ef->ro)
        return 0;

    ef->sb->volume_state = cpu_to_le16(
            le16_to_cpu(ef->sb->volume_state) | EXFAT_STATE_MOUNTED);
    return commit_super_block(ef);
}

static uint64_t rootdir_size(const struct exfat* ef)
{
    uint32_t clusters = 0;
    uint32_t clusters_max = le32_to_cpu(ef->sb->cluster_count);
    cluster_t rootdir_cluster = le32_to_cpu(ef->sb->rootdir_cluster);

    /* Iterate all clusters of the root directory to calculate its size.
       It can't be contiguous because there is no flag to indicate this. */
    do
    {
        if (clusters == clusters_max) /* infinite loop detected */
        {
            exfat_error("root directory cannot occupy all %d clusters",
                        clusters);
            return 0;
        }
        if (CLUSTER_INVALID(*ef->sb, rootdir_cluster))
        {
            exfat_error("bad cluster %#x while reading root directory",
                        rootdir_cluster);
            return 0;
        }
        rootdir_cluster = exfat_next_cluster(ef, ef->root, rootdir_cluster);
        clusters++;
    }
    while (rootdir_cluster != EXFAT_CLUSTER_END);

    return (uint64_t) clusters * CLUSTER_SIZE(*ef->sb);
}

static void exfat_free(struct exfat* ef)
{
    free(ef->dev);
    ef->dev = NULL;
    free(ef->root);
    ef->root = NULL;
    free(ef->zero_cluster);
    ef->zero_cluster = NULL;
    free(ef->cmap.chunk);
    ef->cmap.chunk = NULL;
    free(ef->upcase);
    ef->upcase = NULL;
    free(ef->sb);
    ef->sb = NULL;
    free(ef);
}

static struct exfat_dev* exfat_open_raio(JNIEnv *env, jobject raio, enum exfat_mode mode)
{
    struct exfat_dev* dev;
    off64_t size;

    dev = malloc(sizeof(struct exfat_dev));
    if (dev == NULL)
    {
        exfat_error("failed to allocate memory for device structure");
        return NULL;
    }
    dev->env = env;
    dev->mode = mode;
    dev->raio = (*env)->NewGlobalRef(env, raio);
    if(dev->raio == NULL)
    {
        free(dev);
        return NULL;
    }
    size = raio_get_size(env, dev->raio);
    if(size == -1)
    {
        (*env)->DeleteGlobalRef(env, dev->raio);
        free(dev);
        exfat_error("failed to get size of the image");
        return NULL;
    }
    dev->size = size;
    return dev;
}

static struct exfat_dev* exfat_open(JNIEnv *env, jobject exfat_instance, enum exfat_mode mode)
{
    struct exfat_dev* dev;
    jobject local_ref = (*env)->GetObjectField(env, exfat_instance, exfatImage);
    if(local_ref == NULL)
        return NULL;
    dev = exfat_open_raio(env, local_ref, mode);
    (*env)->DeleteLocalRef(env, local_ref);
    return dev;
}

int exfat_close(struct exfat_dev* dev)
{
    JNIEnv  *env = dev->env;
    (*env)->DeleteGlobalRef(env, dev->raio);
    free(dev);
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_openFS(JNIEnv *env, jobject instance, jboolean readOnly)
{
    int rc;
    struct exfat* ef;

    exfat_tzset();
    ef = malloc(sizeof(struct exfat));
    if (ef == NULL)
    {
        exfat_error("failed to allocate memory for the exfat structure");
        return 0;
    }
    memset(ef, 0, sizeof(struct exfat));

    ef->dev = exfat_open(env, instance, readOnly ? EXFAT_MODE_RO : EXFAT_MODE_RW);
    if (ef->dev == NULL)
    {
        exfat_error("failed to allocate memory for device structure");
        exfat_free(ef);
        return 0;
    }

    ef->sb = malloc(sizeof(struct exfat_super_block));
    if (ef->sb == NULL)
    {
        exfat_error("failed to allocate memory for the super block");
        exfat_free(ef);
        return 0;
    }
    memset(ef->sb, 0, sizeof(struct exfat_super_block));

    if (exfat_pread(ef->dev, ef->sb, sizeof(struct exfat_super_block), 0) < 0)
    {
        exfat_error("failed to read boot sector");
        exfat_free(ef);
        return 0;
    }
    if (memcmp(ef->sb->oem_name, "EXFAT   ", 8) != 0)
    {
        exfat_error("exFAT file system is not found");
        exfat_free(ef);
        return 0;
    }
    /* sector cannot be smaller than 512 bytes */
    if (ef->sb->sector_bits < 9)
    {
        exfat_error("too small sector size: 2^%hhd", ef->sb->sector_bits);
        exfat_free(ef);
        return 0;
    }
    /* officially exFAT supports cluster size up to 32 MB */
    if ((int) ef->sb->sector_bits + (int) ef->sb->spc_bits > 25)
    {
        exfat_error("too big cluster size: 2^(%hhd+%hhd)",
                    ef->sb->sector_bits, ef->sb->spc_bits);
        exfat_free(ef);
        return 0;
    }
    ef->zero_cluster = malloc(CLUSTER_SIZE(*ef->sb));
    if (ef->zero_cluster == NULL)
    {
        exfat_error("failed to allocate zero sector");
        exfat_free(ef);
        return 0;
    }
    /* use zero_cluster as a temporary buffer for VBR checksum verification */
    if (!verify_vbr_checksum(ef->dev, ef->zero_cluster, SECTOR_SIZE(*ef->sb)))
    {
        exfat_free(ef);
        return 0;
    }
    memset(ef->zero_cluster, 0, CLUSTER_SIZE(*ef->sb));
    if (ef->sb->version.major != 1 || ef->sb->version.minor != 0)
    {
        exfat_error("unsupported exFAT version: %hhu.%hhu",
                    ef->sb->version.major, ef->sb->version.minor);
        exfat_free(ef);
        return 0;
    }
    if (ef->sb->fat_count != 1)
    {
        exfat_error("unsupported FAT count: %hhu", ef->sb->fat_count);
        exfat_free(ef);
        return 0;
    }
    if (le64_to_cpu(ef->sb->sector_count) * SECTOR_SIZE(*ef->sb) >
        exfat_get_size(ef->dev))
    {
        /* this can cause I/O errors later but we don't fail mounting to let
           user rescue data */
        exfat_warn("file system in sectors is larger than device: "
                           "%"PRIu64" * %d > %"PRIu64,
                le64_to_cpu(ef->sb->sector_count), SECTOR_SIZE(*ef->sb),
                exfat_get_size(ef->dev));
    }
    if ((off64_t) le32_to_cpu(ef->sb->cluster_count) * CLUSTER_SIZE(*ef->sb) >
        exfat_get_size(ef->dev))
    {
        exfat_error("file system in clusters is larger than device: "
                            "%u * %d > %"PRIu64,
                le32_to_cpu(ef->sb->cluster_count), CLUSTER_SIZE(*ef->sb),
                exfat_get_size(ef->dev));
        exfat_free(ef);
        return 0;
    }

    ef->root = malloc(sizeof(struct exfat_node));
    if (ef->root == NULL)
    {
        exfat_error("failed to allocate root node");
        exfat_free(ef);
        return 0;
    }
    memset(ef->root, 0, sizeof(struct exfat_node));
    ef->root->attrib = EXFAT_ATTRIB_DIR;
    ef->root->start_cluster = le32_to_cpu(ef->sb->rootdir_cluster);
    ef->root->fptr_cluster = ef->root->start_cluster;
    ef->root->name[0] = cpu_to_le16('\0');
    ef->root->size = rootdir_size(ef);
    if (ef->root->size == 0)
    {
        exfat_free(ef);
        return 0;
    }
    /* exFAT does not have time attributes for the root directory */
    ef->root->mtime = 0;
    ef->root->atime = 0;
    /* always keep at least 1 reference to the root node */
    exfat_get_node(ef->root);

    rc = exfat_cache_directory(ef, ef->root);
    if (rc != 0)
        goto error;
    if (ef->upcase == NULL)
    {
        exfat_error("upcase table is not found");
        goto error;
    }
    if (ef->cmap.chunk == NULL)
    {
        exfat_error("clusters bitmap is not found");
        goto error;
    }

    if (prepare_super_block(ef) != 0)
        goto error;

    return (jlong) ef;

    error:
    exfat_put_node(ef, ef->root);
    exfat_reset_cache(ef);
    exfat_free(ef);
    return 0;

}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_getAttr(JNIEnv *env, jobject instance, jobject fileStat, jstring pathString)
{
    const char *path;
    struct exfat_node* node;
    struct stat stbuf;
    int rc;

    struct exfat * ef = get_exfat(env, instance);
    if(!ef)
        return -1;

    path = (*env)->GetStringUTFChars(env, pathString, NULL);
    exfat_debug("[%s] %s", __func__, path);
    rc = exfat_lookup(ef, &node, path);
    (*env)->ReleaseStringUTFChars(env, pathString, path);
    if (rc != 0)
        return -ENOENT;

    exfat_stat(ef, node, &stbuf);
    exfat_put_node(ef, node);
    (*env)->SetBooleanField(env, fileStat, IsDir, S_ISDIR(stbuf.st_mode) ? true : false);
    (*env)->SetLongField(env, fileStat, Size, stbuf.st_size);
    (*env)->SetLongField(env, fileStat, ModDate, stbuf.st_mtime);
    return 0;
}

JNIEXPORT jint JNICALL
Java_com_sovworks_eds_fs_exfat_ExFat_readDir(
        JNIEnv *env, jobject instance, jstring pathString, jobject filesList)
{
    jstring childNameString;
    jthrowable exc;
    struct exfat_node* parent;
    struct exfat_node* node;
    struct exfat_iterator it;
    int rc;
    char name[EXFAT_UTF8_NAME_BUFFER_MAX];

    struct exfat * ef = get_exfat(env, instance);
    if(!ef)
        return -1;

    const char *path = (*env)->GetStringUTFChars(env, pathString, 0);

    exfat_debug("[%s] %s", __func__, path);

    rc = exfat_lookup(ef, &parent, path);
    if (rc != 0)
    {
        (*env)->ReleaseStringUTFChars(env, pathString, path);
        return rc;
    }
    if (!(parent->attrib & EXFAT_ATTRIB_DIR))
    {
        exfat_put_node(ef, parent);
        exfat_error("'%s' is not a directory (%#hx)", path, parent->attrib);
        (*env)->ReleaseStringUTFChars(env, pathString, path);
        return -ENOTDIR;
    }
    rc = exfat_opendir(ef, parent, &it);
    if (rc != 0)
    {
        exfat_put_node(ef, parent);
        exfat_error("failed to open directory '%s'", path);
        (*env)->ReleaseStringUTFChars(env, pathString, path);
        return rc;
    }

    (*env)->ReleaseStringUTFChars(env, pathString, path);

    while ((node = exfat_readdir(&it)))
    {
        exfat_get_name(node, name);
        exfat_debug("[%s] %s: %s, %"PRId64" bytes, cluster 0x%x", __func__,
                    name, node->is_contiguous ? "contiguous" : "fragmented",
                    node->size, node->start_cluster);
        childNameString = (*env)->NewStringUTF(env, name);
        if(childNameString == NULL)
        {
            rc = -1;
            break;
        }
        (*env)->CallBooleanMethod(env, filesList, CollectionAdd, childNameString);
        (*env)->DeleteLocalRef(env, childNameString);
        exc = (*env)->ExceptionOccurred(env);
        if(exc!=NULL)
        {
            (*env)->ExceptionClear(env);
            rc = -1;
            break;
        }
        exfat_put_node(ef, node);
    }
    exfat_closedir(ef, &it);
    exfat_put_node(ef, parent);
    return rc;
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_makeDir
        (JNIEnv *env, jobject instance, jstring pathString)
{
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    const char *path = (*env)->GetStringUTFChars(env, pathString, 0);
    exfat_debug("[%s] %s", __func__, path);
    int rc = exfat_mkdir(ef, path);
    (*env)->ReleaseStringUTFChars(env, pathString, path);
    return rc;
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_makeFile
        (JNIEnv *env, jobject instance, jstring pathString)
{
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    const char *path = (*env)->GetStringUTFChars(env, pathString, 0);
    exfat_debug("[%s] %s", __func__, path);
    int rc = exfat_mknod(ef, path);
    (*env)->ReleaseStringUTFChars(env, pathString, path);
    return rc;
}

JNIEXPORT jlong JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_getFreeSpace
        (JNIEnv *env, jobject instance)
{
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    exfat_debug("[%s]", __func__);
    return (jlong)exfat_count_free_clusters(ef)*CLUSTER_SIZE(*ef->sb);
}

static uint32_t find_last_used_cluster(const struct exfat* ef)
{
    uint32_t i;

    for (i = ef->cmap.size - 1; i >= 0; i--)
        if (BMAP_GET(ef->cmap.chunk, i))
            break;
    return i;
}

JNIEXPORT jlong JNICALL
Java_com_sovworks_eds_fs_exfat_ExFat_getFreeSpaceStartOffset(JNIEnv *env, jobject instance)
{
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    exfat_debug("[%s]", __func__);
    uint32_t c = find_last_used_cluster(ef);
    if(c == -1)
        c = EXFAT_FIRST_DATA_CLUSTER;
    else
        c = c + EXFAT_FIRST_DATA_CLUSTER;
    if(CLUSTER_INVALID(*ef->sb, c))
        return -1;
    return exfat_c2o(ef, c);
}

JNIEXPORT jint JNICALL
Java_com_sovworks_eds_fs_exfat_ExFat_randFreeSpace(JNIEnv *env, jobject instance)
{
    exfat_debug("[%s]", __func__);
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    size_t cluster_size = (size_t) CLUSTER_SIZE(*ef->sb);
    uint8_t *buf = malloc(cluster_size);
    if(!buf)
        return -1;

    srand((unsigned int) time(NULL));
    for (uint32_t  i = 0; i < ef->cmap.size; i++)
        if (BMAP_GET(ef->cmap.chunk, i) == 0)
        {
            for(int j=0;j<cluster_size;j++)
                buf[i] = (uint8_t) (rand() % 256);

            uint32_t cluster = i + EXFAT_FIRST_DATA_CLUSTER;
            if (exfat_pwrite(ef->dev, buf, cluster_size,
                             exfat_c2o(ef, cluster)) < 0)
            {
                exfat_error("failed to write cluster %#x", cluster);
                free(buf);
                return -EIO;
            }
        }
    free(buf);
    return 0;
}


JNIEXPORT jlong JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_getTotalSpace
        (JNIEnv *env, jobject instance)
{
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    exfat_debug("[%s]", __func__);
    return ((jlong)le64_to_cpu(ef->sb->sector_count) >> ef->sb->spc_bits)*CLUSTER_SIZE(*ef->sb);
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_rename
        (JNIEnv *env, jobject instance, jstring oldPathString, jstring newPathString)
{
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    const char *old_path = (*env)->GetStringUTFChars(env, oldPathString, 0);
    const char *new_path = (*env)->GetStringUTFChars(env, newPathString, 0);
    exfat_debug("[%s] %s => %s", __func__, old_path, new_path);
    int rc = exfat_rename(ef, old_path, new_path);
    (*env)->ReleaseStringUTFChars(env, oldPathString, old_path);
    (*env)->ReleaseStringUTFChars(env, newPathString, new_path);
    return rc;
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_delete
        (JNIEnv *env, jobject instance, jstring pathString)
{
    struct exfat_node* node;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    const char *path = (*env)->GetStringUTFChars(env, pathString, 0);
    exfat_debug("[%s] %s", __func__, path);
    int rc = exfat_lookup(ef, &node, path);
    (*env)->ReleaseStringUTFChars(env, pathString, path);
    if (rc != 0)
        return rc;

    rc = exfat_unlink(ef, node);
    exfat_put_node(ef, node);
    if (rc != 0)
        return rc;
    return exfat_cleanup_node(ef, node);
}

JNIEXPORT jint JNICALL
Java_com_sovworks_eds_fs_exfat_ExFat_rmdir(JNIEnv *env, jobject instance, jstring pathString)
{
    struct exfat_node* node;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    const char *path = (*env)->GetStringUTFChars(env, pathString, 0);
    exfat_debug("[%s] %s", __func__, path);
    int rc = exfat_lookup(ef, &node, path);
    (*env)->ReleaseStringUTFChars(env, pathString, path);
    if (rc != 0)
        return rc;

    rc = exfat_rmdir(ef, node);
    exfat_put_node(ef, node);
    if (rc != 0)
        return rc;
    return exfat_cleanup_node(ef, node);
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_truncate
        (JNIEnv *env, jobject instance, jlong handle, jlong size)
{
    struct exfat_node* node = (struct exfat_node *) handle;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    exfat_debug("[%s] %"PRId64, __func__, size);

    return exfat_truncate(ef, node, (uint64_t) size, true);
}

JNIEXPORT jint JNICALL
Java_com_sovworks_eds_fs_exfat_ExFat_updateTime(JNIEnv *env, jobject instance, jstring pathString, jlong time)
{
    struct timespec tv[2];
    struct exfat_node* node;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    const char *path = (*env)->GetStringUTFChars(env, pathString, 0);
    exfat_debug("[%s] %s %"PRId64, __func__, path, time);
    int rc = exfat_lookup(ef, &node, path);
    (*env)->ReleaseStringUTFChars(env, pathString, path);
    if (rc != 0)
        return rc;
    tv[0].tv_sec = (time_t) (time / 1000);
    tv[0].tv_nsec = (long) ((time % 1000) * 1000000L);
    tv[1].tv_sec = tv[0].tv_sec;
    tv[1].tv_nsec = tv[0].tv_nsec;
    exfat_utimes(node, tv);
    rc = exfat_flush_node(ef, node);
    exfat_put_node(ef, node);
    return rc;
}

JNIEXPORT jlong JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_openFile
        (JNIEnv *env, jobject instance, jstring pathString)
{
    struct exfat_node* node;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return 0;
    const char *path = (*env)->GetStringUTFChars(env, pathString, 0);
    exfat_debug("[%s] %s", __func__, path);
    int rc = exfat_lookup(ef, &node, path);
    (*env)->ReleaseStringUTFChars(env, pathString, path);
    if (rc != 0)
        return 0;
    return (jlong) node;
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_closeFile
        (JNIEnv *env, jobject instance, jlong handle)
{
    struct exfat_node* node = (struct exfat_node *) handle;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    exfat_debug("[%s] ", __func__);

    exfat_flush_node(ef, node);
    exfat_put_node(ef, node);
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_getSize
        (JNIEnv *env, jobject instance, jlong handle)
{
    struct exfat_node* node = (struct exfat_node *) handle;
    exfat_debug("[%s] ", __func__);
    return node->size;
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_read
        (JNIEnv *env, jobject instance, jlong handle, jbyteArray buf, jint bufOffset, jint count, jlong position)
{
    exfat_debug("[%s] (%zu bytes)", __func__, count);
    struct exfat_node* node = (struct exfat_node *) handle;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    char *tmp_buf = malloc((size_t) count);
    if(tmp_buf == NULL)
        return -1;
    int res = exfat_generic_pread(ef, node, tmp_buf, (size_t) count, position);
    if(res > 0)
    {
        (*env)->SetByteArrayRegion(env, buf, bufOffset, res, (const jbyte *) tmp_buf);
        jthrowable exc = (*env)->ExceptionOccurred(env);
        if(exc!=NULL)
        {
            (*env)->ExceptionClear(env);
            res = -1;
        }
    }
    free(tmp_buf);
    return res;
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_write
        (JNIEnv *env, jobject instance, jlong handle, jbyteArray buf, jint bufOffset, jint count, jlong position)
{
    exfat_debug("[%s] (%zu bytes)", __func__, count);
    struct exfat_node* node = (struct exfat_node *) handle;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    char *tmp_buf = malloc((size_t) count);
    if(tmp_buf == NULL)
        return -1;
    (*env)->GetByteArrayRegion(env, buf, bufOffset, count, (jbyte *) tmp_buf);
    jthrowable exc = (*env)->ExceptionOccurred(env);
    if(exc!=NULL)
    {
        (*env)->ExceptionClear(env);
        free(tmp_buf);
        return -1;
    }
    int res = exfat_generic_pwrite(ef, node, tmp_buf, (size_t) count, position);
    free(tmp_buf);
    return res;
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_flush
        (JNIEnv *env, jobject instance, jlong handle)
{
    struct exfat_node* node = (struct exfat_node *) handle;
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    exfat_debug("[%s]", __func__);
    return exfat_flush_node(ef, node);
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_closeFS
        (JNIEnv *env, jobject instance)
{
    struct exfat *ef = get_exfat(env, instance);
    if(ef == NULL)
        return -1;
    exfat_debug("[%s]", __func__);
    exfat_unmount(ef);
    return 0;
}

JNIEXPORT jint JNICALL Java_com_sovworks_eds_fs_exfat_ExFat_makeFS(JNIEnv *env, jclass type, jobject raio, jstring labelString,
        jint volumeSerial, jlong firstSector,
        jint sectorsPerCluster)
{

    exfat_tzset();
    struct exfat_dev *dev = exfat_open_raio(env, raio, EXFAT_MODE_RW);
    if (dev == NULL)
        return -1;

    const char *label = labelString ? (*env)->GetStringUTFChars(env, labelString, 0) : NULL;
    int res = format_exfat(dev, label, (uint32_t) volumeSerial, (uint64_t) firstSector, sectorsPerCluster);
    if(labelString)
        (*env)->ReleaseStringUTFChars(env, labelString, label);
    exfat_close(dev);
    return res;
}

JNIEXPORT jint JNICALL
Java_com_sovworks_eds_fs_exfat_ExFat_getVersion(JNIEnv *env, jclass type)
{
    return EDSEXFAT_VERSION;
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *jvm, void *reserved)
{
    int res = JNI_VERSION_1_2;
    JNIEnv *env = init_jni(jvm);
    if(!env)
    {
        LOGI("init_jni failed");
        return JNI_ERR;
    }
    if(init_raio(env) == JNI_ERR)
    {
        LOGI("init_raio failed");
        clear_jni();
        res = JNI_ERR;
        goto detach;
    }

    if(cache_classes(env)==JNI_ERR)
    {
        LOGI("cache_classes failed");
        clear_raio(env);
        clear_jni();
        res = JNI_ERR;
        goto detach;
    }

    if(cache_methods(env)==JNI_ERR)
    {
        LOGI("cache_methods failed");
        clean_classes_cache(env);
        clear_raio(env);
        clear_jni();
        res = JNI_ERR;
        goto detach;
    }

    detach:
    return res;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM  *jvm, void *reserved)
{
    JNIEnv *env = get_env();
    clean_classes_cache(env);
    clear_raio(env);
    clear_jni();
}