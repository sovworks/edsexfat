/*
	io.c (02.09.09)
	exFAT file system implementation library.

	Free exFAT implementation.
	Copyright (C) 2010-2017  Andrew Nayenko

	Modified by sovworks.

	This program is free software; you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 2 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License along
	with this program; if not, write to the Free Software Foundation, Inc.,
	51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

#include "exfat.h"
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/mount.h>

#include "../raio.h"

int exfat_fsync(struct exfat_dev* dev)
{
	int rc = 0;

	if (raio_flush(dev->env, dev->raio) != 0)
	{
		exfat_error("fsync failed: %s", strerror(errno));
		rc = -EIO;
	}
	return rc;
}

enum exfat_mode exfat_get_mode(const struct exfat_dev* dev)
{
	return dev->mode;
}

off64_t exfat_get_size(const struct exfat_dev* dev)
{
	return dev->size;
}

off64_t exfat_seek(struct exfat_dev* dev, off64_t offset, int whence)
{
	int rc;
	off64_t cur;
	switch (whence)
	{
		case SEEK_SET:
			rc = raio_seek(dev->env, dev->raio, offset);
			if(rc)
				return (off64_t) -1;
			break;
		case SEEK_CUR:
			cur = raio_get_pos(dev->env, dev->raio);
			if( cur == (off64_t)-1)
				return cur;
			rc = raio_seek(dev->env, dev->raio, offset + cur);
			if(rc)
				return (off64_t) -1;
			break;
		case SEEK_END:
			cur = raio_get_size(dev->env, dev->raio);
			if( cur == (off64_t)-1)
				return cur;
			rc = raio_seek(dev->env, dev->raio, offset + cur);
			if(rc)
				return (off64_t) -1;
			break;
	}
	return raio_get_pos(dev->env, dev->raio);
}

ssize_t exfat_read(struct exfat_dev* dev, void* buffer, size_t size)
{
	return raio_read(dev->env, dev->raio, buffer, size);
}

ssize_t exfat_write(struct exfat_dev* dev, const void* buffer, size_t size)
{
	return raio_write(dev->env, dev->raio, buffer, size);
}

ssize_t exfat_pread(struct exfat_dev* dev, void* buffer, size_t size,
		off64_t offset)
{
	return raio_pread(dev->env, dev->raio, buffer, size, offset);
}

ssize_t exfat_pwrite(struct exfat_dev* dev, const void* buffer, size_t size,
		off64_t offset)
{
	return raio_pwrite(dev->env, dev->raio, buffer, size, offset);
}

ssize_t exfat_generic_pread(const struct exfat* ef, struct exfat_node* node,
		void* buffer, size_t size, off64_t offset)
{
	cluster_t cluster;
	char* bufp = buffer;
	off64_t lsize, loffset, remainder;

	if (offset >= node->size)
		return 0;
	if (size == 0)
		return 0;

	cluster = exfat_advance_cluster(ef, node, offset / CLUSTER_SIZE(*ef->sb));
	if (CLUSTER_INVALID(*ef->sb, cluster))
	{
		exfat_error("invalid cluster 0x%x while reading", cluster);
		return -EIO;
	}

	loffset = offset % CLUSTER_SIZE(*ef->sb);
	remainder = MIN(size, node->size - offset);
	while (remainder > 0)
	{
		if (CLUSTER_INVALID(*ef->sb, cluster))
		{
			exfat_error("invalid cluster 0x%x while reading", cluster);
			return -EIO;
		}
		lsize = MIN(CLUSTER_SIZE(*ef->sb) - loffset, remainder);
		if (exfat_pread(ef->dev, bufp, lsize,
					exfat_c2o(ef, cluster) + loffset) < 0)
		{
			exfat_error("failed to read cluster %#x", cluster);
			return -EIO;
		}
		bufp += lsize;
		loffset = 0;
		remainder -= lsize;
		cluster = exfat_next_cluster(ef, node, cluster);
	}
	if (!(node->attrib & EXFAT_ATTRIB_DIR) && !ef->ro && !ef->noatime)
		exfat_update_atime(node);
	return MIN(size, node->size - offset) - remainder;
}

ssize_t exfat_generic_pwrite(struct exfat* ef, struct exfat_node* node,
		const void* buffer, size_t size, off64_t offset)
{
	int rc;
	cluster_t cluster;
	const char* bufp = buffer;
	off64_t lsize, loffset, remainder;

 	if (offset > node->size)
	{
		rc = exfat_truncate(ef, node, offset, true);
		if (rc != 0)
			return rc;
	}
  	if (offset + size > node->size)
	{
		rc = exfat_truncate(ef, node, offset + size, false);
		if (rc != 0)
			return rc;
	}
	if (size == 0)
		return 0;

	cluster = exfat_advance_cluster(ef, node, offset / CLUSTER_SIZE(*ef->sb));
	if (CLUSTER_INVALID(*ef->sb, cluster))
	{
		exfat_error("invalid cluster 0x%x while writing", cluster);
		return -EIO;
	}

	loffset = offset % CLUSTER_SIZE(*ef->sb);
	remainder = size;
	while (remainder > 0)
	{
		if (CLUSTER_INVALID(*ef->sb, cluster))
		{
			exfat_error("invalid cluster 0x%x while writing", cluster);
			return -EIO;
		}
		lsize = MIN(CLUSTER_SIZE(*ef->sb) - loffset, remainder);
		if (exfat_pwrite(ef->dev, bufp, lsize,
				exfat_c2o(ef, cluster) + loffset) < 0)
		{
			exfat_error("failed to write cluster %#x", cluster);
			return -EIO;
		}
		bufp += lsize;
		loffset = 0;
		remainder -= lsize;
		cluster = exfat_next_cluster(ef, node, cluster);
	}
	if (!(node->attrib & EXFAT_ATTRIB_DIR))
		/* directory's mtime should be updated by the caller only when it
		   creates or removes something in this directory */
		exfat_update_mtime(node);
	return size - remainder;
}
