package org.apache.flink.fs.osssimple;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSSFileStatus implements FileStatus {
    private Path path;
    private long length;
    private boolean isdir;
    private short block_replication;
    private long blocksize;
    private long modification_time;
    private long access_time;
    private FsPermission permission;
    private String owner;
    private String group;
    private Path symlink;

    public OSSFileStatus() {
        this(0L, false, 0, 0L, 0L, 0L, (FsPermission)null, (String)null, (String)null, (Path)null);
    }

    public OSSFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time, Path path) {
        this(length, isdir, block_replication, blocksize, modification_time, 0L, (FsPermission)null, (String)null, (String)null, path);
    }

    public OSSFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time, long access_time, FsPermission permission, String owner, String group, Path path) {
        this(length, isdir, block_replication, blocksize, modification_time, access_time, permission, owner, group, (Path)null, path);
    }

    public OSSFileStatus(long length, boolean isdir, int block_replication, long blocksize, long modification_time, long access_time, FsPermission permission, String owner, String group, Path symlink, Path path) {
        this.length = length;
        this.isdir = isdir;
        this.block_replication = (short)block_replication;
        this.blocksize = blocksize;
        this.modification_time = modification_time;
        this.access_time = access_time;
        if (permission != null) {
            this.permission = permission;
        } else if (isdir) {
            this.permission = FsPermission.getDirDefault();
        } else if (symlink != null) {
            this.permission = FsPermission.getDefault();
        } else {
            this.permission = FsPermission.getFileDefault();
        }

        this.owner = owner == null ? "" : owner;
        this.group = group == null ? "" : group;
        this.symlink = symlink;
        this.path = path;

        assert isdir && symlink == null || !isdir;

    }

    public OSSFileStatus(long length, boolean isdir, int blockReplication, long blocksize, long modTime, Path path, String user) {
        this(length, isdir, blockReplication, blocksize, modTime, path);
        this.setOwner(user);
        this.setGroup(user);
    }

    public OSSFileStatus(OSSFileStatus other) throws IOException {
        this(other.getLen(), other.isDirectory(), other.getReplication(), other.getBlockSize(), other.getModificationTime(), other.getAccessTime(), other.getPermission(), other.getOwner(), other.getGroup(), other.isSymlink() ? other.getSymlink() : null, other.getPath());
    }

    public long getLen() {
        return this.length;
    }

    public boolean isFile() {
        return !this.isdir && !this.isSymlink();
    }

    public boolean isDirectory() {
        return this.isdir;
    }

    /** @deprecated */
    @Deprecated
    public boolean isDir() {
        return this.isdir;
    }

    public boolean isSymlink() {
        return this.symlink != null;
    }

    public long getBlockSize() {
        return this.blocksize;
    }

    public short getReplication() {
        return this.block_replication;
    }

    public long getModificationTime() {
        return this.modification_time;
    }

    public long getAccessTime() {
        return this.access_time;
    }

    public FsPermission getPermission() {
        return this.permission;
    }

    public boolean isEncrypted() {
        return this.permission.getEncryptedBit();
    }

    public String getOwner() {
        return this.owner;
    }

    public String getGroup() {
        return this.group;
    }

    public Path getPath() {
        return this.path;
    }

    public void setPath(Path p) {
        this.path = p;
    }

    protected void setPermission(FsPermission permission) {
        this.permission = permission == null ? FsPermission.getFileDefault() : permission;
    }

    protected void setOwner(String owner) {
        this.owner = owner == null ? "" : owner;
    }

    protected void setGroup(String group) {
        this.group = group == null ? "" : group;
    }

    public Path getSymlink() throws IOException {
        if (!this.isSymlink()) {
            throw new IOException("Path " + this.path + " is not a symbolic link");
        } else {
            return this.symlink;
        }
    }

    public void setSymlink(Path p) {
        this.symlink = p;
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.getPath().toString(), 1048576);
        out.writeLong(this.getLen());
        out.writeBoolean(this.isDirectory());
        out.writeShort(this.getReplication());
        out.writeLong(this.getBlockSize());
        out.writeLong(this.getModificationTime());
        out.writeLong(this.getAccessTime());
        this.getPermission().write(out);
        Text.writeString(out, this.getOwner(), 1048576);
        Text.writeString(out, this.getGroup(), 1048576);
        out.writeBoolean(this.isSymlink());
        if (this.isSymlink()) {
            Text.writeString(out, this.getSymlink().toString(), 1048576);
        }

    }

    public void readFields(DataInput in) throws IOException {
        String strPath = Text.readString(in, 1048576);
        this.path = new Path(strPath);
        this.length = in.readLong();
        this.isdir = in.readBoolean();
        this.block_replication = in.readShort();
        this.blocksize = in.readLong();
        this.modification_time = in.readLong();
        this.access_time = in.readLong();
        this.permission.readFields(in);
        this.owner = Text.readString(in, 1048576);
        this.group = Text.readString(in, 1048576);
        if (in.readBoolean()) {
            this.symlink = new Path(Text.readString(in, 1048576));
        } else {
            this.symlink = null;
        }

    }

    public int compareTo(org.apache.hadoop.fs.FileStatus o) {
        return this.getPath().compareTo(o.getPath());
    }

    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (this == o) {
            return true;
        } else if (!(o instanceof org.apache.hadoop.fs.FileStatus)) {
            return false;
        } else {
            org.apache.hadoop.fs.FileStatus other = (org.apache.hadoop.fs.FileStatus)o;
            return this.getPath().equals(other.getPath());
        }
    }

    public int hashCode() {
        return this.getPath().hashCode();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName());
        sb.append("{");
        sb.append("path=" + this.path);
        sb.append("; isDirectory=" + this.isdir);
        if (!this.isDirectory()) {
            sb.append("; length=" + this.length);
            sb.append("; replication=" + this.block_replication);
            sb.append("; blocksize=" + this.blocksize);
        }

        sb.append("; modification_time=" + this.modification_time);
        sb.append("; access_time=" + this.access_time);
        sb.append("; owner=" + this.owner);
        sb.append("; group=" + this.group);
        sb.append("; permission=" + this.permission);
        sb.append("; isSymlink=" + this.isSymlink());
        if (this.isSymlink()) {
            sb.append("; symlink=" + this.symlink);
        }

        sb.append("}");
        return sb.toString();
    }
}
