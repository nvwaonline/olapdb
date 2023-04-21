package com.olapdb.core.tables;

import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.TimeEntity;
import org.apache.hadoop.hbase.client.Result;

public abstract class ArchiveableEntity extends TimeEntity {
    public ArchiveableEntity(byte[] row, boolean autoload) {
        super(row, autoload);
    }

    public ArchiveableEntity(byte[] row) {
        super(row);
    }

    public ArchiveableEntity(Result r) {
        super(r);
    }

    protected void setArchiveTime(long value) {
        this.setAttribute("archiveTime", Bytez.from(value));
    }
    public long getArchiveTime(){
        return this.getAttributeAsLong("archiveTime");
    }
}
