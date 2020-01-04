package lud.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LongAndTextWritable implements Writable {
	
	private LongWritable rowKey;
    private Text rowValue;
    
    public LongAndTextWritable() {
        this.rowKey = new LongWritable(0);
        this.rowValue = new Text("");
    }
	
	public LongAndTextWritable(LongWritable k, Text v) {
        this.rowKey = k;
        this.rowValue = v;
    }
	
	public LongWritable getKey() {
		return rowKey;
	}
	
	public Text getValue() {
		return rowValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		rowKey.readFields(in);
		rowValue.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		rowKey.write(out);
		rowValue.write(out);
	}
	
	@Override
    public String toString() {
        return rowKey.toString() + "\t" + rowValue.toString();
    }

}
