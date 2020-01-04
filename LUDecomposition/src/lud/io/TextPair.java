package lud.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {
	
	private String t1;
	private String t2;
	
	public String getFirst() {
		return this.t1;
	}
	
	public String getSecond() {
		return this.t2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.t1 = in.readUTF();
		this.t2 = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.t1);
		out.writeUTF(this.t2);
	}
	
	public TextPair() {
        this.t1 = new String();
        this.t2 = new String();
    }
	
	public TextPair(String t1, String t2) {
        this.t1 = new String(t1);
        this.t2 = new String(t2);
    }
	
	public int compareTo(TextPair tp) {
        int sortKey = this.t1.compareTo(tp.getFirst());
        if (sortKey == 0) {
        	sortKey = this.t2.compareTo(tp.getSecond());
        }
        return sortKey;
    }
	
	public String toString () {
		String s = "";
		if (this.t2.compareTo("") == 0) {
			s += this.t1;
		}
		else {
			s += this.t1 + "," + this.t2;
		}
		return s;
	}
	
}
