package ict.ada.common.model;

import java.io.UnsupportedEncodingException;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;


/**
 *RelationType 类维护了一个静态relationTypeMap,把所有请求过的relationtype存储起来,<br>
 *RelationType线程安全,
 *
 */
public final class RelationType {

	private  static ConcurrentLinkedHashMap<String, RelationType> relationTypeMap = new ConcurrentLinkedHashMap.Builder<String, RelationType>().maximumWeightedCapacity(Integer.MAX_VALUE).build();
     //ConcurrentLinkedHashMap来管理状态,达到线程安全目的
	private String stringForm;
    private byte[] bytesForm;
	private RelationType(String stringForm) {
		this.stringForm = stringForm;
		try {
			bytesForm=stringForm.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static boolean isExsits(String stringForm){
		return relationTypeMap.containsKey(stringForm);
	}
	/**
	 *如果relationTypeMap不存在该type,则创建该relationType,并放入relationTypeMap中,然后返回.
	 * @param type
	 * @return
	 */
	public static RelationType getType(String stringForm) {
		if(!relationTypeMap.containsKey(stringForm)){
			synchronized(relationTypeMap){
				if(!relationTypeMap.containsKey(stringForm))
				{
					RelationType relationType= new RelationType(stringForm);
					relationTypeMap.put(stringForm, relationType);
				}
			}
		}
			return relationTypeMap.get(stringForm); 
	}
	public byte[] getBytesForm(){
		return bytesForm;
	}

	public String getStringForm() {
		return stringForm;
	}
 
	 @Override
	public String toString() {
		return stringForm;
	}
}
