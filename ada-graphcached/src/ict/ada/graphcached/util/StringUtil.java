package ict.ada.graphcached.util;

public class StringUtil {
	/**
	 * Get substring between the start and end.
	 * @param contents string contents.
	 * @param start start tag.
	 * @param end end tag.
	 * @return substring between the start and end if successful or null
	 *         if it does not match any substring.
	 */
	public static String GetBetween(String contents, String start, String end) {
		int startPos = -1, endPos = -1;
		startPos = contents.indexOf(start);
		endPos = contents.indexOf(end, startPos + start.length());
		if (startPos != -1 && endPos != -1 && endPos > startPos) {
			return contents.substring(startPos + start.length(), endPos);
		} else return null;
	}
	
	/**
	 * Get substring between the start and end from pos.
	 * @param contents string contents.
	 * @param pos where to start searching.
	 * @param start start tag.
	 * @param end end tag.
	 * @param sbMatch substring between the start and end if successful
	 * @return last position of the string before which we have searched.
	 */
	public static int GetBetweenAndForward(String contents, int pos,
			String start, String end, StringBuilder sbMatch) {
		int startPos = -1, endPos = -1, newPos = -1;
		startPos = contents.indexOf(start, pos);
		endPos = contents.indexOf(end, startPos + start.length());
		if (startPos != -1 && endPos != -1 && endPos > startPos) {
			sbMatch.append(contents.substring(startPos + start.length(), endPos));
			newPos = endPos + end.length();
		} else return -1;
		return newPos;
	}
	
	/**
	 * Get substring from the start tag to the end of contents.
	 * @param contents string contents.
	 * @param start start tag
	 * @return substring between the start tag to end if successful or null
	 *         if it does not match any substring.
	 */
	public static String GetSubStringToEnd(String contents, String start) {
		int startPos = -1;
		startPos = contents.indexOf(start);
		if (startPos != -1) {
			return contents.substring(startPos + start.length());
		} else return null;
	}

}
