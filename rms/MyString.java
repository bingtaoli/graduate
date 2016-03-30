package rms;

import java.util.Objects;

public class MyString {
	
	public static String join(CharSequence delimiter, CharSequence... elements) {
        Objects.requireNonNull(delimiter);
        Objects.requireNonNull(elements);
        // Number of elements not likely worth Arrays.stream overhead.
        MyStringJoiner joiner = new MyStringJoiner(delimiter);
        for (CharSequence cs: elements) {
            joiner.add(cs);
        }
        return joiner.toString();
    }
	
}
