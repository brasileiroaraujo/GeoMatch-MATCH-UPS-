package PointMatching20;

import java.io.UnsupportedEncodingException;

public class teste {

	public static void main(String[] args) throws UnsupportedEncodingException {
		String st = "Novo mobili�rio";
		String text = new String(st.getBytes("UTF-8"),"ISO-8859-1");
		System.out.println(text);

	}

}
