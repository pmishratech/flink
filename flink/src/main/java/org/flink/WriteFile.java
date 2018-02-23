package org.flink;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteFile {
	public static void main(String[] args) {
		try {
			File fac = new File("test.txt");
			if (!fac.exists()) {
				fac.createNewFile();
			}
			System.out.println("\n----------------------------------");
			System.out.println("The file has been created.");
			System.out.println("------------------------------------");
			FileWriter wr = new FileWriter(fac);
			for (int i = 1; i <= 100000; i++) {
				wr.write(i + System.getProperty("line.separator"));
			}
			wr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}