package ocrap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class FileSplitter {
	
	public static void main(String[] args) throws Exception {
		
		Integer fileNr = new Integer(-1);
		Integer countLines = new Integer(0);
		Integer reviewsInCurrentFile = new Integer(0);
		
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
		PrintWriter out = new PrintWriter("data/splitted/foods" + fileNr + ".txt");
		
		String line;
		while ((line = in.readLine()) != null) {
			
			String[] elements = line.split(" ", 2);
			
			// count how many reviews are already in the current file
			if (elements[0].equals("product/productId:")) {
				reviewsInCurrentFile++;
			}
			
			// creates a new file after writing 127 reviews in a file
			if (elements[0].equals("product/productId:") && reviewsInCurrentFile % 127 == 0) {
				reviewsInCurrentFile = 0;
				fileNr++;
				out.close();
				out = new PrintWriter("data/splitted/foods" + fileNr + ".txt");
			}
			
			out.println(line);
			countLines++;
		}
	}
}
