import java.util.*;
public class Splitter{
	public static void main(String args[]){
		Scanner sc = new Scanner(System.in);
		String inp = sc.nextLine();
		System.out.println(inp);

		if(inp.contains("\"")){
			StringBuilder sb = new StringBuilder();
			String[] inpList = inp.trim().split("\"");	
			inpList[1] = inpList[1].replace(","," ");
			for(String s: inpList){
				sb.append(s);
			}
			inp = sb.toString();
		}
		String[] inpListCSV = inp.trim().split(",");
		System.out.println(inpListCSV.length);
		for(int i=0;i<inpListCSV.length;i++){
			if(inpListCSV[i]==""){
				inpListCSV[i]=null;
			}
			System.out.println(inpListCSV[i]);
		}
	}
}