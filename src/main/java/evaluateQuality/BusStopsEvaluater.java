package evaluateQuality;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BusStopsEvaluater {

	public static void main(String[] args) {
		String file1 = "outputs/outputBusStops20/part-00000";
		String file2 = "outputs/outputBusStopsContext2/part-00000";
		try (BufferedReader br1 = new BufferedReader(new FileReader(file1))) {
			BufferedReader br2 = new BufferedReader(new FileReader(file2));
			
			HashMap<Integer, List<Integer>> map1 = new HashMap<Integer, List<Integer>>();

			String sCurrentLine;
			while ((sCurrentLine = br1.readLine()) != null) {
				Integer ent1 = Integer.parseInt(sCurrentLine.split(";")[0].split("\\(")[1].split("\\)")[0]);
				Integer ent2 = Integer.parseInt(sCurrentLine.split(";")[1].split("\\(")[1].split("\\)")[0]);
				
				if (map1.containsKey(ent1)) {
					map1.get(ent1).add(ent2);
				} else {
					List<Integer> list = new ArrayList<Integer>();
					list.add(ent2);
					map1.put(ent1, list);
				}
			}
			
			
			HashMap<Integer, List<Integer>> map2 = new HashMap<Integer, List<Integer>>();
			String sCurrentLine2;
			while ((sCurrentLine2 = br2.readLine()) != null) {
				Integer ent1 = Integer.parseInt(sCurrentLine2.split(";")[0].split("\\(")[1].split("\\)")[0]);
				Integer ent2 = Integer.parseInt(sCurrentLine2.split(";")[1].split("\\(")[1].split("\\)")[0]);
				
				if (map2.containsKey(ent1)) {
					map2.get(ent1).add(ent2);
				} else {
					List<Integer> list = new ArrayList<Integer>();
					list.add(ent2);
					map2.put(ent1, list);
				}
			}
			
			int naoEncontrados1 = 0;
			int duplicates1 = 0;
			for (Integer key : map1.keySet()) {
				if (!map2.containsKey(key)){
					System.out.println(key);
					naoEncontrados1++;
				}
				duplicates1 += (map1.get(key).size() - 1);
			}
			
			System.out.println("Nao encontrados S1 para S2: " + naoEncontrados1);
			System.out.println("Duplicatas em S1: " + duplicates1);
			System.out.println("Total Correnspondecias: " + (duplicates1+map1.size()));
			System.out.println();
			
			int naoEncontrados2 = 0;
			int duplicates2 = 0;
			for (Integer key : map2.keySet()) {
				if (!map1.containsKey(key)){
					naoEncontrados2++;
					System.out.println(key);
				}
				duplicates2 += (map2.get(key).size() - 1);
			}
			
			System.out.println("Nao encontrados S12 para S1: " + naoEncontrados2);
			System.out.println("Duplicatas em S2: " + duplicates2);
			System.out.println("Total Correnspondecias: " + (duplicates2+map2.size()));

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
