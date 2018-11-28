package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
 
public class checkPolygonsUnidentified {
 
  public static void main(String[] args) {
    String nomeGabarito = "C:/Users/Brasileiro/Eclipse Bigsea/eclipse/workspace/geographicmatching/outputCSV/squares_pref_curitiba.csv";
    String nomeRespostas = "C:/Users/Brasileiro/Eclipse Bigsea/eclipse/workspace/geographicmatching/outputs/outputISCCSquaresCuritiba2/part-00000";
    List<String> allPolygons = new ArrayList<String>();
    
    try {
      FileReader arq = new FileReader(nomeGabarito);
      BufferedReader lerArq = new BufferedReader(arq);
 
      lerArq.readLine();//tirar cabecalho
      String linha = lerArq.readLine();
      while (linha != null) {
    	if (!linha.split(";")[1].equalsIgnoreCase("null")) {
    		allPolygons.add(linha.split(";")[1]);
		}
 
        linha = lerArq.readLine(); // lê da segunda até a última linha
      }
 
      arq.close();
    } catch (IOException e) {
        System.err.printf("Erro na abertura do arquivo: %s.\n",
          e.getMessage());
    }
    
    try {
        FileReader arq = new FileReader(nomeRespostas);
        BufferedReader lerArq = new BufferedReader(arq);
   
        lerArq.readLine();//tirar cabecalho
        String linha = lerArq.readLine();
        while (linha != null) {
        String key = linha.split(";")[1].split("\\(")[0].trim();
      	if (allPolygons.contains(key)) {
      		allPolygons.remove(key);
  		}
   
          linha = lerArq.readLine(); // lê da segunda até a última linha
        }
   
        arq.close();
      } catch (IOException e) {
          System.err.printf("Erro na abertura do arquivo: %s.\n",
            e.getMessage());
      }
    
    for (String string : allPolygons) {
		System.out.println(string);
	}
    System.out.println(allPolygons.size());
    
    
    
 
    System.out.println();
  }
}