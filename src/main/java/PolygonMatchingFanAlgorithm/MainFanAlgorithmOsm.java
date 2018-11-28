package PolygonMatchingFanAlgorithm;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.io.ParseException;

import PolygonDependencies.GeoPolygon;
import PolygonDependencies.InputTypes;
import PolygonDependencies.PolygonClassification;
import PolygonDependencies.PolygonPair;
import genericEntity.datasource.DataSource;
import genericEntity.exec.AbstractExec;
import genericEntity.util.data.GenericObject;
import genericEntity.util.data.storage.StorageManager;

public class MainFanAlgorithmOsm {
	
	private final static double thresholdPolygon = 0.3;//Fan uses a threshold of 30 percent (paper: Quality assessment for building footprints data on OpenStreetMap)

	public static void main(String[] args) throws ParseException {
		DataSource dataSourcePref = null;
		DataSource dataSourceOSM = null;
		
		String dataSource1 = args[0];
		String dataSource2 = args[1];
		String output = args[2];
		String sourceType = args[3];
		
		if (sourceType.equals("CSV")) {
			dataSourcePref = AbstractExec.getDataCSV(dataSource1, ';');
			dataSourceOSM = AbstractExec.getDataCSV(dataSource2, ';');
		} else { //is postgis
			dataSourcePref = AbstractExec.getDataPostGres(dataSource1);
			dataSourceOSM = AbstractExec.getDataPostGres(dataSource2);
		}

        StorageManager storagePref = new StorageManager();
        StorageManager storageOSM = new StorageManager();
		
        storagePref.enableInMemoryProcessing();
        storageOSM.enableInMemoryProcessing();

		// adds the "data" to the algorithm
        storagePref.addDataSource(dataSourcePref);
        storageOSM.addDataSource(dataSourceOSM);

		if(!storagePref.isDataExtracted()) {
			storagePref.extractData();
		}
		if(!storageOSM.isDataExtracted()) {
			storageOSM.extractData();
		}
		
		
		List<GeoPolygon> geoentitiesPref = new ArrayList<GeoPolygon>();
		List<GeoPolygon> geoentitiesOSM = new ArrayList<GeoPolygon>();
		
		// the algorithm returns each generated pair step-by-step
		int indexOfPref = 0;
		for (GenericObject genericObj : storagePref.getExtractedData()) {
			String nome = "";
			Integer id;
			if (!genericObj.getData().get("name").toString().equals("null")) {//for curitiba use atribute "nome" for new york "signname"
				nome = genericObj.getData().get("name").toString();
				id = Integer.parseInt(genericObj.getData().get("id").toString());//for curitiba use atribute "gid" for new york "id"
				geoentitiesPref.add(new GeoPolygon(genericObj.getData().get("geometry").toString(), nome, InputTypes.GOV_POLYGON, indexOfPref, id));
				indexOfPref++;
			}
			
		}
		
		
		int indexOfOSM = 0;
		for (GenericObject genericObj : storageOSM.getExtractedData()) {
			String nome = "";
			Integer id;
			if (!genericObj.getData().get("name").toString().equals("null")) {
				nome = genericObj.getData().get("name").toString();
				id = Integer.parseInt(genericObj.getData().get("id").toString());
				geoentitiesOSM.add(new GeoPolygon(genericObj.getData().get("geometry").toString(), nome, InputTypes.OSM_POLYGON, indexOfOSM, id));
				indexOfOSM++;
			}
			
		}
		
		
		List<PolygonPair> pairs = new ArrayList<PolygonPair>();
		for (GeoPolygon geoOSM : geoentitiesOSM) {
			for (GeoPolygon geoPref : geoentitiesPref) {
				//calculate the polygon similarity
				double polygonSimilarity = geoOSM.getPolygonSimilarity(geoPref);
				
				//classification of pairs
				PolygonPair pair;
				if (polygonSimilarity > thresholdPolygon) {
					pair = new PolygonPair(geoOSM, geoPref, 0.0, polygonSimilarity, PolygonClassification.MATCH);
					pairs.add(pair);
				}
				
			}
		}
		
		writeOnFile(output, pairs);

	}

	private static void writeOnFile(String path, List<PolygonPair> pairs) {
		BufferedWriter bw = null;
		FileWriter fw = null;

		try {
			fw = new FileWriter(path);
			bw = new BufferedWriter(fw);
			
			for (PolygonPair polygonPair : pairs) {
				bw.write(polygonPair.toStringCSV() + "\n");
			}
			

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}

		}
		
	}

}
