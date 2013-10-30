package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.VertexDataCache;

public class RecommenderFactory {
	public static final String MODEL_NAME_KEY = "algorithm";
	public static final String MODEL_ID_KEY = "id";
	public static final String DEFAULT_MODEL_ID = "<DEFAULT>";
	
	private static final DateFormat DF = new SimpleDateFormat("MM/dd/yyyy_HH:mm:ss");
	
	public static final String REC_ALS = "ALS";
	public static final String REC_SVDPP = "SVDPP";
	public static final String REC_PMF = "PMF";
	public static final String REC_LIBFM_SGD = "LibFM_SGD";
	public static final String REC_BIAS_SGD = "BIAS_SGD";
	
	
	public RecommenderFactory() {
		// TODO Auto-generated constructor stub
	}
	
	public static List<RecommenderAlgorithm> buildRecommenders(DataSetDescription dataDesc, 
		String modelDescJsonFile, VertexDataCache vertexDataCache) {
		
		List<Map<String,  String>> modelDescMaps = getRecommederParamsFromJson(modelDescJsonFile);
		
		List<RecommenderAlgorithm> recommenders = new ArrayList<RecommenderAlgorithm>();
		
		for(Map<String, String> modelDescMap : modelDescMaps) {
			String id = parseModelId(modelDescMap.get(MODEL_ID_KEY), modelDescMap.get(MODEL_NAME_KEY));
			
			if(modelDescMap.get(MODEL_NAME_KEY).equals(REC_ALS)) {
				//Build an ALS recommender engine
				ALSParams params = new ALSParams(id, modelDescMap);
				recommenders.add(new ALS(dataDesc, params));
			} else if(modelDescMap.get(MODEL_NAME_KEY).equals(REC_SVDPP)) {
				//Build a SVDPP recommender engine
				SVDPPParams params = new SVDPPParams(id, modelDescMap);
				recommenders.add(new SVDPP(dataDesc, params));
			} else if(modelDescMap.get(MODEL_NAME_KEY).equals(REC_PMF)) {
				//Build PMF parameters.
				/*PMFParameters params = new PMFParameters(modelDescMap.get(MODEL_ID_KEY), modelDescMap.get(MODEL_PARAM_JSON_KEY));
				recommenders.add(new PMF(dataDesc, params));*/
			} else if(modelDescMap.get(MODEL_NAME_KEY).equals(REC_LIBFM_SGD)) {
				//Build a LibFM_SGD recommender. 
				LibFM_SGDParams params = new LibFM_SGDParams(id, modelDescMap);
				LibFM_SGD rec = new LibFM_SGD(dataDesc, params);
				rec.vertexDataCache = vertexDataCache;
				recommenders.add(rec);				
			} else if(modelDescMap.get(MODEL_NAME_KEY).equals(REC_BIAS_SGD)){
				BiasSgdParams params = new BiasSgdParams(id, modelDescMap);
				recommenders.add(new BiasSgd(dataDesc, params));
			} else {			
				//No model by the given name found.
			}
		}
		
		return recommenders;
		
	}
	
	public static List<Map<String, String>> getRecommederParamsFromJson(String modelDescJsonFile) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			List<Map<String, String>> models = mapper.readValue(
					new File(modelDescJsonFile), TypeFactory.collectionType(List.class, Map.class));
			
			return models;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(2);
		}
		return null;
	}
	
	public static Map<String, String> parseModelDescJson(String modelDescJson) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			return (Map<String, String>) mapper.readValue(modelDescJson, Map.class);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(2);
		}
		return null;
	}
	
	public static String parseModelId(String modelId, String algorithm) {
		if(modelId == null || algorithm == null) {
			return null;
		} else if(modelId.length() == 0 || modelId.equals(DEFAULT_MODEL_ID)) {
			return algorithm + "_" + DF.format(new Date());
		} else {
			return modelId;
		}
		
		
	}

}
