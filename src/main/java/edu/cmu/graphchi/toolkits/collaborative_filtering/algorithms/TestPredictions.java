package edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms;

import java.io.IOException;
import java.util.List;

import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.InputDataReader;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.InputDataReaderFactory;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParameters;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ModelParametersPrediction;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.SerializationUtils;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.VertexDataCache;
import gov.sandia.cognition.math.matrix.mtj.SparseVector;

public class TestPredictions {
    private VertexDataCache vertexDataCache;
    private InputDataReader dataReader;
    
    private List<ModelParametersPrediction> modelParams;
    private DataSetDescription dataDesc;
    
    public TestPredictions(List<ModelParametersPrediction> modelParams, DataSetDescription dataDesc) {
        this.modelParams = modelParams;
        this.dataDesc = dataDesc;
        
        this.vertexDataCache = VertexDataCache.createVertexDataCache(this.dataDesc);
        this.dataReader = InputDataReaderFactory.createInputDataReader(this.dataDesc);
    }
    
    public void predictOnTest() throws Exception{
        this.dataReader.initRatingData();
        
        while( this.dataReader.nextRatingData()){
            
            int userId = this.dataReader.getCurrRatingFrom();
            int itemId = this.dataReader.getCurrRatingTo();
            double rating = this.dataReader.getCurrRating();
            
            SparseVector userFeatures = null;
            if(this.vertexDataCache != null) {
                userFeatures = this.vertexDataCache.getFeatures(userId);
            }
            
            SparseVector itemFeatures = null;
            if(this.vertexDataCache != null) {
                itemFeatures = this.vertexDataCache.getFeatures(itemId); 
            }
            //TODO: Figure out how to support edge values.
            SparseVector edgeFeatures = null;
            
            for(int i = 0 ; i < modelParams.size(); i++){
                ModelParameters params = modelParams.get(i).getParams();            
                double predictedValue = params.predict(userId, itemId, userFeatures, itemFeatures, 
                        edgeFeatures, this.dataDesc);
                modelParams.get(i).addErrorInstance(rating, predictedValue);
            }
        }
        
        for(ModelParametersPrediction param : modelParams) {
            System.out.println("Model " + param.getParams().getId() + " " + param.getErrorTypeString() 
                    + " Error: " + param.getFinalError());
        }
        
    }
    
    public static void main(String args[]){
        ProblemSetup problemSetup = new ProblemSetup(args);     
        DataSetDescription dataDesc = new DataSetDescription();
        dataDesc.loadFromJsonFile(problemSetup.dataMetadataFile);
        List<ModelParametersPrediction> params = SerializationUtils.deserializeJSON(problemSetup.paramFile);
        
        TestPredictions testPredictions = new TestPredictions(params, dataDesc);
        
        try {
            testPredictions.predictOnTest();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

