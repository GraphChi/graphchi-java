package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.cmu.graphchi.io.MatrixMarketDataReader;

public class URLFileInputDataReader extends FileInputDataReader {
    
    public URLFileInputDataReader(DataSetDescription datasetDesc) {
        super(datasetDesc);
    }
    
    public URLFileInputDataReader(String dataSetDescFile) {
        super(dataSetDescFile);
    }

    @Override
    public boolean initRatingData() throws IOException, InconsistentDataException {
        URL ratingsUrl = new URL(this.getDataSetDescription().getRatingsUrl());
        BufferedInputStream inStream = new BufferedInputStream((ratingsUrl.openStream()));
        
        this.ratingsReader = new MatrixMarketDataReader(inStream);
        this.ratingsReader.init();
        this.metadata.setNumUsers(ratingsReader.numLeft);
        this.metadata.setNumItems(ratingsReader.numRight);
        this.metadata.setNumRatings(ratingsReader.numRatings);
        
        return true;
    }
    
    @Override
    public boolean initUserData()  throws IOException, InconsistentDataException {
        URL userDataUrl = new URL(this.getDataSetDescription().getUserFeaturesUrl());
        this.userBr = new BufferedReader(new InputStreamReader(userDataUrl.openStream()));
        
        return true;
    }
    
    @Override
    public boolean initItemData()  throws IOException, InconsistentDataException {
        URL itemDataUrl = new URL(this.getDataSetDescription().getItemFeaturesUrl());
        this.itemBr = new BufferedReader(new InputStreamReader(itemDataUrl.openStream()));
        
        return true;
    }

}
