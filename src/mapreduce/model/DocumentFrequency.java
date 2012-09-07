/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mapreduce.model;

import java.util.Map;

/**
 *
 * @author sxg282
 */
public class DocumentFrequency {

    private int numberOfLines;
    private Map<String, Integer> termDocumentFrequency;

    public DocumentFrequency(int numberOfLines, Map<String, Integer> termDocumentFrequency) {
        this.numberOfLines = numberOfLines;
        this.termDocumentFrequency = termDocumentFrequency;
    }



    public int getNumberOfLines() {
        return numberOfLines;
    }

    public void setNumberOfLines(int numberOfLines) {
        this.numberOfLines = numberOfLines;
    }

    public Map<String, Integer> getTermDocumentFrequency() {
        return termDocumentFrequency;
    }

    public void setTermDocumentFrequency(Map<String, Integer> termDocumentFrequency) {
        this.termDocumentFrequency = termDocumentFrequency;
    }



}
