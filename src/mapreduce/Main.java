/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import mapreduce.model.DocumentFrequency;
import mapreduce.service.FrequencyCalculator;

/**
 *
 * @author sxg282
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            File file = new File("pg2600.txt");
            FrequencyCalculator fc = new FrequencyCalculator();
            long startTime = System.currentTimeMillis();
            DocumentFrequency df = fc.calculateTotalDocumentFrequency(file, 4);
            long endTime = System.currentTimeMillis();
            System.out.println(fc.formatResult(df, endTime - startTime));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
            //DocumentFrequency df = fc.calculateTotalDocumentFrequency(null, threads)
        
        //DocumentFrequency df = fc.calculateTotalDocumentFrequency(null, threads)


    }

}
