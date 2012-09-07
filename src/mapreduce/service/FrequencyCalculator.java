/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mapreduce.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import mapreduce.model.DocumentFrequency;

/**
 *
 * @author sxg282
 */
public class FrequencyCalculator {

    /*
     * @file - corpus
     * @threads - number of threads to use
     */
    public DocumentFrequency calculateTotalDocumentFrequency(File file, int threads) throws FileNotFoundException, IOException {

        long[] offsets = new long[threads];
        // first way I could think of to split a file of arbitrary length
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        for (int i = 1; i < threads; i++) {
            raf.seek(i * file.length() / threads);

            while (true) {
                int read = raf.read();
                if (read == '\n' || read == -1) {
                    break;
                }
            }

            offsets[i] = raf.getFilePointer();
        }
        raf.close();

        /**
         * This is where I have tried to model the map reduction paradigm
         *
         */
        ExecutorService service = Executors.newFixedThreadPool(threads);
        List<Future<DocumentFrequency>> list = new ArrayList<Future<DocumentFrequency>>();
        for (int i = 0; i < threads; i++) { // break into buckets
            long start = offsets[i];
            long end = i < threads - 1 ? offsets[i + 1] : file.length();

            // do the map and produce intermediary results
            Callable<DocumentFrequency> worker = new Worker(file, start, end);
            Future<DocumentFrequency> submit = service.submit(worker);
            list.add(submit);
        }
        service.shutdown();

        // now do the reduction
        int sum = 0;
        Map<String, Integer> result = new TreeMap<String, Integer>();
        for(Future<DocumentFrequency> future : list) {
            try {
                sum += future.get().getNumberOfLines();
                Map<String, Integer> map = future.get().getTermDocumentFrequency();
                for(Map.Entry<String, Integer> entry : map.entrySet()) {

                    if(result.containsKey(entry.getKey())) {
                        result.put(entry.getKey(), result.get(entry.getKey()) + entry.getValue());
                    } else {
                        result.put(entry.getKey(), entry.getValue());
                    }

                }
            } catch (InterruptedException ex) {
                // real system would have logger
                System.out.println(ex);
            } catch (ExecutionException ex) {
                System.out.println(ex);
            }
        }

        return new DocumentFrequency(sum, result);

    }

    public static class Worker implements Callable<DocumentFrequency> {

        private final File file;
        private final long start;
        private final long end;

        public Worker(File file, long start, long end) {
            this.file = file;
            this.start=start;
            this.end = end;
        }

        @Override
        public DocumentFrequency call() {
            int count = 0;
            Map<String, Integer> documentFrequency = new TreeMap<String, Integer>();
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                raf.seek(start);


                while (raf.getFilePointer() < end) {
                    String line = raf.readLine();
                    Set<String> uniqueWords = new HashSet<String>(Arrays.asList(line.split(" ")));

                    for(String s : uniqueWords) {

                        if(documentFrequency.containsKey(s)) {
                            documentFrequency.put(s, documentFrequency.get(s)+1);
                        } else {
                            documentFrequency.put(s, 1);
                        }
                    }
                    count++;
                }

                raf.close();
            } catch (IOException e) {
                System.out.println("some io exception occurred");
            }
            return new DocumentFrequency(count, documentFrequency);
        }


    }

    public String formatResult(DocumentFrequency df, long elapsedTime) {

        StringBuilder sb = new StringBuilder();
        sb.append("Doc Frequencies: \n");
        sb.append("------------------------\n");

        for(Map.Entry<String, Integer> entry : df.getTermDocumentFrequency().entrySet()) {
            sb.append("  ");
            sb.append(entry.getKey());
            sb.append(": ");
            sb.append(entry.getValue());
            sb.append("\n");
        }
        sb.append("------------------------\n");
        sb.append("Total Docs: ");
        sb.append(df.getNumberOfLines());
        sb.append("\n");
        sb.append("Total Time: ");
        sb.append(elapsedTime);
        sb.append(" ms");
        return sb.toString();

    }
}

