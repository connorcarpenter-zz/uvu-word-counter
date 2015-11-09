import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class CountThread implements Runnable {

    public ArrayList<String> linesToProcess;
    private HashMap<String, Integer> wordMap;
    private HashMap<String, Integer> mainWordMap;
    private String outputPath;

    CountThread(String outputPath, HashMap<String, Integer> mainWordMap) {
        this.outputPath = outputPath;
        this.mainWordMap = mainWordMap;
        wordMap = new HashMap<>();
        linesToProcess = new ArrayList<>();
    }

    @Override
    public void run() {
        for(String str : linesToProcess){
            String[] words = str.split("[^a-z]+");
            for(String word : words){
                if(word.length() != 0)
                    addWord(word);
            }
        }

        WordCount.WordMapToFile(wordMap, outputPath);
    }

    private void addWord(String word){
        try {
            wordMap.put(word, (wordMap.containsKey(word) ? wordMap.get(word) : 0) + 1);
            mainWordMap.put(word, (mainWordMap.containsKey(word) ? mainWordMap.get(word) : 0) + 1);
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }
}

public class WordCount {

    private static int chunkSize;
    private static int maxThreadNumber;
    private static String outputDirPath;
    private static boolean pathIsDirectory;
    private static File pathFile;

    public static void main(String[] args){
        if(!Initialize(args))
            return;
        ArrayList<String> filesToProcess = new ArrayList<>();
        String path;
        if(pathIsDirectory) {
            Collections.addAll(filesToProcess, pathFile.list());
            path = "" + pathFile.getAbsolutePath() + "\\";
        } else {
            filesToProcess.add(pathFile.getAbsolutePath());
            path = "" + pathFile.getParent() + "\\";
        }

        //start processing
        HashMap<String, Integer> mainWordMap = new HashMap<>();
        ExecutorService threadPool = Executors.newFixedThreadPool(maxThreadNumber);
        for(String fileStr : filesToProcess){
            if(fileStr.equals("output"))continue;
            String lineToProcess;
            int chunkNumber = 0;
            int linesProcessed = 0;
            CountThread currentThread = null;
                try (
                        InputStream fis = new FileInputStream(path + fileStr);
                        InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
                        BufferedReader br = new BufferedReader(isr)) {
                    File file = new File(fileStr);
                    while ((lineToProcess = br.readLine()) != null) {
                        if(currentThread == null) {
                            String outputPath = outputDirPath + "\\" + file.getName() + "_" + chunkNumber + ".chunk";
                            currentThread = new CountThread(outputPath, mainWordMap);
                        }
                        currentThread.linesToProcess.add(lineToProcess.toLowerCase());
                        linesProcessed += 1;
                        if(linesProcessed == chunkSize){
                            threadPool.execute(currentThread);
                            linesProcessed = 0;
                            currentThread = null;
                            chunkNumber+=1;
                        }
                    }
                    if(linesProcessed > 0){
                        threadPool.execute(currentThread);
                    }
                }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        //wait for threads to finish
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //print main wordmap
        WordMapToFile(mainWordMap, outputDirPath + "\\results.txt");
    }

    static <K,V extends Comparable<? super V>> SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
        SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<>(
                (e1, e2) -> {
                    int res = e2.getValue().compareTo(e1.getValue());
                    if (e1.getKey().equals(e2.getKey())) {
                        return res;
                    } else {
                        return res != 0 ? res : 1;
                    }
                }
        );
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

    protected static void WordMapToFile(Map<String, Integer> wordMap, String pathName) {
        try {
            PrintWriter writer = new PrintWriter(pathName, "UTF-8");
            for(Map.Entry<String, Integer> entry : entriesSortedByValues(wordMap)){
                writer.println(entry.getKey() + " " + entry.getValue());
            }
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private static boolean Initialize(String[] args) {
        //evaluating arguments
        if (args.length != 3) {
            InvalidArgumentMessage();
            return false;
        }

        String pathName = args[0];
        chunkSize = Integer.parseInt(args[1]);
        maxThreadNumber = Integer.parseInt(args[2]);
        if (chunkSize < 10 || chunkSize > 5000 || maxThreadNumber < 1 || maxThreadNumber > 100) {
            InvalidArgumentMessage();
            return false;
        }

        //check if path is valid
        pathFile = new File(pathName);
        if (!pathFile.exists()) {
            PathDoesNotExistMessage(pathName);
            return false;
        }
        if(pathFile.isDirectory()) {
            pathIsDirectory = true;
        } else {
            if(pathFile.isFile()) {
                pathIsDirectory = false;
            } else {
                PathDoesNotExistMessage(pathName);
                return false;
            }
        }

        //see if possible to create output directory
        if(pathIsDirectory){
            outputDirPath = pathName;
        } else {
            outputDirPath = pathFile.getParent();
        }
        outputDirPath += "\\output";
        File outputDir = new File(outputDirPath);

        //delete output folder if it exists
        if(outputDir.exists()){
            DeleteDirectory(outputDir);
            if(outputDir.exists()){
                CannotCreateOutputDirMessage();
                return false;
            }
        }

        //create new output folder
        outputDir.mkdir();

        if(!outputDir.exists()){
            CannotCreateOutputDirMessage();
            return false;
        }

        return true;
    }

    private static void DeleteDirectory(File dir) {
        String[]entries = dir.list();
        for(String s: entries){
            File currentFile = new File(dir.getPath(),s);
            currentFile.delete();
        }
        dir.delete();
    }

    private static void CannotCreateOutputDirMessage() {
        System.out.println("Cannot create output directory, please try again.");
    }

    private static void PathDoesNotExistMessage(String pathName) {
        System.out.println("No such file/directory: " + pathName);
    }

    private static void InvalidArgumentMessage() {
        System.out.println("Usage: java WordCount <file|directory> <chunk size 10-5000> <num of threads 1-100>");
    }
}
