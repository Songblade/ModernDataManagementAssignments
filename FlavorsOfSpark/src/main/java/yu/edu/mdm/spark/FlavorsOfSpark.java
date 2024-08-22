package yu.edu.mdm.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.File;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FlavorsOfSpark {
    private static JavaSparkContext sc;
    private static SparkSession spark;
    private static final Encoder<Book> bookEncoder = Encoders.bean(Book.class);

    // For each thing that you want added to the writeup, I print to the terminal
    // If you wanted it in a separate file, I also do that in the code

    public static void main(String[] args) {
        // Note: I'm getting strange errors if I try to run this directly from IntelliJ
        // If I run it directly from Maven through FakeTest.java, it works, but I get strange warnings
        SparkConf sparkConf = new SparkConf().setAppName("RDD Flavor")
                .setMaster("local").set("spark.executor.memory", "2g");
        sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> titles = step1();
        step2(titles);
        sc.close();

        // it looks like the dataframe stuff is from completely different API
        spark = SparkSession.builder()
                .appName("Dataset Flavor").master("local")
                .getOrCreate();

        Dataset<Row> bookRows = step3();
        Dataset<Book> bookSet = step4(bookRows);
        Dataset<Row> goodDateSet = step5(bookSet);

        step6(goodDateSet);
        step7(goodDateSet);
        step8(goodDateSet);

        spark.stop();

    }

    // I have decided to use a method for each stage
    // Step 1
    private static JavaRDD<String> step1() {
        JavaRDD<String> bookRows = sc.textFile("books.csv");
        bookRows = bookRows.map(bookRow -> {
            // there's probably a better way of doing things, but the easiest other way I can find involves
            // making a dataframe and then converting back
            int firstComma = bookRow.indexOf(',');
            int titleStart = bookRow.indexOf(',', firstComma + 1) + 1;
            int titleEnd;
            if (bookRow.charAt(titleStart) == '"') {
                titleEnd = bookRow.indexOf('"', titleStart + 1) + 1;
            } else {
                titleEnd = bookRow.indexOf(',', titleStart);
            }
            return bookRow.substring(titleStart, titleEnd);
        });

        printRDD(bookRows);
        return bookRows;
    }

    private static void step2(JavaRDD<String> titles) {
        JavaRDD<String> harryPotterTitles = titles.filter(title -> title.contains("Harry Potter"));
        System.out.println(); // to give space between the outputs
        printRDD(harryPotterTitles);

        String directoryName = "rddOutput";
        File directory = new File(directoryName);
        if (directory.exists()) {
            directory.delete();
        }
        // TODO: Figure out file saving
        //harryPotterTitles.saveAsTextFile(directoryName);
    }

    private static Dataset<Row> step3() {
        Dataset<Row> bookRows = spark.read().format("csv")
                .option("header", "true").load("books.csv");

        System.out.println();
        System.out.println(bookRows.showString(5, 0, true));

        return bookRows;
    }

    private static Dataset<Book> step4(Dataset<Row> bookRows) {
        bookRows.printSchema();
        Dataset<Book> bookSet = bookRows.map((MapFunction<Row, Book>) row->
            new Book(new String[]{row.getAs("id"), row.getAs("authorId"), row.getAs("title"), row.getAs("releaseDate"), row.getAs("link")}),
                bookEncoder);

        System.out.println(bookSet.showString(5, 0, true));
        bookSet.printSchema();

        // TODO: Save this also
        //saveDataset(bookSet, "dataSet", "json");

        return bookSet;
    }

    private static Dataset<Row> step5(Dataset<Book> bookSet) {
        Dataset<Row> newSet = bookSet.withColumn("niceDate",
                        functions.concat(functions.col("releaseDate.year").plus(functions.lit(1900)),
                                        functions.lit("-"),
                                        functions.col("releaseDate.month").plus(functions.lit(1)),
                                        functions.lit('-'), functions.col("releaseDate.date"))
                                .cast("date"))
                .drop("releaseDate");
        // since it's stored as number of years since 1900, I have to add 1900 to get the full date
        // while month is stored starting from 0, so I have to add 1 to it
        // it sounds like this is what you want for date, though it isn't clear
        System.out.println(newSet.showString(25, 0, true));
        newSet.printSchema();

        // TODO: Save this also
        //saveDataset(newSet, "niceDate", "csv");

        return newSet;
    }

    private static void step6(Dataset<Row> bookSet) {
        Dataset<Row> sortedSet = bookSet.sort(functions.desc("niceDate"));
        System.out.println(sortedSet.showString(25, 0, true));
    }

    private static void step7(Dataset<Row> bookSet) {
        Dataset<Row> nullSet = bookSet.filter(functions.col("niceDate").isNull());
        System.out.println(nullSet.showString(25, 0, true));
    }

    private static void step8(Dataset<Row> bookSet) {
        Dataset<Row> mostBooks = bookSet.groupBy("authorId")
                .count()
                .withColumnRenamed("authorId", "id")
                .sort(functions.desc("count"))
                .limit(1);
        System.out.println(mostBooks.showString(1, 0, true));
    }

    private static void printRDD(JavaRDD<String> rdd) {
        // print them in a pretty way
        for (String element : rdd.collect()) {
            System.out.println(element);
        }
    }

    private static <T> void saveDataset(Dataset<T> dataset, String directoryName, String format) {
        File directory = new File(directoryName);
        if (directory.exists()) {
            directory.delete();
        }
        dataset.write().format(format).save(directoryName);
    }

    // Something here is probably unnecessary or roundabout, but it took many tries to get this working
    // So I'm touching nothing
    public static class Book implements Serializable {
        private int id;
        private int authorId;
        private String link;
        private String title;
        private Date releaseDate;

        public Book(){}

        public Book(String[] args) {
            this.id = Integer.parseInt(args[0]);
            this.authorId = Integer.parseInt(args[1]);
            this.title = args[2];
            try {
                this.releaseDate = args[3] == null? null : new SimpleDateFormat("MM/dd/yy").parse(args[3]);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            this.link = args[4];
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getAuthorId() {
            return authorId;
        }

        public void setAuthorId(int authorId) {
            this.authorId = authorId;
        }

        public String getLink() {
            return link;
        }

        public void setLink(String link) {
            this.link = link;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public Date getReleaseDate() {
            return releaseDate;
        }

        public void setReleaseDate(Date releaseDate) {
            this.releaseDate = releaseDate;
        }
    }

}
