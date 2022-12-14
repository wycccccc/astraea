package org.astraea.app.backup;

import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.csv.CsvReader;
import org.astraea.common.csv.CsvReaderBuilder;
import org.astraea.common.csv.CsvWriter;
import org.astraea.fs.FileSystem;

import java.io.FileReader;
import java.io.FileWriter;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;

import static org.astraea.fs.ftp.FtpFileSystem.HOSTNAME_KEY;
import static org.astraea.fs.ftp.FtpFileSystem.PASSWORD_KEY;
import static org.astraea.fs.ftp.FtpFileSystem.PORT_KEY;
import static org.astraea.fs.ftp.FtpFileSystem.USER_KEY;

public class SkipCsv {
    private static FileSystem of(URI uri) {
        if (uri.getScheme().equals("local")) {
            return FileSystem.of("local", Configuration.of(Map.of()));
        }
        if (uri.getScheme().equals("ftp")) {
            // userInfo[0] is user, userInfo[1] is password.
            String[] userInfo = uri.getUserInfo().split(":", 2);
            return FileSystem.of(
                    "ftp",
                    Configuration.of(
                            Map.of(
                                    HOSTNAME_KEY,
                                    uri.getHost(),
                                    PORT_KEY,
                                    String.valueOf(uri.getPort()),
                                    USER_KEY,
                                    userInfo[0],
                                    PASSWORD_KEY,
                                    userInfo[1])));
        }
        throw new IllegalArgumentException("unsupported schema: " + uri.getScheme());
    }

    public static void main(String[] args) {
        var sourcePath = URI.create("local:/home/warren/ImportcsvTest/source");
        var sinkPath = URI.create("local:/home/warren/ImportcsvTest/sink");
        try (var source = of(sourcePath);
             var sink = of(sinkPath)) {
            // Process each file in target path.
            source
                    .listFiles(sourcePath.getPath())
                    .forEach(path->{
                        String csvName = findFinal(path);
                        try (CsvReader csvReader = CsvReader.builder(Utils.packException(() -> new FileReader(path))).build();
                             CsvWriter csvWriter = CsvWriter.builder(Utils.packException(() -> new FileWriter(sinkPath.getPath() + "/" + csvName))).build();
                        ){
                            csvReader.skip(2);
                            while (csvReader.hasNext()){
                                csvWriter.rawAppend(csvReader.rawNext());
                            }
                        }
                    });
        }
    }

    private static String findFinal(String path) {
        return Arrays.stream(path.split("/")).reduce((first, second) -> second).orElse("");
    }
}


