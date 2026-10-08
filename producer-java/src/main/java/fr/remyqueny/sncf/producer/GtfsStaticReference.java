package fr.remyqueny.sncf.producer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.DuplicateHeaderMode;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public record GtfsStaticReference(
        Map<String, String> tripRoutes,
        Map<String, Route> routes,
        Map<String, Stop> stops
) {

    public GtfsStaticReference {
        tripRoutes = Map.copyOf(tripRoutes);
        routes = Map.copyOf(routes);
        stops = Map.copyOf(stops);
    }

    public record Route(
            String routeId,
            String shortName,
            String longName
    ) {
    }

    public record Stop(String stopId, String name) {
    }

    public static GtfsStaticReference load(Path archivePath)
            throws IOException {

        try (ZipFile archive = new ZipFile(archivePath.toFile())) {
            Map<String, String> tripRoutes = readTable(
                    archive,
                    "trips.txt",
                    "trip_id",
                    row -> requiredValue(row, "route_id"),
                    "trip_id", "route_id"
            );

            Map<String, Route> routes = readTable(
                    archive,
                    "routes.txt",
                    "route_id",
                    row -> new Route(
                            requiredValue(row, "route_id"),
                            row.get("route_short_name"),
                            row.get("route_long_name")
                    ),
                    "route_id", "route_short_name", "route_long_name"
            );

            Map<String, Stop> stops = readTable(
                    archive,
                    "stops.txt",
                    "stop_id",
                    row -> new Stop(
                            requiredValue(row, "stop_id"),
                            row.get("stop_name")
                    ),
                    "stop_id", "stop_name"
            );

            return new GtfsStaticReference(tripRoutes, routes, stops);
        }
    }

    private static <T> Map<String, T> readTable(
            ZipFile archive,
            String filename,
            String keyColumn,
            Function<CSVRecord, T> mapper,
            String... requiredColumns
    ) throws IOException {

        var entries = archive.stream()
                .filter(entry -> !entry.isDirectory())
                .filter(entry ->
                        entry.getName().equals(filename)
                                || entry.getName().endsWith("/" + filename)
                )
                .toList();

        if (entries.size() != 1) {
            throw new IOException(
                    "Fichier absent ou ambigu dans le ZIP : " + filename
            );
        }

        ZipEntry entry = entries.get(0);
        Map<String, T> result = new HashMap<>();

        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setDuplicateHeaderMode(DuplicateHeaderMode.DISALLOW)
                .get();

        try (PushbackReader reader = new PushbackReader(
                new InputStreamReader(
                        archive.getInputStream(entry),
                        StandardCharsets.UTF_8
                ),
                1
        )) {
            int firstCharacter = reader.read();
            if (firstCharacter != -1 && firstCharacter != '\uFEFF') {
                reader.unread(firstCharacter);
            }

            try (CSVParser parser = format.parse(reader)) {
                for (String column : requiredColumns) {
                    if (!parser.getHeaderMap().containsKey(column)) {
                        throw new IOException(
                                filename + " : colonne absente : " + column
                        );
                    }
                }

                for (CSVRecord row : parser) {
                    if (!row.isConsistent()) {
                        throw new IOException(
                                filename + " : ligne CSV incohérente : "
                                        + row.getRecordNumber()
                        );
                    }

                    String key = requiredValue(row, keyColumn);
                    T value = mapper.apply(row);

                    if (result.putIfAbsent(key, value) != null) {
                        throw new IOException(
                                filename + " : identifiant dupliqué : " + key
                        );
                    }
                }
            }
        }

        if (result.isEmpty()) {
            throw new IOException("Table de référence vide : " + filename);
        }

        return result;
    }

    private static String requiredValue(CSVRecord row, String column) {
        String value = row.get(column);

        if (value.isBlank()) {
            throw new IllegalArgumentException(
                    "Valeur vide pour " + column
                            + ", ligne " + row.getRecordNumber()
            );
        }

        return value;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "Usage : GtfsStaticReference <archive.zip>"
            );
        }

        GtfsStaticReference reference = load(Path.of(args[0]));

        System.out.println("Courses : " + reference.tripRoutes().size());
        System.out.println("Lignes : " + reference.routes().size());
        System.out.println("Arrêts : " + reference.stops().size());

        long unresolvedRoutes = reference.tripRoutes().values().stream()
                .filter(routeId -> !reference.routes().containsKey(routeId))
                .count();

        System.out.println(
                "Courses dont la ligne est absente : " + unresolvedRoutes
        );
    }
}