package fr.remyqueny.sncf.producer;

import java.io.IOException;
import java.util.Optional;

public final class GtfsRealtimeDownloadException extends IOException {

    public enum Kind {
        HTTP_RESPONSE,
        INVALID_FEED
    }

    private final Kind kind;
    private final int statusCode;
    private final String retryAfter;

    private GtfsRealtimeDownloadException(
            Kind kind,
            String message,
            int statusCode,
            String retryAfter,
            Throwable cause
    ) {
        super(message, cause);
        this.kind = kind;
        this.statusCode = statusCode;
        this.retryAfter = retryAfter;
    }

    public static GtfsRealtimeDownloadException httpResponse(
            int statusCode,
            String retryAfter
    ) {
        return new GtfsRealtimeDownloadException(
                Kind.HTTP_RESPONSE,
                "Téléchargement refusé : HTTP " + statusCode,
                statusCode,
                retryAfter,
                null
        );
    }

    public static GtfsRealtimeDownloadException invalidFeed(String message) {
        return invalidFeed(message, null);
    }

    public static GtfsRealtimeDownloadException invalidFeed(
            String message,
            Throwable cause
    ) {
        return new GtfsRealtimeDownloadException(
                Kind.INVALID_FEED,
                message,
                0,
                null,
                cause
        );
    }

    public Kind kind() {
        return kind;
    }

    public int statusCode() {
        return statusCode;
    }

    public Optional<String> retryAfter() {
        return Optional.ofNullable(retryAfter);
    }
}