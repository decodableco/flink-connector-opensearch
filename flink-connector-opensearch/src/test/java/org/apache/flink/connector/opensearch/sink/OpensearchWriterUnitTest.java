package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;

import java.io.IOException;

/** Unit tests for {@link OpensearchWriter}. */
public class OpensearchWriterUnitTest {

    private static final String MAPPER_PARSING_EXCEPTION_TEMPLATE =
            "OpenSearch exception [type=mapper_parsing_exception, reason=object mapping for [{}] tried to parse field [{}] as object, but got EOF, has a concrete value been provided to it?]";

    private static final String ILLEGAL_ARGUMENT_EXCEPTION =
            "OpenSearch exception [type=illegal_argument_exception, reason=failed to parse date field [{}] with format [strict_date_optional_time||epoch_millis]]";
    private static final String ROOT_CAUSE_EXCEPTION =
            "OpenSearch exception [type=date_time_parse_exception, reason=parse_exception: Failed to parse field [{}]]";

    @Test
    public void testDedup() throws IOException {
        // Simulate three failed responses with the same root cause and one with a distinct root
        // cause. The
        // resulting exception should only include 1 suppressed exception.
        BulkItemResponse[] responses =
                new BulkItemResponse[] {
                    new BulkItemResponse(
                            1,
                            DocWriteRequest.OpType.UPDATE,
                            new BulkItemResponse.Failure(
                                    "index",
                                    "id",
                                    new OpenSearchException(
                                            MAPPER_PARSING_EXCEPTION_TEMPLATE,
                                            new OpenSearchException(
                                                    ILLEGAL_ARGUMENT_EXCEPTION,
                                                    new OpenSearchException(
                                                            ROOT_CAUSE_EXCEPTION, "date_field1"),
                                                    "date_field1"),
                                            "_doc",
                                            "first_message"))),
                    new BulkItemResponse(
                            2,
                            DocWriteRequest.OpType.UPDATE,
                            new BulkItemResponse.Failure(
                                    "index",
                                    "id",
                                    new OpenSearchException(
                                            MAPPER_PARSING_EXCEPTION_TEMPLATE,
                                            new OpenSearchException(
                                                    ILLEGAL_ARGUMENT_EXCEPTION,
                                                    new OpenSearchException(
                                                            ROOT_CAUSE_EXCEPTION, "date_field1"),
                                                    "date_field1"),
                                            "_doc",
                                            "first_message"))),
                    new BulkItemResponse(
                            3,
                            DocWriteRequest.OpType.UPDATE,
                            new BulkItemResponse.Failure(
                                    "index",
                                    "id",
                                    new OpenSearchException(
                                            MAPPER_PARSING_EXCEPTION_TEMPLATE,
                                            new OpenSearchException(
                                                    ILLEGAL_ARGUMENT_EXCEPTION,
                                                    new OpenSearchException(
                                                            ROOT_CAUSE_EXCEPTION, "date_field2"),
                                                    "date_field2"),
                                            "_doc",
                                            "first_message"))),
                    new BulkItemResponse(
                            4,
                            DocWriteRequest.OpType.UPDATE,
                            new BulkItemResponse.Failure(
                                    "index",
                                    "id",
                                    new OpenSearchException(
                                            MAPPER_PARSING_EXCEPTION_TEMPLATE,
                                            new OpenSearchException(
                                                    ILLEGAL_ARGUMENT_EXCEPTION,
                                                    new OpenSearchException(
                                                            ROOT_CAUSE_EXCEPTION, "date_field1"),
                                                    "date_field1"),
                                            "_doc",
                                            "first_message")))
                };

        BulkResponse bulkResponse = new BulkResponse(responses, 1);

        try {
            OpensearchWriter.dedupBulkResponseExceptionsAndHandleFailures(
                    bulkResponse, OpensearchWriter.DEFAULT_FAILURE_HANDLER);
        } catch (FlinkRuntimeException e) {
            Assertions.assertEquals(1, e.getCause().getSuppressed().length);
        }
    }
}
