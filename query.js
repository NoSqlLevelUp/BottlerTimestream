
import { constants } from "./constants.js";
import {CancelQueryCommand, QueryCommand} from "@aws-sdk/client-timestream-query";

const factory = "buillding-24Gju";

// See records ingested into this table so far
const SELECT_ALL_QUERY = "SELECT * FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME;

//1. Find the average, p90, p95, and p99 bottled still water for a specific factory over the past 2 hours.
const QUERY_1 = "SELECT province, region, factory, BIN(time, 15s) AS binned_timestamp, " +
    "    ROUND(AVG(measure_value::double), 2) AS avg_still_water, " +
    "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_still_water, " +
    "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_still_water, " +
    "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_still_water " +
    "FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "WHERE measure_name = 'still_water' " +
    "   AND factory = '" + factory + "' " +
    "    AND time > ago(2h) " +
    "GROUP BY province, factory, region, BIN(time, 15s) " +
    "ORDER BY binned_timestamp ASC";

//2. Identify factories that bottled still water quantity that is higher by 10% or more compared to the average still water bottling of the entire collection of factories for the past 2 hours.
const QUERY_2 = "WITH avg_factory_bottled_still_water AS ( " +
    "    SELECT COUNT(DISTINCT factory) AS total_host_count, AVG(measure_value::double) AS fleet_avg_still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'still_water' " +
    "        AND time > ago(2h) " +
    "), avg_per_factory_still_water AS ( " +
    "    SELECT province, region, factory, AVG(measure_value::double) AS avg_still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'still_water' " +
    "        AND time > ago(2h) " +
    "    GROUP BY province, region, factory " +
    ") " +
    "SELECT province, region, factory, avg_still_water, fleet_avg_still_water " +
    "FROM avg_factory_bottled_still_water, avg_per_factory_still_water " +
    "WHERE avg_still_water > 1.1 * factories_avg_still_water " +
    "ORDER BY avg_still_water DESC";

//3. Find the average still water bottling binned at 30 second intervals for a specific factory over the past 2 hours.
const QUERY_3 = "SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_still_water, " +
    "factory FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "WHERE measure_name = 'still_water' " +
    "    AND factory = '" + factory + "' " +
    "    AND time > ago(2h) " +
    "GROUP BY factory, BIN(time, 30s) " +
    "ORDER BY binned_timestamp ASC";

//4. Find the average still water binned at 30 second intervals for a specific factory over the past 2 hours, filling in the missing values using linear interpolation.
const QUERY_4 = "WITH binned_timeseries AS ( " +
    "    SELECT factory, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'still_water' " +
    "       AND factory = '" + factory + "' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory, BIN(time, 30s) " +
    "), interpolated_timeseries AS ( " +
    "    SELECT factory, " +
    "        INTERPOLATE_LINEAR( " +
    "            CREATE_TIME_SERIES(binned_timestamp, avg_still_water), " +
    "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_still_water " +
    "    FROM binned_timeseries " +
    "    GROUP BY factory " +
    ") " +
    "SELECT time, ROUND(value, 2) AS interpolated_still_water " +
    "FROM interpolated_timeseries " +
    "CROSS JOIN UNNEST(interpolated_avg_still_water)";

//5. Find the average still water binned at 30 second intervals for a specific factory over the past 2 hours, filling in the missing values using interpolation based on the last observation carried forward.
const QUERY_5 = "WITH binned_timeseries AS ( " +
    "    SELECT factory, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'still_water' " +
    "        AND factory = '" + factory + "' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory, BIN(time, 30s) " +
    "), interpolated_timeseries AS ( " +
    "    SELECT factory, " +
    "        INTERPOLATE_LOCF( " +
    "            CREATE_TIME_SERIES(binned_timestamp, avg_still_water), " +
    "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_still_water " +
    "    FROM binned_timeseries " +
    "    GROUP BY factory " +
    ") " +
    "SELECT time, ROUND(value, 2) AS interpolated_still_water " +
    "FROM interpolated_timeseries " +
    "CROSS JOIN UNNEST(interpolated_avg_still_water)";

//6. Find the average still water binned at 30 second intervals for a specific factory over the past 2 hours, filling in the missing values using interpolation based on a constant value.
const QUERY_6 = "WITH binned_timeseries AS ( " +
    "    SELECT factory, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'still_water' " +
    "       AND factory = '" + factory + "' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory, BIN(time, 30s) " +
    "), interpolated_timeseries AS ( " +
    "    SELECT factory, " +
    "        INTERPOLATE_FILL( " +
    "            CREATE_TIME_SERIES(binned_timestamp, avg_still_water), " +
    "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s), 10.0) AS interpolated_avg_still_water " +
    "    FROM binned_timeseries " +
    "    GROUP BY factory " +
    ") " +
    "SELECT time, ROUND(value, 2) AS interpolated_still_water " +
    "FROM interpolated_timeseries " +
    "CROSS JOIN UNNEST(interpolated_avg_still_water)";

//7. Find the average still water binned at 30 second intervals for a specific factory over the past 2 hours, filling in the missing values using cubic spline interpolation.
const QUERY_7 = "WITH binned_timeseries AS ( " +
    "    SELECT factory, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'still_water' " +
    "        AND factory = '" + factory + "' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory, BIN(time, 30s) " +
    "), interpolated_timeseries AS ( " +
    "    SELECT factory, " +
    "        INTERPOLATE_SPLINE_CUBIC( " +
    "            CREATE_TIME_SERIES(binned_timestamp, avg_still_water), " +
    "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_still_water " +
    "    FROM binned_timeseries " +
    "    GROUP BY factory " +
    ") " +
    "SELECT time, ROUND(value, 2) AS interpolated_still_water " +
    "FROM interpolated_timeseries " +
    "CROSS JOIN UNNEST(interpolated_avg_still_water)";

//8. Find the average still water binned at 30 second intervals for all factories over the past 2 hours, filling in the missing values using linear interpolation.
const QUERY_8 = "WITH per_host_min_max_timestamp AS ( " +
    "    SELECT factory, min(time) as min_timestamp, max(time) as max_timestamp " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'still_water' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory " +
    "), interpolated_timeseries AS ( " +
    "    SELECT m.factory, " +
    "        INTERPOLATE_LINEAR( " +
    "            CREATE_TIME_SERIES(time, measure_value::double), " +
    "                SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 1s)) as interpolated_avg_still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " m " +
    "        INNER JOIN per_host_min_max_timestamp ph ON m.factory = ph.factory " +
    "    WHERE measure_name = 'still_water' " +
    "        AND time > ago(2h) " +
    "    GROUP BY m.factory " +
    ") " +
    "SELECT factory, AVG(still_water) AS avg_still_water " +
    "FROM interpolated_timeseries " +
    "CROSS JOIN UNNEST(interpolated_avg_still_water) AS t (time, still_water) " +
    "GROUP BY factory " +
    "ORDER BY avg_still_water DESC";

//9. Find the percentage of measurements with bottled still water above 70% for a specific factory over the past 2 hours, filling in the missing values using linear interpolation.
const QUERY_9 = "WITH time_series_view AS ( " +
    "    SELECT INTERPOLATE_LINEAR( " +
    "        CREATE_TIME_SERIES(time, ROUND(measure_value::double,2)), " +
    "        SEQUENCE(min(time), max(time), 10s)) AS still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE factory = '" + factory + "' " +
    "        AND  " +
    "measure_name = 'still_water' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory " +
    ") " +
    "SELECT FILTER(still_water, x -> x.value > 70.0) AS still_water_above_threshold, " +
    "    REDUCE(FILTER(still_water, x -> x.value > 70.0), 0, (s, x) -> s + 1, s -> s) AS count_still_water_above_threshold, " +
    "    ROUND(REDUCE(still_water, CAST(ROW(0, 0) AS ROW(count_high BIGINT, count_total BIGINT)), " +
    "        (s, x) -> CAST(ROW(s.count_high + IF(x.value > 70.0, 1, 0), s.count_total + 1) AS ROW(count_high BIGINT, count_total BIGINT)), " +
    "        s -> IF(s.count_total = 0, NULL, CAST(s.count_high AS DOUBLE) / s.count_total)), 4) AS fraction_still_water_above_threshold " +
    "FROM time_series_view";

//10. List the measurements with still water lower than 75% for a specific factory over the past 2 hours, filling in the missing values using linear interpolation.
const QUERY_10 = "WITH time_series_view AS ( " +
    "    SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR( " +
    "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
    "        SEQUENCE(min(time), max(time), 10s)) AS still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE  " +
    " factory = '" + factory + "' " +
    "        AND  " +
    "measure_name = 'still_water' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory " +
    ") " +
    "SELECT FILTER(still_water, x -> x.value < 75 AND x.time > oldest_time + 1m) " +
    "FROM time_series_view";

//11. Find the total number of measurements with of still water of 0% for a specific factory over the past 2 hours, filling in the missing values using linear interpolation.
const QUERY_11 = "WITH time_series_view AS ( " +
    "    SELECT INTERPOLATE_LINEAR( " +
    "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
    "        SEQUENCE(min(time), max(time), 10s)) AS still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "     WHERE  " +
    " factory = '" + factory + "' " +
    "        AND  " +
    "measure_name = 'still_water' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory " +
    ") " +
    "SELECT REDUCE(still_water, " +
    "    DOUBLE '0.0', " +
    "    (s, x) -> s + 1, " +
    "    s -> s) AS count_still_water " +
    "FROM time_series_view";

//12. Find the average still water for a factory over the past 2 hours, filling in the missing values using linear interpolation.
const QUERY_12 = "WITH time_series_view AS ( " +
    "    SELECT INTERPOLATE_LINEAR( " +
    "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
    "        SEQUENCE(min(time), max(time), 10s)) AS still_water " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "     WHERE  " +
    " factory = '" + factory + "' " +
    "     AND  " +
    "measure_name = 'still_water' " +
    "        AND time > ago(2h) " +
    "    GROUP BY factory " +
    ") " +
    "SELECT REDUCE(still_water, " +
    "    CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), " +
    "    (s, x) -> CAST(ROW(x.value + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), " +
    "     s -> IF(s.count = 0, NULL, s.sum / s.count)) AS avg_still_water " +
    "FROM time_series_view";

export async function runAllQueries(queryClient) {
    const queries = [QUERY_1]// QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7, QUERY_8, QUERY_9, QUERY_10, QUERY_11, QUERY_12];

    for (let i = 0; i < queries.length; i++) {
        console.log(`Running query ${i+1} : ${queries[i]}`);
        await getAllRows(queryClient, queries[i], null);
    }
}

export async function getAllRows(queryClient, query, nextToken) {
    const params = new QueryCommand({
        QueryString: query
    });

    if (nextToken) {
        params.input.NextToken = nextToken
    }

    await queryClient.send(params).then(
            (response) => {
                parseQueryResult(response);
                if (response.NextToken) {
                    getAllRows(queryClient, query, response.NextToken);
                }
            },
            (err) => {
                console.error("Error while querying:", err);
            });
}

export async function tryQueryWithMultiplePages(queryClient, limit) {
    const queryWithLimits = SELECT_ALL_QUERY + " LIMIT " + limit;
    console.log(`Running query with multiple pages: ${queryWithLimits}`);
    await getAllRows(queryClient, queryWithLimits, null)
}

export async function tryCancelQuery(queryClient) {
    const params = new QueryCommand({
        QueryString: SELECT_ALL_QUERY
    });
    console.log(`Running query: ${SELECT_ALL_QUERY}`);

    await queryClient.send(params)
        .then(
            async (response) => {
                await cancelQuery(queryClient, response.QueryId);
            },
            (err) => {
                console.error("Error while executing select all query:", err);
            });
}

export async function cancelQuery(queryClient, queryId) {
    const cancelParams = new CancelQueryCommand({
        QueryId: queryId
    });
    console.log(`Sending cancellation for query: ${SELECT_ALL_QUERY}`);
    await queryClient.send(cancelParams).then(
            (response) => {
                console.log("Query has been cancelled successfully");
            },
            (err) => {
                console.error("Error while cancelling select all:", err);
            });
}

function parseQueryResult(response) {
    const columnInfo = response.ColumnInfo;
    const rows = response.Rows;

    console.log("Metadata: " + JSON.stringify(columnInfo));
    console.log("Data: ");

    rows.forEach(function (row) {
        console.log(parseRow(columnInfo, row));
    });
}

function parseRow(columnInfo, row) {
    const data = row.Data;
    const rowOutput = [];

    var i;
    for ( i = 0; i < data.length; i++ ) {
        let info = columnInfo[i];
        let datum = data[i];
        rowOutput.push(parseDatum(info, datum));
    }

    return `{${rowOutput.join(", ")}}`
}

function parseDatum(info, datum) {
    if (datum.NullValue != null && datum.NullValue === true) {
        return `${info.Name}=NULL`;
    }

    const columnType = info.Type;

    // If the column is of TimeSeries Type
    if (columnType.TimeSeriesMeasureValueColumnInfo != null) {
        return parseTimeSeries(info, datum);
    }
    // If the column is of Array Type
    else if (columnType.ArrayColumnInfo != null) {
        const arrayValues = datum.ArrayValue;
        return `${info.Name}=${parseArray(info.Type.ArrayColumnInfo, arrayValues)}`;
    }
    // If the column is of Row Type
    else if (columnType.RowColumnInfo != null) {
        const rowColumnInfo = info.Type.RowColumnInfo;
        const rowValues = datum.RowValue;
        return parseRow(rowColumnInfo, rowValues);
    }
    // If the column is of Scalar Type
    else {
        return parseScalarType(info, datum);
    }
}

function parseTimeSeries(info, datum) {
    const timeSeriesOutput = [];
    datum.TimeSeriesValue.forEach(function (dataPoint) {
        timeSeriesOutput.push(`{time=${dataPoint.Time}, value=${parseDatum(info.Type.TimeSeriesMeasureValueColumnInfo, dataPoint.Value)}}`)
    });

    return `[${timeSeriesOutput.join(", ")}]`
}

function parseScalarType(info, datum) {
    return parseColumnName(info) + datum.ScalarValue;
}

function parseColumnName(info) {
    return info.Name == null ? "" : `${info.Name}=`;
}

function parseArray(arrayColumnInfo, arrayValues) {
    const arrayOutput = [];
    arrayValues.forEach(function (datum) {
        arrayOutput.push(parseDatum(arrayColumnInfo, datum));
    });
    return `[${arrayOutput.join(", ")}]`
}