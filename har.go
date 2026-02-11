package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const DatabaseOverride = "otel_map"

type harFile struct {
	Log harLog `json:"log"`
}

type harLog struct {
	Entries []harEntry `json:"entries"`
}

type harEntry struct {
	Request harRequest `json:"request"`
	// Timestamp is when the request started
	Timestamp time.Time `json:"startedDateTime"`
	// Duration is how long the request ran in milliseconds
	Duration float64 `json:"time"`
}

type harRequest struct {
	Method string `json:"method"`
	URL    string `json:"url"`

	PostData struct {
		Text string `json:"text"`
	} `json:"postData"`
}

type hdxParam int

const (
	hdxParamUnknown hdxParam = iota
	hdxParamDatabase
	hdxParamTable
	hdxParamTimeRangeAfter
	hdxParamTimeRangeBefore
)

var hdxParamLookup = map[string]hdxParam{
	"HYPERDX_PARAM_1148736945": hdxParamDatabase,
	"HYPERDX_PARAM_1084069686": hdxParamTable,
	"HYPERDX_PARAM_1793348460": hdxParamTimeRangeAfter,
	"HYPERDX_PARAM_1639973743": hdxParamTimeRangeAfter,
	"HYPERDX_PARAM_1793410119": hdxParamTimeRangeAfter,
	"HYPERDX_PARAM_1642682583": hdxParamTimeRangeBefore,
}

func openHar(filePath string) ([]UserQuery, error) {
	harBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open HAR file: %w", err)
	}

	var harData harFile
	err = json.Unmarshal(harBytes, &harData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal HAR data: %w", err)
	}

	if len(harData.Log.Entries) == 0 {
		return nil, errors.New("har file has no entries")
	}

	queries := make([]UserQuery, 0, len(harData.Log.Entries))
	var fileStartTime time.Time
	for _, e := range harData.Log.Entries {
		if e.Request.Method != http.MethodPost || e.Request.PostData.Text == "" {
			continue
		}

		if fileStartTime.IsZero() {
			fileStartTime = e.Timestamp
		}

		reqURL, err := url.Parse(e.Request.URL)
		if err != nil {
			continue
		}

		sql := e.Request.PostData.Text
		sql = strings.ReplaceAll(sql, "FORMAT JSONCompactEachRowWithNamesAndTypes", "FORMAT Null")
		sql = strings.ReplaceAll(sql, "FORMAT JSONEachRow", "FORMAT Null")
		sql = strings.ReplaceAll(sql, "FORMAT JSON", "FORMAT Null")

		query := UserQuery{
			SQL:        sql,
			Parameters: make(map[string]string),
			Delay:      e.Timestamp.Sub(fileStartTime),

			FileStartTime: fileStartTime,
		}

		reqQueryParams := reqURL.Query()
		if !reqQueryParams.Has("query_id") {
			continue
		}

		for key := range reqQueryParams {
			if key == "user" {
				continue
			}
			paramName := key[6:] // remove "param_" prefix
			value := reqQueryParams.Get(key)

			if hdxParamLookup[paramName] == hdxParamDatabase {
				value = DatabaseOverride
			}

			if hdxParamLookup[paramName] == hdxParamTimeRangeAfter {
				query.TimeRangeAfterParam = paramName
			} else if hdxParamLookup[paramName] == hdxParamTimeRangeBefore {
				query.TimeRangeBeforeParam = paramName
			}

			// TODO: extract settings too?
			if strings.HasPrefix(key, "param_") {
				query.Parameters[paramName] = value
			}

			if query.TimeRangeBeforeParam != "" && query.TimeRangeAfterParam != "" {
				afterTime := parseTimestamp(query.Parameters[query.TimeRangeAfterParam])
				beforeTime := parseTimestamp(query.Parameters[query.TimeRangeBeforeParam])

				query.TimeRangeDuration = beforeTime.Sub(afterTime)
			}
		}

		queries = append(queries, query)
	}
	fmt.Println("Total HAR queries found:", len(queries))

	return queries, nil
}

func parseTimestamp(unixStr string) time.Time {
	parsedTimeInt, err := strconv.ParseInt(unixStr, 10, 64)
	if err != nil {
		return time.Time{}
	}

	parsedTime := time.UnixMilli(parsedTimeInt)

	// Exclude unrealistically outdated data
	if time.Since(parsedTime) > (365*24*3*time.Hour) || time.Since(parsedTime) < 0 {
		return time.Time{}
	}

	return parsedTime
}
