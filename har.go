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
		}

		reqQueryParams := reqURL.Query()
		if !reqQueryParams.Has("query_id") {
			continue
		}

		for key := range reqQueryParams {
			if key == "user" {
				continue
			}
			value := reqQueryParams.Get(key)

			// overwrite database
			if value == "otel_v2" {
				value = DatabaseOverride
			}

			// value is probably a UTC time. This will eventually break.
			if strings.HasPrefix(value, "17") {
				parsedTime := parseTimestamp(value)
				// Shift timestamp to current time
				if !parsedTime.IsZero() {
					parsedTime = parsedTime.Add(time.Since(fileStartTime))
					value = strconv.Itoa(int(parsedTime.UnixMilli()))
				}
			}

			// TODO: extract settings too?
			if strings.HasPrefix(key, "param_") {
				query.Parameters[key[6:]] = value
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
