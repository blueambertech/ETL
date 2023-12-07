package ga

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blueambertech/ETL/pkg/metricdata"
	ar "google.golang.org/api/analyticsreporting/v4"
)

const (
	pullTimeout  = time.Second * 30
	viewID       = "<Google Analytics View ID>"
	TotalRevenue = "totalRevenue"
)

var dimensionMap = map[string]string{
	metricdata.DimensionChannelGroup: "ga:channelGrouping",
	metricdata.DimensionSource:       "ga:source",
	metricdata.DimensionMedium:       "ga:medium",
}

// GetMetrics is a function designed to be run concurrently, it connects to the Google Analytics API and retrieves data based on the provided
// params. It then transforms these data into common objects (metricdata.Metric). It outputs the retrieved metrics or any errors to the provided channels.
func GetMetrics(wg *sync.WaitGroup, metric string, dimensions, start, end []string, dataOut chan<- *metricdata.Metric, errs chan<- error) {
	defer wg.Done()
	ctx, cancel := context.WithTimeout(context.Background(), pullTimeout)
	defer cancel()

	client, err := ar.NewService(ctx)
	if err != nil {
		errs <- fmt.Errorf("ga.GetMetrics unable to create Analytics service: %v", err)
		return
	}

	dateRanges := []*ar.DateRange{}
	for i := range start {
		dateRanges = append(dateRanges, &ar.DateRange{
			StartDate: start[i],
			EndDate:   end[i],
		})
	}

	dims := []*ar.Dimension{}
	for _, dim := range dimensions {
		dims = append(dims, &ar.Dimension{
			Name: dimensionMap[dim],
		})
	}

	request := &ar.ReportRequest{
		ViewId:     viewID,
		DateRanges: dateRanges,
		Metrics: []*ar.Metric{
			{
				Expression: "ga:transactionRevenue",
			},
		},
		Dimensions: dims,
	}

	getReportsRequest := &ar.GetReportsRequest{
		ReportRequests: []*ar.ReportRequest{request},
	}

	response, err := client.Reports.BatchGet(getReportsRequest).Do()
	if err != nil {
		errs <- err
		return
	}

	for _, report := range response.Reports {
		for _, row := range report.Data.Rows {
			mStart, mEnd := start[0], end[0]
			if len(start) > 1 {
				mStart, mEnd, err = getDateRange(row, start, end)
				if err != nil {
					errs <- errors.New("ga.GetMetrics error when extracting dates: " + err.Error())
					return
				}
			}

			m := &metricdata.Metric{
				Source:    "GA",
				Name:      metric,
				Value:     row.Metrics[0].Values[0],
				Dimension: row.Dimensions[0],
				Start:     mStart,
				End:       mEnd,
			}
			dataOut <- m
		}
	}
}

func getDateRange(row *ar.ReportRow, start, end []string) (string, string, error) {
	if len(row.Dimensions) < 2 {
		return "", "", errors.New("ga.getDateRange error with dimension values length")
	}
	idxName := row.Dimensions[1]
	s := strings.Split(idxName, "_")
	i, e := strconv.Atoi(s[len(s)-1])
	if e != nil {
		return "", "", e
	}
	return start[i], end[i], nil
}
