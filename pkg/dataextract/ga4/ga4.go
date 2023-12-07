package ga4

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blueambertech/ETL/pkg/metricdata"
	ga "google.golang.org/api/analyticsdata/v1beta"
)

const (
	pullTimeout  = time.Second * 30
	propertyID   = "<GA4 Property ID>"
	TotalRevenue = "totalRevenue"
)

var dimensionMap = map[string]string{
	metricdata.DimensionChannelGroup: "defaultChannelGroup",
	metricdata.DimensionSource:       "firstUserSource",
	metricdata.DimensionMedium:       "firstUserMedium",
}

// GetMetrics is a function designed to be run concurrently, it connects to the Google Analytics 4 API and retrieves data based on the provided
// params. It then transforms these data into common objects (metricdata.Metric). It outputs the retrieved metrics or any errors to the provided channels.
func GetMetrics(wg *sync.WaitGroup, metric string, dimensions, start, end []string, dataOut chan<- *metricdata.Metric, errs chan<- error) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), pullTimeout)
	defer cancel()

	client, err := ga.NewService(ctx)
	if err != nil {
		errs <- errors.New("ga4.GetMetrics error creating new service: " + err.Error())
		return
	}

	if len(start) != len(end) {
		errs <- errors.New("ga4.GetMetrics error with length of start dates not matching length of end dates")
		return
	}

	dateRanges := []*ga.DateRange{}
	for i := range start {
		dateRanges = append(dateRanges, &ga.DateRange{
			StartDate: start[i],
			EndDate:   end[i],
		})
	}

	dims := []*ga.Dimension{}
	for _, dim := range dimensions {
		dims = append(dims, &ga.Dimension{
			Name: dimensionMap[dim],
		})
	}

	runReportRequest := &ga.RunReportRequest{
		DateRanges: dateRanges,
		Dimensions: dims,
		Metrics: []*ga.Metric{
			{
				Name: metric,
			},
		},
	}

	results, err := client.Properties.RunReport("properties/"+propertyID, runReportRequest).Do()
	if err != nil {
		errs <- errors.New("ga4.GetMetrics error making call to get data: " + err.Error())
		return
	}
	if results.RowCount < 1 {
		errs <- errors.New("ga4.GetMetrics error with no rows returned")
		return
	}

	for i := 0; i < len(results.Rows); i++ {
		row := results.Rows[i]
		mStart, mEnd := start[0], end[0]
		if len(start) > 1 {
			mStart, mEnd, err = getDateRange(row, start, end)
			if err != nil {
				errs <- errors.New("ga4.GetMetrics error when extracting dates: " + err.Error())
				return
			}
		}

		m := &metricdata.Metric{
			Source:    "GA4",
			Name:      metric,
			Value:     row.MetricValues[0].Value,
			Dimension: row.DimensionValues[0].Value,
			Start:     mStart,
			End:       mEnd,
		}
		dataOut <- m
	}
}

func getDateRange(row *ga.Row, start, end []string) (string, string, error) {
	if len(row.DimensionValues) < 2 {
		return "", "", errors.New("ga4.getDateRange error with dimension values length")
	}
	idxName := row.DimensionValues[1].Value
	s := strings.Split(idxName, "_")
	i, e := strconv.Atoi(s[len(s)-1])
	if e != nil {
		return "", "", e
	}
	return start[i], end[i], nil
}
