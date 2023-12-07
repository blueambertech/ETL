package dataextract

import (
	"log"
	"sync"

	"github.com/blueambertech/ETL/pkg/dataextract/ga"
	"github.com/blueambertech/ETL/pkg/dataextract/ga4"
	"github.com/blueambertech/ETL/pkg/metricdata"
	"github.com/blueambertech/ETL/pkg/util"
)

const (
	GoogleAnalytics  = "ga"
	GoogleAnalytics4 = "ga4"
	Klaviyo          = "kl"
	Embryo           = "em"
)

func GetData(srcs, metrics, start, end, dimensions []string) []*metricdata.Metric {
	allMetrics := []*metricdata.Metric{}
	for _, metricName := range metrics {
		switch metricName {
		case metricdata.TotalRevenue:
			if data, err := GetTotalRevenueData(srcs, start, end, dimensions); err == nil {
				allMetrics = append(allMetrics, data...)
			} else {
				log.Println("dataextract.GetData error pulling total revenue: " + err.Error())
			}
		}
	}
	return allMetrics
}

func GetTotalRevenueData(srcs, start, end, dimensions []string) ([]*metricdata.Metric, error) {
	var wg sync.WaitGroup
	pullErrs := make(chan error, len(srcs))
	revenueData := make(chan *metricdata.Metric)
	var metrics []*metricdata.Metric

	for i := range srcs {
		switch srcs[i] {
		case GoogleAnalytics:
			wg.Add(1)
			go ga.GetMetrics(&wg, ga.TotalRevenue, dimensions, start, end, revenueData, pullErrs)
		case GoogleAnalytics4:
			wg.Add(1)
			go ga4.GetMetrics(&wg, ga4.TotalRevenue, dimensions, start, end, revenueData, pullErrs)
		default:
			log.Println("dataextract.GetTotalRevenueData warning unrecognised src: ", srcs[i])
		}
	}
	go func() {
		wg.Wait()
		close(pullErrs)
		close(revenueData)
	}()

	for m := range revenueData {
		metrics = append(metrics, m)
	}

	errs := util.JoinChannelErrors(pullErrs, ", ")
	if len(errs) > 0 {
		log.Println("dataextract.GetTotalRevenueData warning, error(s) occurred with data pull: ", errs)
	}
	return util.RemoveZeroEntries(metrics), nil
}
