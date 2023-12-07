package generategooglesheet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/blueambertech/ETL/pkg/metricdata"
	"google.golang.org/api/sheets/v4"
)

const (
	// TODO: these variables would be updated with the URL of the data service and the ID of the spreadsheet to output to
	dataSvcEndpoint = "<data service endpoint"
	spreadsheetID   = "<ID of Google Sheet>"

	dfmt   = "2006-01-02"
	gaKey  = "GA"
	ga4Key = "GA4"
)

var (
	monthMap = map[string]int{
		"January":   1,
		"February":  2,
		"March":     3,
		"April":     4,
		"May":       5,
		"June":      6,
		"July":      7,
		"August":    8,
		"September": 9,
		"October":   10,
		"November":  11,
		"December":  12,
	}
	tabMap = map[string]map[string]string{
		gaKey: {
			metricdata.DimensionChannelGroup: "GA - Default Channel Group",
			metricdata.DimensionMedium:       "GA - Medium",
			metricdata.DimensionSource:       "GA - Source",
		},
		ga4Key: {
			metricdata.DimensionChannelGroup: "GA4 - Default Channel Group",
			metricdata.DimensionMedium:       "GA4 - Medium",
			metricdata.DimensionSource:       "GA4 - Source",
		},
	}
)

func init() {
	functions.HTTP("GenerateSheet", generateSheet)
}

func generateSheet(w http.ResponseWriter, r *http.Request) {
	numDataTasks := 3
	wg := sync.WaitGroup{}
	wg.Add(numDataTasks)
	dataOut := make(chan map[string][][]byte, numDataTasks)
	errs := make(chan error, numDataTasks)
	go runDataTask(&wg, metricdata.DimensionChannelGroup, dataOut, errs)
	go runDataTask(&wg, metricdata.DimensionMedium, dataOut, errs)
	go runDataTask(&wg, metricdata.DimensionSource, dataOut, errs)
	wg.Wait()
	if len(errs) > 0 {
		e := <-errs
		http.Error(w, e.Error(), http.StatusInternalServerError)
		return
	}
	allData := []map[string][][]byte{}
	for i := 0; i < numDataTasks; i++ {
		allData = append(allData, <-dataOut)
	}

	ssUrl, err := pushToSheet(r.Context(), flattenMaps(allData...))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte("<html><body><h3>Finished generating spreadsheet...</h3><br /><a href=\"" + ssUrl + "\">Click here...</a></body></html>"))
}

func runDataTask(wg *sync.WaitGroup, metricDimension string, out chan<- map[string][][]byte, errs chan<- error) {
	defer wg.Done()
	d, err := getMetricData(metricDimension)
	if err != nil {
		errs <- err
		return
	}
	out <- map[string][][]byte{metricDimension: d}
}

func getMetricData(dimName string) ([][]byte, error) {
	baseUrl := dataSvcEndpoint + "?srcs=ga,ga4&metrictypes=totalrevenue&dimensions=" + dimName
	starts, ends := generateMonthlyDates()
	urls := []string{}
	for i := range starts {
		urls = append(urls, fmt.Sprintf(baseUrl+"&start=%s&end=%s", starts[i], ends[i]))
	}

	returnData := [][]byte{}
	for _, u := range urls {
		resp, err := http.Get(u)
		if err != nil {
			return returnData, errors.New("generategooglesheet.getMetricData error making http request: " + err.Error())
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return returnData, errors.New("generategooglesheet.getMetricData error reading http response: " + err.Error())
		}
		if resp.StatusCode != 200 {
			return returnData, fmt.Errorf("generategooglesheet.getMetricData error non OK response code (%d)", resp.StatusCode)
		}
		returnData = append(returnData, b)
	}
	return returnData, nil
}

func pushToSheet(ctx context.Context, data map[string][][]byte) (string, error) {
	svc, err := sheets.NewService(ctx)
	if err != nil {
		return "", errors.New("generategooglesheet.pushToSheet error when creating sheets service: " + err.Error())
	}

	ss, err := svc.Spreadsheets.Get(spreadsheetID).Do()
	if err != nil {
		return "", errors.New("generategooglesheet.pushToSheet error when getting sheet: " + err.Error())
	}

	for dim, dat := range data {
		gaTab := tabMap[gaKey][dim]
		ga4Tab := tabMap[ga4Key][dim]

		_, err = svc.Spreadsheets.Values.BatchClear(spreadsheetID, &sheets.BatchClearValuesRequest{
			Ranges: []string{
				gaTab + "!A1:Z100",
				ga4Tab + "!A1:Z100",
			},
		}).Do()
		if err != nil {
			return "", errors.New("generategooglesheet.pushToSheet error when clearing old values: " + err.Error())
		}

		gaMetrics, err := getTotalRevenueMetrics(dat, gaKey)
		if err != nil {
			return "", err
		}
		ga4Metrics, err := getTotalRevenueMetrics(dat, ga4Key)
		if err != nil {
			return "", err
		}

		err = updateTab(gaMetrics, gaTab, svc)
		if err != nil {
			return "", err
		}
		err = updateTab(ga4Metrics, ga4Tab, svc)
		if err != nil {
			return "", err
		}
	}
	return ss.SpreadsheetUrl, nil
}

func updateTab(metrics []metricdata.Metric, tabName string, svc *sheets.Service) error {
	monthCatMap, months, categories := extractToMonthAndCategory(metrics)
	numColumns := len(months) + 1
	numRows := len(categories) + 1
	vr := make([][]interface{}, numColumns)
	for i := 0; i < numColumns; i++ {
		vr[i] = make([]interface{}, numRows)
		if i == 0 {
			for k, catName := range categories {
				vr[i][k+1] = catName
			}
			continue
		}
		for j := 0; j < numRows; j++ {
			if j == 0 {
				vr[i][j] = months[i-1]
			} else {
				key := vr[i][0].(string) + vr[0][j].(string)
				vr[i][j] = monthCatMap[key]
			}
		}
	}
	writeRange := fmt.Sprintf("%s!A1:%c%d", tabName, numColumns+65, numRows)
	valueRange := &sheets.ValueRange{
		Values:         vr,
		MajorDimension: "COLUMNS",
	}

	_, err := svc.Spreadsheets.Values.Update(spreadsheetID, writeRange, valueRange).ValueInputOption("RAW").Do()
	if err != nil {
		return errors.New("generategooglesheet.updateTab error when writing to sheet: " + err.Error())
	}
	return nil
}

func getTotalRevenueMetrics(data [][]byte, src string) ([]metricdata.Metric, error) {
	trm := []metricdata.Metric{}
	for _, d := range data {
		mcs := []metricdata.Metric{}
		err := json.Unmarshal(d, &mcs)
		if err != nil {
			return nil, errors.New("generategooglesheet.getTotalRevenueMetrics error when unmarshaling data: " + err.Error())
		}
		for _, mc := range mcs {
			if mc.Name == "totalRevenue" && mc.Source == src {
				trm = append(trm, mc)
			}
		}
	}
	return trm, nil
}

func generateMonthlyDates() ([]string, []string) {
	startDate := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Now()

	starts, ends := []string{}, []string{}
	for m := startDate; m.Before(endDate.AddDate(0, 1, 0)); m = m.AddDate(0, 1, 0) {
		now := time.Now()
		if m.After(now) {
			break
		}
		end := m.AddDate(0, 1, -1)
		if end.After(now) {
			end = now
		}
		starts = append(starts, m.Format(dfmt))
		ends = append(ends, end.Format(dfmt))
	}
	return starts, ends
}

func getMonthName(m *metricdata.Metric) (string, error) {
	if len(m.Start) > 0 {
		dt, err := time.Parse(dfmt, m.Start)
		if err != nil {
			return "", err
		}
		return dt.Month().String(), nil
	}
	return "", errors.New("start date of metric was empty")
}

func extractToMonthAndCategory(metrics []metricdata.Metric) (map[string]string, []string, []string) {
	mcm := make(map[string]string)
	mm := make(map[string]bool)
	cm := make(map[string]bool)
	for _, m := range metrics {
		mn, err := getMonthName(&m)
		if err != nil {
			log.Printf("warning - metric omitted: %v: %v\n", m, err)
			continue
		}
		mm[mn] = true
		cm[m.Dimension] = true
		key := mn + m.Dimension
		mcm[key] = m.Value
	}

	months := toSliceFromKeys(mm)
	sort.Slice(months, func(i, j int) bool {
		return monthMap[months[i]] < monthMap[months[j]]
	})
	categories := toSliceFromKeys(cm)
	sort.Slice(categories, func(i, j int) bool {
		return categories[i] < categories[j]
	})
	return mcm, months, categories
}

func toSliceFromKeys[T comparable, Y any](m map[T]Y) []T {
	r := []T{}
	for k := range m {
		r = append(r, k)
	}
	return r
}

func flattenMaps[T comparable, Y any](maps ...map[T]Y) map[T]Y {
	result := make(map[T]Y)
	for _, m := range maps {
		for key, value := range m {
			if _, ok := result[key]; ok {
				log.Printf("warning: key [%v] already exists, values will be overwritten", key)
			}
			result[key] = value
		}
	}
	return result
}
