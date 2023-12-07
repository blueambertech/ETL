package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/blueambertech/ETL/pkg/dataextract"
)

func RegisterHandlers() {
	http.HandleFunc("/getdata", getDataHandler)
}

func getDataHandler(w http.ResponseWriter, r *http.Request) {
	// Validate fields
	qs := r.URL.Query()

	srcs, ok := qs["srcs"]
	if !ok {
		http.Error(w, "srcs param missing", http.StatusBadRequest)
		return
	}
	start, ok := qs["start"]
	if !ok {
		http.Error(w, "start param missing", http.StatusBadRequest)
		return
	}
	_, err := time.Parse(time.DateOnly, start[0])
	if err != nil {
		http.Error(w, "error when parsing start date: "+err.Error(), http.StatusBadRequest)
		return
	}
	end, ok := qs["end"]
	if !ok {
		http.Error(w, "end param missing", http.StatusBadRequest)
		return
	}
	_, err = time.Parse(time.DateOnly, end[0])
	if err != nil {
		http.Error(w, "error when parsing end date: "+err.Error(), http.StatusBadRequest)
		return
	}
	mt, ok := qs["metrictypes"]
	if !ok {
		http.Error(w, "metrictypes param missing", http.StatusBadRequest)
		return
	}
	dim, ok := qs["dimensions"]
	if !ok {
		http.Error(w, "dimensions param missing", http.StatusBadRequest)
		return
	}

	srcList := strings.Split(srcs[0], ",")
	metricList := strings.Split(mt[0], ",")
	dimensionList := strings.Split(dim[0], ",")
	data := dataextract.GetData(srcList, metricList, start, end, dimensionList)
	j, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "couldn't marshal metrics collection to JSON", http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	w.Write(j)
}
