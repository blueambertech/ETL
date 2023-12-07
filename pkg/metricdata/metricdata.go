package metricdata

const (
	TotalRevenue = "totalrevenue"

	DimensionMedium       = "medium"
	DimensionSource       = "source"
	DimensionChannelGroup = "channelgroup"
)

type Metric struct {
	Source    string `json:"src"`
	Name      string `json:"name"`
	Value     string `json:"value"`
	Dimension string `json:"dimension"`
	Start     string `json:"start"`
	End       string `json:"end"`
}
