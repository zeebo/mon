module github.com/zeebo/mon/monprom

go 1.12

require (
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4
	github.com/prometheus/common v0.6.0
	github.com/zeebo/mon v0.0.0-20190829025240-97443e9d2649
)

replace github.com/zeebo/mon => ../
