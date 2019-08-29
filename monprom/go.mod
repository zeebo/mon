module github.com/zeebo/mon/monprom

go 1.12

replace github.com/zeebo/mon => ../

require (
	github.com/prometheus/client_golang v1.1.0
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4
	github.com/prometheus/common v0.6.0
	github.com/zeebo/mon v0.0.0-20190625213908-a1fc66af882c
)
