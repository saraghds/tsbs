package constants

// Formats supported for generation
const (
	FormatCassandra       = "cassandra"
	FormatClickhouse      = "clickhouse"
	FormatInflux          = "influx"
	FormatInfluxDB3       = "influxdb3"
	FormatMongo           = "mongo"
	FormatSiriDB          = "siridb"
	FormatTimescaleDB     = "timescaledb"
	FormatAkumuli         = "akumuli"
	FormatCrateDB         = "cratedb"
	FormatPrometheus      = "prometheus"
	FormatVictoriaMetrics = "victoriametrics"
	FormatTimestream      = "timestream"
	FormatQuestDB         = "questdb"
)

func SupportedFormats() []string {
	return []string{
		FormatCassandra,
		FormatClickhouse,
		FormatInflux,
		FormatInfluxDB3,
		FormatMongo,
		FormatSiriDB,
		FormatTimescaleDB,
		FormatAkumuli,
		FormatCrateDB,
		FormatPrometheus,
		FormatVictoriaMetrics,
		FormatTimestream,
		FormatQuestDB,
	}
}
