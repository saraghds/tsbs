package influxdb3

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/pkg/query"
)

// IoT produces InfluxDB3-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// NewIoT makes an IoT object ready to generate Queries.
func NewIoT(start, end time.Time, scale int, g *BaseGenerator) *IoT {
	c, err := iot.NewCore(start, end, scale)
	databases.PanicIfErr(err)
	return &IoT{
		Core:          c,
		BaseGenerator: g,
	}
}

func (i *IoT) columnSelect(column string) string {
	return column
}

func (i *IoT) withAlias(column string) string {
	return fmt.Sprintf("%s AS %s", i.columnSelect(column), column)
}

func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("'%s'", s))
	}
	return fmt.Sprintf("name IN (%s)", strings.Join(nameClauses, ","))
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	databases.PanicIfErr(err)
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	sql := fmt.Sprintf(`SELECT name, driver, latitude, longitude 
		FROM readings 
		WHERE %s
		ORDER BY time 
		LIMIT 1`,
		i.getTruckWhereString(nTrucks))

	humanLabel := "InfluxDB3 last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	sql := fmt.Sprintf(`SELECT longitude, latitude
		FROM readings
		WHERE name IS NOT NULL
		AND fleet = '%s'
		ORDER BY time DESC LIMIT 1`,
		i.GetRandomFleet())

	humanLabel := "InfluxDB3 last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	sql := fmt.Sprintf(`SELECT name, driver, fuel_state 
		FROM diagnostics 
		WHERE fuel_state < 0.1 
		AND fleet = '%s'
		ORDER BY time DESC LIMIT 1`,
		i.GetRandomFleet())

	humanLabel := "InfluxDB3 trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	sql := fmt.Sprintf(`SELECT name, driver, current_load, load_capacity 
		FROM (SELECT  current_load, load_capacity 
			FROM diagnostics WHERE fleet = '%s'
			GROUP BY name, driver 
			ORDER BY time DESC 
			LIMIT 1) 
		WHERE current_load >= 0.9 * load_capacity 
		ORDER BY time DESC`,
		i.GetRandomFleet())

	humanLabel := "InfluxDB3 trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	sql := fmt.Sprintf(`SELECT name, driver 
		FROM readings 
		WHERE time >= '%s' AND time < '%s'
		AND fleet = '%s'
		GROUP BY 1, 2  
		HAVING avg(velocity) < 1`,
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt),
		i.GetRandomFleet())
	humanLabel := "InfluxDB3 stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	sql := fmt.Sprintf(`SELECT name, driver
		FROM (
			SELECT name, driver, %s AS ten_minutes
			FROM readings 
			WHERE time >= '%s' AND time < '%s'
			GROUP BY ten_minutes
			HAVING avg(velocity) > 1 
			ORDER BY ten_minutes
		)
		WHERE fleet = '%s'
		GROUP BY name, driver
		HAVING count(ten_minutes) > %d`,
		getNonTimeBucket("time", 10*time.Minute),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt),
		i.GetRandomFleet(),
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "InfluxDB3 trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	sql := fmt.Sprintf(`SELECT name, driver
		FROM (
			SELECT name, driver, %s AS ten_minutes 
			FROM readings 
			WHERE time >= '%s' AND time < '%s'
			GROUP BY ten_minutes
			HAVING avg(velocity) > 1 
			ORDER BY ten_minutes
		)
		WHERE fleet = '%s'
		GROUP BY name, driver 
		HAVING count(ten_minutes) > %d`,
		getNonTimeBucket("time", 10*time.Minute),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt),
		i.GetRandomFleet(),
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 35 mins per hour.
		tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "InfluxDB3 trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	sql := `SELECT fleet, avg(fuel_consumption) AS avg_fuel_consumption, 
		avg(nominal_fuel_consumption) AS projected_fuel_consumption
		FROM readings
		WHERE velocity > 1 
		GROUP BY fleet`

	humanLabel := "InfluxDB3 average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	sql := fmt.Sprintf(`WITH ten_minute_driving_sessions
		AS (
			SELECT %s AS ten_minutes
			FROM readings
			GROUP BY ten_minutes
			HAVING avg(velocity) > 1
			), daily_total_session
		AS (
			SELECT %s AS day, count(*) / 6 AS hours
			FROM ten_minute_driving_sessions
			GROUP BY day
			)
		SELECT fleet, name, driver, avg(hours) AS avg_daily_hours
		FROM daily_total_session
		GROUP BY fleet, name, driver`,
		getNonTimeBucket("time", 10*time.Minute),
		getNonTimeBucket("ten_minutes", 24*time.Hour))

	humanLabel := "InfluxDB3 average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	sql := fmt.Sprintf(`WITH driver_status
		AS (
			SELECT name, %s AS ten_minutes, avg(velocity) > 5 AS driving
			FROM readings
			GROUP BY ten_minutes
			ORDER BY ten_minutes
			), driver_status_change
		AS (
			SELECT name, ten_minutes AS start, lead(ten_minutes) OVER (ORDER BY ten_minutes) AS stop, driving
			FROM (
				SELECT ten_minutes, driving, lag(driving) OVER (ORDER BY ten_minutes) AS prev_driving
				FROM driver_status
				) x
			WHERE x.driving <> x.prev_driving
			)
		SELECT name, %s AS day, avg(age(stop, start)) AS duration
		FROM driver_status_change
		WHERE driving = true
		GROUP BY name, day
		ORDER BY name, day`,
		getNonTimeBucket("time", 10*time.Minute),
		getNonTimeBucket("start", 24*time.Hour))

	humanLabel := "InfluxDB3 average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	sql := `SELECT fleet, model, load_capacity, avg(avg_load / load_capacity) AS avg_load_percentage
		FROM (
			SELECT fleet, model, load_capacity, avg(current_load) AS avg_load
			FROM diagnostics
			GROUP BY fleet, model, load_capacity
		)
		GROUP BY fleet, model, load_capacity`

	humanLabel := "InfluxDB3 average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	sql := fmt.Sprintf(`SELECT fleet, model, day, sum(ten_mins_per_day) / 144 AS daily_activity
		FROM (
			SELECT %s AS day, %s AS ten_minutes, count(*) AS ten_mins_per_day
			FROM diagnostics
			GROUP BY day, ten_minutes
			HAVING avg(STATUS) < 1
		)
		GROUP BY fleet, model, day
		ORDER BY day`,
		getNonTimeBucket("time", 24*time.Hour),
		getNonTimeBucket("time", 10*time.Minute))

	humanLabel := "InfluxDB3 daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.ReadingsTableName, sql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	sql := fmt.Sprintf(`WITH breakdown_per_truck_per_ten_minutes
		AS (
			SELECT %s AS ten_minutes, count(STATUS = 0) / count(*) >= 0.5 AS broken_down
			FROM diagnostics
			GROUP BY ten_minutes
			), breakdowns_per_truck
		AS (
			SELECT ten_minutes, broken_down, lead(broken_down) OVER (ORDER BY ten_minutes) AS next_broken_down
			FROM breakdown_per_truck_per_ten_minutes
			)
		SELECT model, count(*)
		FROM breakdowns_per_truck
		WHERE broken_down = false AND next_broken_down = true
		GROUP BY model`,
		getNonTimeBucket("time", 10*time.Minute))

	humanLabel := "InfluxDB3 truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, iot.DiagnosticsTableName, sql)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}

func getNonTimeBucket(column string, duration time.Duration) string {
	durationSeconds := int(duration.Seconds())
	return fmt.Sprintf("to_timestamp(((extract(epoch from %s)::int)/%d)*%d)", column, durationSeconds, durationSeconds)
}
