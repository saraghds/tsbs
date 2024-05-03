package influxdb3

import (
	"math/rand"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/query"
)

const (
	testScale = 10
)

type testCase struct {
	desc               string
	input              int
	fail               bool
	failMsg            string
	expectedHumanLabel string
	expectedHumanDesc  string
	expectedSQLQuery   string
}

func TestLastLocByTruck(t *testing.T) {
	cases := []testCase{
		{
			desc:    "zero trucks",
			input:   0,
			fail:    true,
			failMsg: "number of trucks cannot be < 1; got 0",
		},
		{
			desc:    "more trucks than scale",
			input:   2 * testScale,
			fail:    true,
			failMsg: "number of trucks (20) larger than total trucks. See --scale (10)",
		},
		{
			desc:  "one truck",
			input: 1,

			expectedHumanLabel: "InfluxDB3 last location by specific truck",
			expectedHumanDesc:  "InfluxDB3 last location by specific truck: random    1 trucks",
			expectedSQLQuery: `SELECT name, driver, latitude, longitude 
		FROM readings 
		WHERE name IN ('truck_5')
		ORDER BY time 
		LIMIT 1`,
		},
		{
			desc:  "three truck",
			input: 3,

			expectedHumanLabel: "InfluxDB3 last location by specific truck",
			expectedHumanDesc:  "InfluxDB3 last location by specific truck: random    3 trucks",
			expectedSQLQuery: `SELECT name, driver, latitude, longitude 
		FROM readings 
		WHERE name IN ('truck_9','truck_3','truck_5')
		ORDER BY time 
		LIMIT 1`,
		},
	}

	testFunc := func(i *IoT, c testCase) query.Query {
		q := i.GenerateEmptyQuery()
		i.LastLocByTruck(q, c.input)
		return q
	}

	runTestCases(t, testFunc, time.Now(), time.Now(), cases)
}

func TestLastLocPerTruck(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 last location per truck",
			expectedHumanDesc:  "InfluxDB3 last location per truck",
			expectedSQLQuery: `SELECT longitude, latitude
		FROM readings
		WHERE name IS NOT NULL
		AND fleet = 'South'
		ORDER BY time DESC LIMIT 1`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.LastLocPerTruck(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestTrucksWithLowFuel(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 trucks with low fuel",
			expectedHumanDesc:  "InfluxDB3 trucks with low fuel: under 10 percent",
			expectedSQLQuery: `SELECT name, driver, fuel_state 
		FROM diagnostics 
		WHERE name IS NOT NULL
		AND fuel_state < 0.1 
		AND fleet = 'South'
		ORDER BY time DESC LIMIT 1`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.TrucksWithLowFuel(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestTrucksWithHighLoad(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 trucks with high load",
			expectedHumanDesc:  "InfluxDB3 trucks with high load: over 90 percent",
			expectedSQLQuery: `SELECT name, driver, current_load, load_capacity, time
		FROM diagnostics
		WHERE name IS NOT NULL 
		AND fleet = 'South'
		AND current_load >= 0.9 * load_capacity 
		ORDER BY time DESC`,
		},
	}

	for _, c := range cases {
		rand.Seed(123)
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Now(), time.Now(), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		g.TrucksWithHighLoad(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestStationaryTrucks(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 stationary trucks",
			expectedHumanDesc:  "InfluxDB3 stationary trucks: with low avg velocity in last 10 minutes",
			expectedSQLQuery: `SELECT name, driver 
		FROM readings 
		WHERE time >= '1970-01-01 00:36:22.646325 +0000' AND time < '1970-01-01 00:46:22.646325 +0000'
		AND fleet = 'West'
		AND name IS NOT NULL
		GROUP BY 1, 2  
		HAVING avg(velocity) < 1`,
		},
	}

	for _, c := range cases {
		b := &BaseGenerator{}
		g := NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(time.Hour), 10, b)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.StationaryTrucks(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestTrucksWithLongDrivingSessions(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 trucks with longer driving sessions",
			expectedHumanDesc:  "InfluxDB3 trucks with longer driving sessions: stopped less than 20 mins in 4 hour period",
			expectedSQLQuery: `SELECT name, driver
		FROM (
			SELECT name, driver, to_timestamp(((extract(epoch from time)::int)/600)*600) AS ten_minutes
			FROM readings 
			WHERE time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-01 04:16:22.646325 +0000'
			AND fleet = 'West'
			AND name IS NOT NULL
			GROUP BY name, driver, ten_minutes
			HAVING avg(velocity) > 1 
			ORDER BY ten_minutes
		)
		GROUP BY name, driver
		HAVING count(ten_minutes) > 22`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(6*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TrucksWithLongDrivingSessions(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestTrucksWithLongDailySessions(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 trucks with longer daily sessions",
			expectedHumanDesc:  "InfluxDB3 trucks with longer daily sessions: drove more than 10 hours in the last 24 hours",
			expectedSQLQuery: `SELECT name, driver
		FROM (
			SELECT name, driver, to_timestamp(((extract(epoch from time)::int)/600)*600) AS ten_minutes 
			FROM readings 
			WHERE time >= '1970-01-01 00:16:22.646325 +0000' AND time < '1970-01-02 00:16:22.646325 +0000'
			AND fleet = 'West'
			AND name IS NOT NULL
			GROUP BY name, driver, ten_minutes
			HAVING avg(velocity) > 1 
			ORDER BY ten_minutes
		)
		GROUP BY name, driver 
		HAVING count(ten_minutes) > 60`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TrucksWithLongDailySessions(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestAvgVsProjectedFuelConsumption(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 average vs projected fuel consumption per fleet",
			expectedHumanDesc:  "InfluxDB3 average vs projected fuel consumption per fleet",

			expectedSQLQuery: `SELECT fleet, avg(fuel_consumption) AS avg_fuel_consumption, 
		avg(nominal_fuel_consumption) AS projected_fuel_consumption
		FROM readings
		WHERE velocity > 1 
		AND name IS NOT NULL
		AND nominal_fuel_consumption IS NOT NULL
		AND fleet IS NOT NULL
		GROUP BY fleet`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgVsProjectedFuelConsumption(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestAvgDailyDrivingDuration(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 average driver driving duration per day",
			expectedHumanDesc:  "InfluxDB3 average driver driving duration per day",
			expectedSQLQuery: `WITH ten_minute_driving_sessions
		AS (
			SELECT fleet, name, driver, to_timestamp(((extract(epoch from time)::int)/600)*600) AS ten_minutes
			FROM readings
			GROUP BY fleet, name, driver, ten_minutes
			HAVING avg(velocity) > 1
			), daily_total_session
		AS (
			SELECT fleet, name, driver, to_timestamp(((extract(epoch from ten_minutes)::int)/86400)*86400) AS day, count(*) / 6 AS hours
			FROM ten_minute_driving_sessions
			GROUP BY fleet, name, driver, day
			)
		SELECT fleet, name, driver, avg(hours) AS avg_daily_hours
		FROM daily_total_session
		GROUP BY fleet, name, driver`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgDailyDrivingDuration(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestAvgDailyDrivingSession(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 average driver driving session without stopping per day",
			expectedHumanDesc:  "InfluxDB3 average driver driving session without stopping per day",

			expectedSQLQuery: `WITH driver_status
		AS (
			SELECT name, to_timestamp(((extract(epoch from time)::int)/600)*600) AS ten_minutes, avg(velocity) > 5 AS driving
			FROM readings
			GROUP BY name, ten_minutes
			ORDER BY ten_minutes
			), driver_status_change
		AS (
			SELECT name, ten_minutes AS start, lead(ten_minutes) OVER (ORDER BY ten_minutes) AS stop, driving
			FROM (
				SELECT name, ten_minutes, driving, lag(driving) OVER (ORDER BY ten_minutes) AS prev_driving
				FROM driver_status
				) x
			WHERE x.driving <> x.prev_driving
			)
		SELECT name, to_timestamp(((extract(epoch from start)::int)/86400)*86400) AS day, avg(date_part('nanosecond', stop) - date_part('nanosecond', start)) AS duration
		FROM driver_status_change
		WHERE driving = true
		AND name IS NOT NULL
		GROUP BY name, day
		ORDER BY name, day`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgDailyDrivingSession(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestAvgLoad(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 average load per truck model per fleet",
			expectedHumanDesc:  "InfluxDB3 average load per truck model per fleet",
			expectedSQLQuery: `SELECT fleet, model, load_capacity, avg(avg_load / load_capacity) AS avg_load_percentage
		FROM (
			SELECT fleet, model, load_capacity, avg(current_load) AS avg_load
			FROM diagnostics
			GROUP BY fleet, model, load_capacity
		)
		GROUP BY fleet, model, load_capacity`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.AvgLoad(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestDailyTruckActivity(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 daily truck activity per fleet per model",
			expectedHumanDesc:  "InfluxDB3 daily truck activity per fleet per model",
			expectedSQLQuery: `SELECT fleet, model, day, sum(ten_mins_per_day) / 144 AS daily_activity
		FROM (
			SELECT fleet, model, to_timestamp(((extract(epoch from time)::int)/86400)*86400) AS day, to_timestamp(((extract(epoch from time)::int)/600)*600) AS ten_minutes, count(*) AS ten_mins_per_day
			FROM diagnostics
			GROUP BY fleet, model, day, ten_minutes
			HAVING avg(STATUS) < 1
		)
		GROUP BY fleet, model, day
		ORDER BY day`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.DailyTruckActivity(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestTruckBreakdownFrequency(t *testing.T) {
	cases := []testCase{
		{
			desc: "default to using tags",

			expectedHumanLabel: "InfluxDB3 truck breakdown frequency per model",
			expectedHumanDesc:  "InfluxDB3 truck breakdown frequency per model",
			expectedSQLQuery: `WITH breakdown_per_truck_per_ten_minutes
		AS (
			SELECT model, to_timestamp(((extract(epoch from time)::int)/600)*600) AS ten_minutes, count(STATUS = 0) / count(*) >= 0.5 AS broken_down
			FROM diagnostics
			GROUP BY model, ten_minutes
			), breakdowns_per_truck
		AS (
			SELECT model, ten_minutes, broken_down, lead(broken_down) OVER (ORDER BY ten_minutes) AS next_broken_down
			FROM breakdown_per_truck_per_ten_minutes
			)
		SELECT model, count(*)
		FROM breakdowns_per_truck
		WHERE broken_down = false AND next_broken_down = true
		GROUP BY model`,
		},
	}

	for _, c := range cases {
		b := BaseGenerator{}
		ig, err := b.NewIoT(time.Unix(0, 0), time.Unix(0, 0).Add(25*time.Hour), 10)
		if err != nil {
			t.Fatalf("Error while creating iot generator")
		}

		g := ig.(*IoT)

		q := g.GenerateEmptyQuery()
		rand.Seed(123)
		g.TruckBreakdownFrequency(q)

		verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
	}
}

func TestTenMinutePeriods(t *testing.T) {
	cases := []struct {
		minutesPerHour float64
		duration       time.Duration
		result         int
	}{
		{
			minutesPerHour: 5.0,
			duration:       4 * time.Hour,
			result:         22,
		},
		{
			minutesPerHour: 10.0,
			duration:       24 * time.Hour,
			result:         120,
		},
		{
			minutesPerHour: 0.0,
			duration:       24 * time.Hour,
			result:         144,
		},
		{
			minutesPerHour: 1.0,
			duration:       0 * time.Minute,
			result:         0,
		},
		{
			minutesPerHour: 0.0,
			duration:       0 * time.Minute,
			result:         0,
		},
		{
			minutesPerHour: 1.0,
			duration:       30 * time.Minute,
			result:         2,
		},
	}

	for _, c := range cases {
		if got := tenMinutePeriods(c.minutesPerHour, c.duration); got != c.result {
			t.Errorf("incorrect result for %.2f minutes per hour, duration %s: got %d want %d", c.minutesPerHour, c.duration.String(), got, c.result)
		}
	}

}

func runTestCases(t *testing.T, testFunc func(*IoT, testCase) query.Query, s time.Time, e time.Time, cases []testCase) {
	rand.Seed(123) // Setting seed for testing purposes.

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			b := BaseGenerator{}
			dq, err := b.NewIoT(s, e, testScale)
			if err != nil {
				t.Fatalf("Error while creating devops generator")
			}
			i := dq.(*IoT)

			if c.fail {
				func() {
					defer func() {
						r := recover()
						if r == nil {
							t.Fatalf("did not panic when should")
						}

						if r != c.failMsg {
							t.Fatalf("incorrect fail message: got %s, want %s", r, c.failMsg)
						}
					}()

					testFunc(i, c)
				}()
			} else {
				q := testFunc(i, c)

				verifyQuery(t, q, c.expectedHumanLabel, c.expectedHumanDesc, c.expectedSQLQuery)
			}
		})
	}
}
