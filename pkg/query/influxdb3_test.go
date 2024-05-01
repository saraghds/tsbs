package query

import "testing"

func TestNewInfluxDB3(t *testing.T) {
	check := func(iq *InfluxDB3) {
		testValidNewQuery(t, iq)
		if got := len(iq.SqlQuery); got != 0 {
			t.Errorf("new query has non-0 sql query: got %d", got)
		}
	}
	iq := NewInfluxDB3()
	check(iq)
	iq.HumanLabel = []byte("foo")
	iq.HumanDescription = []byte("bar")
	iq.SqlQuery = []byte("SELECT * FROM *")
	iq.SetID(1)
	if got := string(iq.HumanLabelName()); got != "foo" {
		t.Errorf("incorrect label name: got %s", got)
	}
	if got := string(iq.HumanDescriptionName()); got != "bar" {
		t.Errorf("incorrect desc: got %s", got)
	}
	iq.Release()

	// Since we use a pool, check that the next one is reset
	iq = NewInfluxDB3()
	check(iq)
	iq.Release()
}

func TestInfluxDB3SetAndGetID(t *testing.T) {
	for i := 0; i < 2; i++ {
		q := NewInfluxDB3()
		testSetAndGetID(t, q)
		q.Release()
	}
}
