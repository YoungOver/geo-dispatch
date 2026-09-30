package wire

import "testing"

func TestRoundTrip(t *testing.T) {
	in := []Ping{{ID: 1, Lat: 55.755812, Lon: 37.617311, Status: 1}, {ID: 1 << 40, Lat: -33.8688, Lon: 151.2093, Status: 2}}
	var b []byte
	for _, p := range in {
		b = Append(b, p)
	}
	var out []Ping
	if err := Decode(b, func(p Ping) { out = append(out, p) }); err != nil {
		t.Fatal(err)
	}
	for i := range in {
		if out[i].ID != in[i].ID || out[i].Status != in[i].Status || abs(out[i].Lat-in[i].Lat) > 1e-6 || abs(out[i].Lon-in[i].Lon) > 1e-6 {
			t.Fatalf("ping %d: %+v != %+v", i, out[i], in[i])
		}
	}
	if Decode(b[:5], func(Ping) {}) != ErrShort {
		t.Fatal("truncated datagram must be rejected")
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
