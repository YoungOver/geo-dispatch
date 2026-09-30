// Package wire defines the compact UDP ping format.
//
// A datagram carries up to 80 pings of 17 bytes each:
//
//	id      uint64  little endian
//	lat     int32   degrees * 1e6
//	lon     int32   degrees * 1e6
//	status  uint8
//
// 17 bytes instead of ~90 bytes of JSON lets one courier app send a position
// every second over a poor mobile link without noticeable traffic.
package wire

import (
	"encoding/binary"
	"errors"
)

const PingSize = 17
const MaxPerDatagram = 80

type Ping struct {
	ID     uint64
	Lat    float64
	Lon    float64
	Status uint8
}

var ErrShort = errors.New("wire: truncated datagram")

func Append(dst []byte, p Ping) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, p.ID)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(int32(p.Lat*1e6)))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(int32(p.Lon*1e6)))
	return append(dst, p.Status)
}

// Decode calls fn for every ping in b.
func Decode(b []byte, fn func(Ping)) error {
	if len(b)%PingSize != 0 {
		return ErrShort
	}
	for len(b) >= PingSize {
		fn(Ping{
			ID:     binary.LittleEndian.Uint64(b),
			Lat:    float64(int32(binary.LittleEndian.Uint32(b[8:]))) / 1e6,
			Lon:    float64(int32(binary.LittleEndian.Uint32(b[12:]))) / 1e6,
			Status: b[16],
		})
		b = b[PingSize:]
	}
	return nil
}
