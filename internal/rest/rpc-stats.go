// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package rest

import (
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/minio/minio/internal/stats"
)

var globalStats = struct {
	errs uint64

	tcpDialErrs    uint64
	tcpConnectTime stats.LastMinuteLatency

	dnsErrs  uint64
	dnsTimes stats.LastMinuteLatency

	tlsErrs  uint64
	tlsTimes stats.LastMinuteLatency
}{}

// RPCStats holds information about the DHCP/TCP metrics and errors
type RPCStats struct {
	Errs uint64

	DialLastMin     uint64
	DialAvgDuration uint64
	DialErrs        uint64

	DNSLastMin     uint64
	DNSErrs        uint64
	DNSAvgDuration uint64

	TLSLastMin     uint64
	TLSErrs        uint64
	TLSAvgDuration uint64
}

// GetRPCStats returns RPC stats, include calls errors and dhcp/tcp metrics
func GetRPCStats() RPCStats {
	s := RPCStats{
		Errs:     atomic.LoadUint64(&globalStats.errs),
		DialErrs: atomic.LoadUint64(&globalStats.tcpDialErrs),
		DNSErrs:  atomic.LoadUint64(&globalStats.dnsErrs),
		TLSErrs:  atomic.LoadUint64(&globalStats.tlsErrs),
	}

	st := globalStats.tcpConnectTime.GetTotal()
	s.DialAvgDuration = uint64(st.AvgTime())
	s.DialLastMin = uint64(st.N)

	st = globalStats.dnsTimes.GetTotal()
	s.DNSAvgDuration = uint64(st.AvgTime())
	s.DNSLastMin = uint64(st.N)

	st = globalStats.tlsTimes.GetTotal()
	s.TLSAvgDuration = uint64(st.AvgTime())
	s.TLSLastMin = uint64(st.N)

	return s
}

// Return a function which update the global stats related to tcp connections
func setupReqStatsUpdate(req *http.Request) (*http.Request, func()) {
	var dialStart, dialEnd int64
	var dnsStart, dnsEnd int64
	var tlsStart, tlsEnd int64

	trace := &httptrace.ClientTrace{
		ConnectStart: func(network, addr string) {
			atomic.StoreInt64(&dialStart, time.Now().UnixNano())
		},
		ConnectDone: func(network, addr string, err error) {
			if err == nil {
				atomic.StoreInt64(&dialEnd, time.Now().UnixNano())
			}
		},
		DNSStart: func(_ httptrace.DNSStartInfo) {
			atomic.StoreInt64(&dnsStart, time.Now().UnixNano())
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			if info.Err != nil {
				atomic.AddUint64(&globalStats.dnsErrs, 1)
			}
			atomic.StoreInt64(&dnsEnd, time.Now().UnixNano())
		},
		TLSHandshakeStart: func() {
			atomic.StoreInt64(&tlsStart, time.Now().UnixNano())
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			if err != nil {
				atomic.AddUint64(&globalStats.tlsErrs, 1)
			}
			atomic.StoreInt64(&tlsEnd, time.Now().UnixNano())
		},
	}

	return req.WithContext(httptrace.WithClientTrace(req.Context(), trace)), func() {
		if ds := atomic.LoadInt64(&dialStart); ds > 0 {
			if de := atomic.LoadInt64(&dialEnd); de == 0 {
				atomic.AddUint64(&globalStats.tcpDialErrs, 1)
			} else if de >= ds {
				globalStats.tcpConnectTime.Add(time.Nanosecond * time.Duration(dialEnd-dialStart))
			}
		}
		if ds := atomic.LoadInt64(&dnsStart); ds > 0 {
			if de := atomic.LoadInt64(&dnsEnd); de == 0 {
				atomic.AddUint64(&globalStats.dnsErrs, 1)
			} else if de >= ds {
				globalStats.dnsTimes.Add(time.Nanosecond * time.Duration(dnsEnd-dnsStart))
			}
		}
		if ds := atomic.LoadInt64(&tlsStart); ds > 0 {
			if de := atomic.LoadInt64(&tlsEnd); de == 0 {
				atomic.AddUint64(&globalStats.tlsErrs, 1)
			} else if de >= ds {
				globalStats.tlsTimes.Add(time.Nanosecond * time.Duration(tlsEnd-tlsStart))
			}
		}
	}
}
