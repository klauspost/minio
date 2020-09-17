package cmd

import "time"

type scanStatus uint8

const (
	scanStateNone scanStatus = iota
	scanStateStarted
	scanStateSuccess
	scanStateError
)

//go:generate msgp -file $GOFILE -unexported

type metacache struct {
	id           string     `msg:"id"`
	bucket       string     `msg:"b"`
	root         string     `msg:"root"`
	recursive    bool       `msg:"rec"`
	status       scanStatus `msg:"stat"`
	error        string     `msg:"err"`
	totalObjects string     `msg:"to"`
	started      time.Time  `msg:"st"`
	ended        time.Time  `msg:"end"`
	startedCycle uint64     `msg:"stc"`
	endedCycle   uint64     `msg:"endc"`
	dataVersion  uint8      `msg:"v"`
}
