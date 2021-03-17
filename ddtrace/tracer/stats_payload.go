// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

//go:generate msgp -unexported -marshal=false -o=stats_payload_msgp.go -tests=false

package tracer

type statsPayload struct {
	Hostname string
	Env      string
	Version  string
	Stats    []statsBucket
}

type statsBucket struct {
	Start    uint64
	Duration uint64
	Stats    []groupedStats
}

type groupedStats struct {
	Service        string `json:"service,omitempty"`
	Name           string `json:"name,omitempty"`
	Resource       string `json:"resource,omitempty"`
	HTTPStatusCode uint32 `json:"HTTP_status_code,omitempty"`
	Type           string `json:"type,omitempty"`
	DBType         string `json:"DB_type,omitempty"`
	Hits           uint64 `json:"hits,omitempty"`
	Errors         uint64 `json:"errors,omitempty"`
	Duration       uint64 `json:"duration,omitempty"`
	OkSummary      []byte `json:"okSummary,omitempty"`
	ErrorSummary   []byte `json:"errorSummary,omitempty"`
	Synthetics     bool   `json:"synthetics,omitempty"`
	TopLevelHits   uint64 `json:"topLevelHits,omitempty"`
}
