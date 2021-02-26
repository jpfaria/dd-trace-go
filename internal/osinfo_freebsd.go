// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package internal

import (
	"os/exec"
	"runtime"
	"strings"
)

const (
	unknown = "unknown"
)

// OSName detects name of the operating system.
func OSName() string {
	return runtime.GOOS
}

// OSVersion detects version of the operating system.
func OSVersion() string {
	out, err := exec.Command("uname", "-r").Output()
	if err != nil {
		return unknown
	}
	return strings.Split(string(out), "-")[0]
}