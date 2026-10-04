package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

// CronParser is the v1 schedule dialect: standard 5-field cron (minute
// first) with @descriptors. There is no seconds field.
func CronParser() cron.Parser {
	return cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
}

// ResolveCronSpec validates a crontab + timezone pair and returns the
// parse-ready spec plus the effective timezone. An empty timezone defaults
// to UTC; an inline TZ=/CRON_TZ= prefix wins over the timezone field. A bad
// expression, zone, or prefix shape yields ErrInvalid.
func ResolveCronSpec(crontab, timezone string) (string, string, error) {
	crontab = strings.TrimSpace(crontab)
	if crontab == "" {
		return "", "", fmt.Errorf("%w: crontab is required", ErrInvalid)
	}
	if zone, schedule, ok := splitInlineTimezone(crontab); ok {
		if zone == "" || schedule == "" {
			return "", "", fmt.Errorf("%w: invalid crontab: a TZ prefix needs a zone and a schedule", ErrInvalid)
		}
		if _, err := time.LoadLocation(zone); err != nil {
			return "", "", fmt.Errorf("%w: invalid timezone %q", ErrInvalid, zone)
		}
		if _, err := CronParser().Parse(crontab); err != nil {
			return "", "", fmt.Errorf("%w: invalid crontab: %v", ErrInvalid, err)
		}
		return crontab, zone, nil
	}
	tz := strings.TrimSpace(timezone)
	if tz == "" {
		tz = "UTC"
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return "", "", fmt.Errorf("%w: invalid timezone %q", ErrInvalid, tz)
	}
	spec := "CRON_TZ=" + tz + " " + crontab
	if _, err := CronParser().Parse(spec); err != nil {
		return "", "", fmt.Errorf("%w: invalid crontab: %v", ErrInvalid, err)
	}
	return spec, tz, nil
}

// splitInlineTimezone splits a TZ=/CRON_TZ= prefixed spec into its zone and
// schedule. ok is false when there is no prefix. The split guards the shape
// before the cron parser sees it: a bare prefix with no schedule would
// panic the parser's zone slice instead of returning an error.
func splitInlineTimezone(crontab string) (zone, schedule string, ok bool) {
	rest, ok := strings.CutPrefix(crontab, "CRON_TZ=")
	if !ok {
		rest, ok = strings.CutPrefix(crontab, "TZ=")
		if !ok {
			return "", "", false
		}
	}
	zone, schedule, _ = strings.Cut(rest, " ")
	return strings.TrimSpace(zone), strings.TrimSpace(schedule), true
}
