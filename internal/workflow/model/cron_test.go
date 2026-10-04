package model_test

import (
	"errors"
	"testing"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
)

func TestResolveCronSpecDefaultsUTC(t *testing.T) {
	spec, tz, err := model.ResolveCronSpec("*/5 * * * *", "")
	if err != nil {
		t.Fatalf("ResolveCronSpec() error = %v", err)
	}
	if tz != "UTC" {
		t.Errorf("timezone = %q, want UTC", tz)
	}
	if _, err := model.CronParser().Parse(spec); err != nil {
		t.Errorf("resolved spec %q does not parse: %v", spec, err)
	}
}

func TestResolveCronSpecExplicitTimezone(t *testing.T) {
	_, tz, err := model.ResolveCronSpec("0 9 * * *", "America/New_York")
	if err != nil {
		t.Fatalf("ResolveCronSpec() error = %v", err)
	}
	if tz != "America/New_York" {
		t.Errorf("timezone = %q, want America/New_York", tz)
	}
}

func TestResolveCronSpecDescriptors(t *testing.T) {
	for _, crontab := range []string{"@daily", "@hourly", "@weekly", "@monthly", "@yearly", "@every 5m", "@every 1h30m"} {
		spec, _, err := model.ResolveCronSpec(crontab, "")
		if err != nil {
			t.Errorf("ResolveCronSpec(%q) error = %v", crontab, err)
			continue
		}
		if _, err := model.CronParser().Parse(spec); err != nil {
			t.Errorf("resolved spec %q does not parse: %v", spec, err)
		}
	}
}

func TestResolveCronSpecInvalid(t *testing.T) {
	for _, crontab := range []string{"", "   ", "not a cron", "0 * * *", "* * * * * *", "0 * * * * *", "@never", "@every banana"} {
		if _, _, err := model.ResolveCronSpec(crontab, ""); !errors.Is(err, model.ErrInvalid) {
			t.Errorf("ResolveCronSpec(%q) error = %v, want ErrInvalid", crontab, err)
		}
	}
}

func TestResolveCronSpecBadTimezone(t *testing.T) {
	if _, _, err := model.ResolveCronSpec("* * * * *", "Mars/Olympus"); !errors.Is(err, model.ErrInvalid) {
		t.Errorf("error = %v, want ErrInvalid", err)
	}
}

func TestResolveCronSpecInlineTimezone(t *testing.T) {
	spec, tz, err := model.ResolveCronSpec("CRON_TZ=America/New_York 0 9 * * *", "")
	if err != nil {
		t.Fatalf("ResolveCronSpec() error = %v", err)
	}
	if tz != "America/New_York" {
		t.Errorf("timezone = %q, want America/New_York", tz)
	}
	if _, err := model.CronParser().Parse(spec); err != nil {
		t.Errorf("resolved spec %q does not parse: %v", spec, err)
	}

	// The TZ= form is accepted too.
	if _, tz, err := model.ResolveCronSpec("TZ=UTC 0 9 * * *", ""); err != nil || tz != "UTC" {
		t.Errorf("TZ= form: tz = %q, err = %v", tz, err)
	}

	// An inline prefix wins over the timezone field.
	if _, tz, err := model.ResolveCronSpec("CRON_TZ=UTC 0 9 * * *", "America/New_York"); err != nil || tz != "UTC" {
		t.Errorf("inline precedence: tz = %q, err = %v", tz, err)
	}

	// A bad inline zone and a prefix with no schedule are invalid, not panics.
	for _, crontab := range []string{"CRON_TZ=Mars/Olympus 0 9 * * *", "CRON_TZ=UTC", "TZ=UTC"} {
		if _, _, err := model.ResolveCronSpec(crontab, ""); !errors.Is(err, model.ErrInvalid) {
			t.Errorf("ResolveCronSpec(%q) error = %v, want ErrInvalid", crontab, err)
		}
	}
}

func TestCronParserRejectsSecondsField(t *testing.T) {
	if _, err := model.CronParser().Parse("0 */5 * * * *"); err == nil {
		t.Errorf("6-field spec parsed, want an error (v1 has no seconds field)")
	}
}
