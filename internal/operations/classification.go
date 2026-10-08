package operations

import "time"

type Diagnosis string

const (
	DiagnosisManualIntervention Diagnosis = "MANUAL_INTERVENTION"
	DiagnosisConflict           Diagnosis = "CONFLICT"
	DiagnosisDuplicate          Diagnosis = "DUPLICATE_EXECUTION"
	DiagnosisUnavailable        Diagnosis = "CHAIN_UNAVAILABLE"
	DiagnosisStuck              Diagnosis = "STUCK"
	DiagnosisOrdinary           Diagnosis = "ORDINARY"
)

type ClassificationInput struct {
	SourceFinal        bool
	RelayEligible      bool
	DestinationSeen    bool
	ObservationReady   bool
	ManualIntervention bool
	Conflict           bool
	DuplicateExecution bool
	AlreadyStuck       bool
	ExpectedBy         time.Time
	Now                time.Time
}

// Due is inclusive: the message becomes late at exactly expected_by.
func Due(input ClassificationInput) bool {
	return input.SourceFinal && input.RelayEligible && !input.DestinationSeen &&
		input.ObservationReady && !input.ManualIntervention && !input.Conflict &&
		!input.DuplicateExecution && !input.ExpectedBy.IsZero() && !input.Now.Before(input.ExpectedBy)
}

func Diagnose(input ClassificationInput) Diagnosis {
	switch {
	case input.ManualIntervention:
		return DiagnosisManualIntervention
	case input.Conflict:
		return DiagnosisConflict
	case input.DuplicateExecution:
		return DiagnosisDuplicate
	case !input.ObservationReady:
		return DiagnosisUnavailable
	case input.AlreadyStuck || Due(input):
		return DiagnosisStuck
	default:
		return DiagnosisOrdinary
	}
}
