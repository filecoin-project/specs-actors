package builtin

// The duration of a chain epoch.
// This is used for deriving epoch-denominated periods that are more naturally expressed in clock time.
const EpochDurationSeconds = 25
const SecondsInHour = 3600
const SecondsInDay = 86400
const SecondsInYear = 31556925
const EpochsInHour = SecondsInHour / EpochDurationSeconds
const EpochsInDay = SecondsInDay / EpochDurationSeconds
const EpochsInYear = SecondsInYear / EpochDurationSeconds