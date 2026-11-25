package vip

import (
	"time"

	"github.com/google/uuid"
)

// LevelChangedEvent is published when a user's VIP level changes.
type LevelChangedEvent struct {
	EventID           uuid.UUID `json:"eventId"`
	UserID            uuid.UUID `json:"userId"`
	OldLevel          int       `json:"oldLevel"`
	NewLevel          int       `json:"newLevel"`
	LevelUpType       string    `json:"levelUpType,omitempty"`
	NewLevelName      string    `json:"newLevelName,omitempty"`
	NewLevelGroupName string    `json:"newLevelGroupName,omitempty"`
	Reason            string    `json:"reason"`
	Timestamp         time.Time `json:"timestamp"`
}

// PointsAddedEvent is published when VIP points are credited.
type PointsAddedEvent struct {
	EventID   uuid.UUID `json:"eventId"`
	UserID    uuid.UUID `json:"userId"`
	Points    string    `json:"points"`
	Reason    string    `json:"reason"`
	Timestamp time.Time `json:"timestamp"`
}
