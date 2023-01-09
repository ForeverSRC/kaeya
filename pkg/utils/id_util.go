package utils

import (
	"strings"

	"github.com/google/uuid"
)

func ID() string {
	id, _ := uuid.NewUUID()
	return strings.ReplaceAll(id.String(), "-", "")
}
