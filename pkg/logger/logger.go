package logger

import (
	"os"

	"github.com/rs/zerolog"
)

var Logger zerolog.Logger

func InitZerolog(level string) error {
	lv, err := zerolog.ParseLevel(level)
	if err != nil {
		return err
	}

	zerolog.SetGlobalLevel(lv)
	Logger = zerolog.New(os.Stderr).Level(lv).With().Timestamp().Logger()
	return nil
}
