package config

import (
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/mcuadros/go-defaults"
	"github.com/spf13/viper"
)

type KaeyaConfig struct {
	Log     LogConfig     `mapstructure:"log" validate:"required"`
	Storage StorageConfig `mapstructure:"storage" validate:"required"`
}

type StorageConfig struct {
	Path    string           `mapstructure:"path"`
	System  string           `mapstructure:"system" default:"segment" validate:"oneof=fs segment"`
	Codec   string           `mapstructure:"codec" default:"csv" validate:"required"`
	Segment SegmentSysConfig `mapstructure:"segment"`
}

type LogConfig struct {
	Level string `mapstructure:"level" default:"info" validate:"oneof=debug info warn error"`
}
type SegmentSysConfig struct {
	BufferSize      string `mapstructure:"buffer_size"`
	RefreshInterval string `mapstructure:"refresh_interval"`
	FlushInterval   string `mapstructure:"flush_interval"`
	MergeInterval   string `mapstructure:"merge_interval"`
	MergeFloor      string `mapstructure:"merge_floor"`
}

func ProvideConfig() (KaeyaConfig, error) {
	sourceConfig := viper.New()
	sourceConfig.AddConfigPath("config")

	err := sourceConfig.MergeInConfig()
	if err != nil {
		return KaeyaConfig{}, err
	}

	var conf KaeyaConfig
	defaults.SetDefaults(&conf)

	err = sourceConfig.Unmarshal(&conf)

	validate := validator.New()
	err = validate.Struct(&conf)
	if err != nil {
		return KaeyaConfig{}, err
	}

	if conf.Storage.Path == "" {
		conf.Storage.Path = setDefaultPath()
	}

	return conf, nil

}

func setDefaultPath() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	return wd
}
