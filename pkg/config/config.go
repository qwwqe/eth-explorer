package config

import (
	"os"
	"reflect"
	"strconv"

	"github.com/joho/godotenv"
)

func load(c any) error {
	v := reflect.ValueOf(c)
	s := v.Elem()

	for i := 0; i < s.NumField(); i++ {
		sf := s.Type().Field(i)
		f := s.Field(i)
		switch f.Kind() {
		case reflect.String:
			e := sf.Tag.Get("env")
			if e != "" && f.CanSet() {
				f.SetString(os.Getenv(e))
			}
		case reflect.Int:
			e := sf.Tag.Get("env")

			if e != "" && f.CanSet() {
				i, err := strconv.Atoi(os.Getenv(e))
				if err != nil {
					return err
				}

				f.SetInt(int64(i))
			}

		}
	}

	return nil
}

func CreateFromEnv[T any]() (*T, error) {
	var t T

	godotenv.Load(".env")

	if err := load(&t); err != nil {
		return nil, err
	}

	return &t, nil
}
