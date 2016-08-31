package unity

import "github.com/akutz/gofig"

const (
	// Name is the name of the storage driver
	Name = "unity"
)

func init() {
	registerConfig()
}

func registerConfig() {
	r := gofig.NewRegistration("Unity")
	r.Key(gofig.String, "", "", "", "unity.endpoint")
	r.Key(gofig.String, "", "", "", "unity.userName")
	r.Key(gofig.String, "", "", "", "unity.password")
	r.Key(gofig.String, "", "", "", "unity.storagePoolID")
	r.Key(gofig.String, "", "", "", "unity.storagePoolName")
	gofig.Register(r)
}