package tests

import (
	"github.com/akutz/gofig"
	"github.com/emccode/libstorage/api/context"
	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/server"
	"github.com/emccode/libstorage/drivers/storage/unity"
	"os"
	"strconv"
	"testing"
)

func skipTests() bool {
	travis, _ := strconv.ParseBool(os.Getenv("TRAVIS"))
	noTest, _ := strconv.ParseBool(os.Getenv("TEST_SKIP_UNITY"))
	return travis || noTest
}

func TestMain(m *testing.M) {
	server.CloseOnAbort()
	ec := m.Run()
	os.Exit(ec)
}

func TestInstanceID(t *testing.T) {
	if skipTests() {
		t.SkipNow()
	}

	sd, err := registry.NewStorageDriver(unity.Name)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := sd.Init(ctx, gofig.New()); err != nil {
		t.Fatal(err)
	}
}
