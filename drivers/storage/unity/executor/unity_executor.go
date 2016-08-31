package executor

import (
	"github.com/akutz/gofig"
	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/unity"
)

// driver is the storage executor for the Unity storage driver.
type driver struct{}

func init() {
	registry.RegisterStorageExecutor(unity.Name, newDriver)
}

func newDriver() types.StorageExecutor {
	return &driver{}
}

func (d *driver) Init(context types.Context, config gofig.Config) error {
	return nil
}

func (d *driver) Name() string {
	return unity.Name
}

// Next Device returns the next available device.
func (d *driver) NextDevice(
	ctx types.Context,
	opts types.Store) (string, error) {
	return "", nil
}

// LocalDevices returns a map of the system's local devices.
func (d *driver) LocalDevices(
	ctx types.Context,
	opts *types.LocalDevicesOpts) (*types.LocalDevices, error) {
	var (
		lvm map[string]string
		err error
	)
	//todo: get real values
	lvm, err = make(map[string]string), nil

	if err != nil {
		return nil, err
	}
	return &types.LocalDevices{
		Driver:    unity.Name,
		DeviceMap: lvm,
	}, nil
}

// InstanceID returns the local system's InstanceID.
func (d *driver) InstanceID(
	ctx types.Context,
	opts types.Store) (*types.InstanceID, error) {
	sg, err := getSdcLocalGUID()
	if err != nil {
		return nil, err
	}
	iid := &types.InstanceID{Driver: unity.Name}
	if err := iid.MarshalMetadata(sg); err != nil {
		return nil, err
	}
	return iid, nil
}

func getSdcLocalGUID() (sdcGUID string, err error) {
	// todo: get the real sdcGUID
	return "", nil
}
