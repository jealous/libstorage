package storage

import (
	log "github.com/Sirupsen/logrus"

	"errors"
	"fmt"
	"github.com/akutz/gofig"
	"github.com/akutz/goof"
	"github.com/emccode/libstorage/api/context"
	"github.com/emccode/libstorage/api/registry"
	"github.com/emccode/libstorage/api/types"
	"github.com/emccode/libstorage/drivers/storage/unity"
	"github.com/jealous/gounity"
	"github.com/jealous/gounity/rsc"
)

type driver struct {
	config gofig.Config
	unity  gounity.Unity
	pool   *rsc.Pool
}

func init() {
	registry.RegisterStorageDriver(unity.Name, newDriver)
}

func newDriver() types.StorageDriver {
	return &driver{}
}

func (d *driver) Name() string {
	return unity.Name
}

func (d *driver) Init(ctx types.Context, config gofig.Config) error {
	d.config = config
	ip := d.endpoint()
	user := d.userName()
	pass := d.password()

	fields := map[string]interface{}{
		"provider":        d.Name(),
		"moduleName":      d.Name(),
		"endpoint":        ip,
		"userName":        user,
		"storagePoolName": d.storagePoolName(),
		"storagePoolID":   d.storagePoolID(),
	}

	ctx.Info("initializing driver: ", fields)
	log.WithField("endpoint", ip).WithField("userName", user).Info("starting unity driver.")

	var err error
	if d.unity, err = gounity.New(ip, user, pass); err != nil {
		return goof.WithFieldsE(fields, "failed to initialize unity instance.", err)
	}
	if err = d.unity.Authenticate(); err != nil {
		return goof.WithFieldsE(fields, "error logging in.", err)
	}

	ctx.WithFields(fields).Info("storage driver initialized.")
	return err
}

func (d *driver) Type(ctx types.Context) (types.StorageType, error) {
	return types.Block, nil
}

func (d *driver) NextDeviceInfo(
	ctx types.Context) (*types.NextDeviceInfo, error) {
	return nil, nil
}

func (d *driver) InstanceInspect(
	ctx types.Context,
	opts types.Store) (*types.Instance, error) {
	iid := context.MustInstanceID(ctx)
	if iid.ID != "" {
		return &types.Instance{InstanceID: iid}, nil
	} else {
		return &types.Instance{
			InstanceID: &types.InstanceID{
				ID:     d.unity.Serial(),
				Driver: d.Name(),
			},
		}, nil
	}
}

func (d *driver) Volumes(
	ctx types.Context,
	opts *types.VolumesOpts) ([]*types.Volume, error) {

	mappedVolumes := make(map[string]string)
	if opts.Attachments {
		if ld, ok := context.LocalDevices(ctx); ok {
			mappedVolumes = ld.DeviceMap
		}
	}

	lunList := d.pool.GetLunList()

	var volumesSD []*types.Volume
	for it := lunList.Iterator(); it.Next(); {
		lun := it.Value().(*rsc.Lun)
		var deviceName string
		if _, exists := mappedVolumes[lun.Id]; exists {
			deviceName = mappedVolumes[lun.Id]
		}
		volumeSD := d.assembleVolumeSD(lun, deviceName)
		volumesSD = append(volumesSD, volumeSD)
	}
	return volumesSD, nil
}

func (d *driver) assembleVolumeSD(lun *rsc.Lun, deviceName string) *types.Volume {
	instanceID := &types.InstanceID{
		ID:     lun.Id,
		Driver: d.Name(),
	}
	attachmentSD := &types.VolumeAttachment{
		VolumeID:   lun.Id,
		InstanceID: instanceID,
		DeviceName: deviceName,
		Status:     "",
	}
	attachmentsSD := []*types.VolumeAttachment{attachmentSD}
	volumeSD := &types.Volume{
		Name:             lun.Name,
		ID:               lun.Id,
		AvailabilityZone: "",
		Status:           "",
		Type:             "thin",
		Size:             int64(lun.SizeTotal / 1024 / 1024 / 1024),
		Attachments:      attachmentsSD,
	}
	return volumeSD
}

func (d *driver) getPool() *rsc.Pool {
	if d.pool == nil {
		poolName := d.storagePoolName()
		poolId := d.storagePoolID()
		if poolId != "" {
			d.pool = d.unity.GetPoolById(poolId)
		} else if poolName != "" {
			d.pool = d.unity.GetPoolByName(poolName)
		} else {
			log.WithField("poolName", poolName).WithField("poolId", poolId).Fatal(
				"cannot find the specified storage pool on array.  please check the configure.")
		}
	}
	return d.pool
}

func getMappedVolumes(ctx types.Context, opts *types.VolumeInspectOpts) map[string]string {
	mappedVolumes := make(map[string]string)
	if opts.Attachments {
		if ld, ok := context.LocalDevices(ctx); ok {
			mappedVolumes = ld.DeviceMap
		}
	}
	return mappedVolumes
}

func (d *driver) VolumeInspect(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeInspectOpts) (*types.Volume, error) {

	if volumeID == "" {
		return nil, goof.New("no volume ID specified.")
	}

	mappedVolumes := getMappedVolumes(ctx, opts)

	lun := d.unity.GetLunById(volumeID)
	if lun == nil {
		return nil, goof.New(fmt.Sprintf("failed to find lun with id %v", volumeID))
	}

	var deviceName string
	if _, exists := mappedVolumes[lun.Id]; exists {
		deviceName = mappedVolumes[lun.Id]
	}

	return d.assembleVolumeSD(lun, deviceName), nil
}

func (d *driver) VolumeCreate(ctx types.Context, volumeName string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {
	log.WithField("name", volumeName).WithField("opts", opts).Info("creating volume")

	volume := &types.Volume{}

	if opts.AvailabilityZone != nil {
		volume.AvailabilityZone = *opts.AvailabilityZone
	}
	if opts.Type != nil {
		volume.Type = *opts.Type
	}
	if opts.Size != nil {
		volume.Size = *opts.Size
	}
	if opts.IOPS != nil {
		volume.IOPS = *opts.IOPS
	}

	lun, err := d.getPool().CreateLun(volumeName, uint32(volume.Size))
	if err != nil {
		return nil, err
	}

	return d.VolumeInspect(ctx, lun.Id, &types.VolumeInspectOpts{
		Attachments: true})
}

func (d *driver) VolumeCreateFromSnapshot(
	ctx types.Context,
	snapshotID, volumeName string,
	opts *types.VolumeCreateOpts) (*types.Volume, error) {
	log.Error("create volume from snapshot is not implemented yet.")
	return nil, nil
}

func (d *driver) VolumeCopy(
	ctx types.Context,
	volumeID, volumeName string,
	opts types.Store) (*types.Volume, error) {
	log.Error("volume copy is not implemented yet.")
	return nil, nil
}

func (d *driver) VolumeSnapshot(
	ctx types.Context,
	volumeID, snapshotName string,
	opts types.Store) (*types.Snapshot, error) {
	log.Error("snaphost related APIs are not implemented yet.")
	return nil, nil
}

func (d *driver) VolumeRemove(
	ctx types.Context,
	volumeID string,
	opts types.Store) error {
	lun := d.unity.GetLunById(volumeID)
	err := lun.Delete()
	if err != nil {
		log.WithError(err).Error("failed to delete volume.")
	}
	return err
}

func (d *driver) VolumeAttach(ctx types.Context,
	volumeID string,
	opts *types.VolumeAttachOpts) (*types.Volume, string, error) {
	lun := d.unity.GetLunById(volumeID)
	if lun == nil {
		return nil, "", errors.New("cannot find the specified lun.")
	}
	hostLUNList := lun.GetHostLUN()
	if hostLUNList.Size() > 0 && !opts.Force {
		return nil, "", goof.New("volume already attached to a host.")
	} else if hostLUNList.Size() > 0 && opts.Force {
		// todo: do detach
	}
	// todo: do attach
	attachedVol, err := d.VolumeInspect(
		ctx, volumeID, &types.VolumeInspectOpts{
			Attachments: true,
			Opts:        opts.Opts,
		})
	if err != nil {
		return nil, "", goof.WithError("error getting volume.", err)
	}
	return attachedVol, attachedVol.ID, nil
}

func (d *driver) VolumeDetach(
	ctx types.Context,
	volumeID string,
	opts *types.VolumeDetachOpts) (*types.Volume, error) {
	iid := context.MustInstanceID(ctx)

	lun := d.unity.GetLunById(volumeID)
	if lun == nil {
		return nil, errors.New("error getting lun.")
	}

	host := d.unity.GetHostById(iid.ID)
	if err := lun.DetachHost(host); err != nil {
		return nil, err
	}

	vol, err := d.VolumeInspect(ctx, volumeID, &types.VolumeInspectOpts{
		Attachments: true})
	if err != nil {
		return nil, err
	}
	return vol, nil
}

func (d *driver) VolumeDetachAll(
	ctx types.Context,
	volumeID string,
	opts types.Store) error {
	lun := d.unity.GetLunById(volumeID)
	if lun == nil {
		return errors.New("error getting lun.")
	}
	return lun.DetachAllHosts()
}

func (d *driver) Snapshots(
	ctx types.Context,
	opts types.Store) ([]*types.Snapshot, error) {
	log.Error("snaphost related APIs are not implemented yet.")
	return nil, nil
}

func (d *driver) SnapshotInspect(
	ctx types.Context,
	snapshotID string,
	opts types.Store) (*types.Snapshot, error) {
	log.Error("snaphost related APIs are not implemented yet.")
	return nil, nil
}

func (d *driver) SnapshotCopy(
	ctx types.Context,
	snapshotID, snapshotName, destinationID string,
	opts types.Store) (*types.Snapshot, error) {
	log.Error("snaphost related APIs are not implemented yet.")
	return nil, nil
}

func (d *driver) SnapshotRemove(
	ctx types.Context,
	snapshotID string,
	opts types.Store) error {
	log.Error("snaphost related APIs are not implemented yet.")
	return nil
}

///////////////////////////////////////////////////////////////////////
//////                  CONFIG HELPER STUFF                   /////////
///////////////////////////////////////////////////////////////////////

func (d *driver) endpoint() string {
	return d.config.GetString("unity.endpoint")
}

func (d *driver) userName() string {
	return d.config.GetString("unity.userName")
}

func (d *driver) password() string {
	return d.config.GetString("unity.password")
}

func (d *driver) storagePoolID() string {
	return d.config.GetString("unity.storagePoolID")
}

func (d *driver) storagePoolName() string {
	return d.config.GetString("unity.storagePoolName")
}
