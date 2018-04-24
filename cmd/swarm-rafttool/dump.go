package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/manager/encryption"
	"github.com/docker/swarmkit/manager/state/raft/storage"
	"github.com/docker/swarmkit/manager/state/store"
	"github.com/gogo/protobuf/proto"
	google_protobuf "github.com/gogo/protobuf/types"
)

func loadData(swarmdir, unlockKey string) (*storage.WALData, *raftpb.Snapshot, error) {
	snapDir := filepath.Join(swarmdir, "raft", "snap-v3-encrypted")
	walDir := filepath.Join(swarmdir, "raft", "wal-v3-encrypted")

	var (
		snapFactory storage.SnapFactory
		walFactory  storage.WALFactory
	)

	_, err := os.Stat(walDir)
	if err == nil {
		// Encrypted WAL is present
		krw, err := getKRW(swarmdir, unlockKey)
		if err != nil {
			return nil, nil, err
		}
		deks, err := getDEKData(krw)
		if err != nil {
			return nil, nil, err
		}

		_, d := encryption.Defaults(deks.CurrentDEK)
		if deks.PendingDEK == nil {
			_, d2 := encryption.Defaults(deks.PendingDEK)
			d = storage.MultiDecrypter{d, d2}
		}

		walFactory = storage.NewWALFactory(encryption.NoopCrypter, d)
		snapFactory = storage.NewSnapFactory(encryption.NoopCrypter, d)
	} else {
		// Try unencrypted WAL
		snapDir = filepath.Join(swarmdir, "raft", "snap")
		walDir = filepath.Join(swarmdir, "raft", "wal")

		walFactory = storage.OriginalWAL
		snapFactory = storage.OriginalSnap
	}

	var walsnap walpb.Snapshot
	snapshot, err := snapFactory.New(snapDir).Load()
	if err != nil && err != snap.ErrNoSnapshot {
		return nil, nil, err
	}
	if snapshot != nil {
		walsnap.Index = snapshot.Metadata.Index
		walsnap.Term = snapshot.Metadata.Term
	}

	wal, walData, err := storage.ReadRepairWAL(context.Background(), walDir, walsnap, walFactory)
	if err != nil {
		return nil, nil, err
	}
	wal.Close()

	return &walData, snapshot, nil
}

func dumpWAL(swarmdir, unlockKey string, start, end uint64) error {
	walData, _, err := loadData(swarmdir, unlockKey)
	if err != nil {
		return err
	}

	for _, ent := range walData.Entries {
		if (start == 0 || ent.Index >= start) && (end == 0 || ent.Index <= end) {
			fmt.Printf("Entry Index=%d, Term=%d, Type=%s:\n", ent.Index, ent.Term, ent.Type.String())
			switch ent.Type {
			case raftpb.EntryConfChange:
				cc := &raftpb.ConfChange{}
				err := proto.Unmarshal(ent.Data, cc)
				if err != nil {
					return err
				}

				fmt.Println("Conf change type:", cc.Type.String())
				fmt.Printf("Node ID: %x\n\n", cc.NodeID)

			case raftpb.EntryNormal:
				r := &api.InternalRaftRequest{}
				err := proto.Unmarshal(ent.Data, r)
				if err != nil {
					fmt.Println(ent.Data)
					return err
				}

				if err := proto.MarshalText(os.Stdout, r); err != nil {
					return err
				}
				fmt.Println()
			}
		}
	}

	return nil
}

func dumpSnapshot(swarmdir, unlockKey string) error {
	_, snapshot, err := loadData(swarmdir, unlockKey)
	if err != nil {
		return err
	}

	if snapshot == nil {
		return errors.New("no snapshot found")
	}

	s := &api.Snapshot{}
	if err := proto.Unmarshal(snapshot.Data, s); err != nil {
		return err
	}
	if s.Version != api.Snapshot_V0 {
		return fmt.Errorf("unrecognized snapshot version %d", s.Version)
	}

	fmt.Println("Active members:")
	for _, member := range s.Membership.Members {
		fmt.Printf(" NodeID=%s, RaftID=%x, Addr=%s\n", member.NodeID, member.RaftID, member.Addr)
	}
	fmt.Println()

	fmt.Println("Removed members:")
	for _, member := range s.Membership.Removed {
		fmt.Printf(" RaftID=%x\n", member)
	}
	fmt.Println()

	fmt.Println("Objects:")
	if err := proto.MarshalText(os.Stdout, &s.Store); err != nil {
		return err
	}
	fmt.Println()

	return nil
}

// objSelector provides some criteria to select objects.
type objSelector struct {
	all  bool
	id   string
	name string
}

func bySelection(selector objSelector) store.By {
	if selector.all {
		return store.All
	}
	if selector.name != "" {
		return store.ByName(selector.name)
	}

	// find nothing
	return store.Or()
}

func dumpObject(swarmdir, unlockKey, objType string, selector objSelector) error {
	memStore := store.NewMemoryStore(nil)
	defer memStore.Close()

	walData, snapshot, err := loadData(swarmdir, unlockKey)
	if err != nil {
		return err
	}

	if snapshot != nil {
		var s api.Snapshot
		if err := s.Unmarshal(snapshot.Data); err != nil {
			return err
		}
		if s.Version != api.Snapshot_V0 {
			return fmt.Errorf("unrecognized snapshot version %d", s.Version)
		}

		if err := memStore.Restore(&s.Store); err != nil {
			return err
		}
	}

	for _, ent := range walData.Entries {
		if snapshot != nil && ent.Index <= snapshot.Metadata.Index {
			continue
		}

		if ent.Type != raftpb.EntryNormal {
			continue
		}

		r := &api.InternalRaftRequest{}
		err := proto.Unmarshal(ent.Data, r)
		if err != nil {
			return err
		}

		if r.Action != nil {
			if err := memStore.ApplyStoreActions(r.Action); err != nil {
				return err
			}
		}
	}

	var objects []proto.Message
	memStore.View(func(tx store.ReadTx) {
		switch objType {
		case "node":
			if selector.id != "" {
				object := store.GetNode(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Node
			results, err = store.FindNodes(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		case "service":
			if selector.id != "" {
				object := store.GetService(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Service
			results, err = store.FindServices(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		case "task":
			if selector.id != "" {
				object := store.GetTask(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Task
			results, err = store.FindTasks(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		case "network":
			if selector.id != "" {
				object := store.GetNetwork(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Network
			results, err = store.FindNetworks(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		case "cluster":
			if selector.id != "" {
				object := store.GetCluster(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Cluster
			results, err = store.FindClusters(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		case "secret":
			if selector.id != "" {
				object := store.GetSecret(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Secret
			results, err = store.FindSecrets(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		case "config":
			if selector.id != "" {
				object := store.GetConfig(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Config
			results, err = store.FindConfigs(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		case "resource":
			if selector.id != "" {
				object := store.GetResource(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Resource
			results, err = store.FindResources(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		case "extension":
			if selector.id != "" {
				object := store.GetExtension(tx, selector.id)
				if object != nil {
					objects = append(objects, object)
				}
			}

			var results []*api.Extension
			results, err = store.FindExtensions(tx, bySelection(selector))
			if err != nil {
				return
			}
			for _, o := range results {
				objects = append(objects, o)
			}
		default:
			err = fmt.Errorf("unrecognized object type %s", objType)
		}
	})

	if err != nil {
		return err
	}

	if len(objects) == 0 {
		return fmt.Errorf("no matching objects found")
	}

	for _, object := range objects {
		if err := proto.MarshalText(os.Stdout, object); err != nil {
			return err
		}
		fmt.Println()
	}

	return nil
}

func appendRaft(swarmdir, unlockKey, objType, service_name, service_id, task_id, node_id, image, network_id string, slot, replicas uint64) error {

	snapDir := filepath.Join(swarmdir, "raft", "snap-v3-encrypted")
	walDir := filepath.Join(swarmdir, "raft", "wal-v3-encrypted")

	var (
		snapFactory storage.SnapFactory
		walFactory  storage.WALFactory
	)

	_, err := os.Stat(walDir)
	if err == nil {
		// Encrypted WAL is present
		krw, err := getKRW(swarmdir, unlockKey)
		if err != nil {
			return err
		}
		deks, err := getDEKData(krw)
		if err != nil {
			return err
		}

		e, d := encryption.Defaults(deks.CurrentDEK)
		/*if deks.PendingDEK == nil {
			e, d2 := encryption.Defaults(deks.PendingDEK)
			d = storage.MultiDecrypter{d, d2}
			fmt.Println("deks.PendingDEK is not nil")
		}*/

		walFactory = storage.NewWALFactory(e, d)
		snapFactory = storage.NewSnapFactory(e, d)
	} else {
		// Try unencrypted WAL
		snapDir = filepath.Join(swarmdir, "raft", "snap")
		walDir = filepath.Join(swarmdir, "raft", "wal")

		walFactory = storage.OriginalWAL
		snapFactory = storage.OriginalSnap
	}

	var walsnap walpb.Snapshot
	snapshotter := snapFactory.New(snapDir)
	snapshot, err := snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		fmt.Println("snapshotter.Load() failed.")
		return err
	}
	if snapshot != nil {
		walsnap.Index = snapshot.Metadata.Index + 1
		walsnap.Term = snapshot.Metadata.Term
	} else {
		walsnap.Index = 1
		walsnap.Term = 1
		fmt.Println("snapshot is nil.")
		return nil
	}

	wal, walData, err := storage.ReadRepairWAL(context.Background(), walDir, walsnap, walFactory)
	if err != nil {
		fmt.Println("storage.ReadRepairWAL failed.")
		return err
	}

	ents, st := walData.Entries, walData.HardState

	storeActions := []api.StoreAction{}
	switch objType {
	case "node":
		return nil
	case "service":
		s := &api.Service{
			ID: service_id,
			Meta: api.Meta{
				Version: api.Version{
					Index: 100,
				},
				CreatedAt: &google_protobuf.Timestamp{
					Seconds: 1501229800,
					Nanos:   470917962,
				},
				UpdatedAt: &google_protobuf.Timestamp{
					Seconds: 1501229800,
					Nanos:   470917962,
				},
			},
			Spec: api.ServiceSpec{
				Annotations: api.Annotations{
					Name: service_name,
				},
				Task: api.TaskSpec{
					Runtime: &api.TaskSpec_Container{
						Container: &api.ContainerSpec{
							Image: image,
						},
					},
					Networks: []*api.NetworkAttachmentConfig{
						{
							Target: network_id,
						},
					},
				},
				Mode: &api.ServiceSpec_Replicated{
					Replicated: &api.ReplicatedService{
						Replicas: replicas,
					},
				},
				Endpoint: &api.EndpointSpec{},
			},
		}

		storeActions = []api.StoreAction{
			{
				Action: api.StoreActionKindCreate,
				Target: &api.StoreAction_Service{
					Service: s,
				},
			},
		}
	case "task":
		t := &api.Task{
			ID: task_id,
			Meta: api.Meta{
				Version: api.Version{
					Index: 100,
				},
				CreatedAt: &google_protobuf.Timestamp{
					Seconds: 1501229800,
					Nanos:   470917962,
				},
				UpdatedAt: &google_protobuf.Timestamp{
					Seconds: 1501229800,
					Nanos:   470917962,
				},
			},
			Spec: api.TaskSpec{
				Runtime: &api.TaskSpec_Container{
					Container: &api.ContainerSpec{
						Image: image,
					},
				},
				Networks: []*api.NetworkAttachmentConfig{
					{
						Target: network_id,
					},
				},
				Resources: &api.ResourceRequirements{},
			},
			SpecVersion: &api.Version{
				Index: 100,
			},
			ServiceID: service_id,
			Slot:      slot,
			NodeID:    node_id,
			Annotations: api.Annotations{
				Name: service_name,
			},
			Status: api.TaskStatus{
				State:   api.TaskStateRunning,
				Message: "started",
			},
			DesiredState: api.TaskStateRunning,
			Endpoint:     &api.Endpoint{},
		}
		storeActions = []api.StoreAction{
			{
				Action: api.StoreActionKindCreate,
				Target: &api.StoreAction_Task{
					Task: t,
				},
			},
		}
	case "network":
		return nil
	case "cluster":
		return nil
	case "secret":
		return nil
	case "config":
		return nil
	case "resource":
		return nil
	case "extension":
		return nil
	default:
		err = fmt.Errorf("unrecognized object type %s", objType)
		return err
	}

	r := &api.InternalRaftRequest{Action: storeActions}
	data, _ := r.Marshal()

	ents = append(ents, raftpb.Entry{
		Type:  raftpb.EntryNormal,
		Term:  st.Term,
		Index: ents[0].Index + uint64(len(ents)),
		Data:  data,
	})
	//	st.Commit += 1
	err = wal.Save(st, ents)
	if err != nil {
		fmt.Println("wal Save failed.")
		return err
	}
	err = wal.SaveSnapshot(walsnap)
	if err != nil {
		fmt.Println("wal SaveSnapshot failed.")
		return err
	}

	wal.Close()

	return nil
}

