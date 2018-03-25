package main

import (
	"fmt"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func connect() *zk.Conn {
	//zksStr := os.Getenv("ZOOKEEPER_SERVERS")
	//zks := strings.Split(zksStr, ",")
	zks := []string{"localhost:2181", "localhost:2182", "localhost:2183"}
	conn, _, err := zk.Connect(zks, time.Second)
	must(err)
	return conn
}

func main() {
	//testConn()
	//testEphemeral()
	//testWatch()
	watchMirror()
}

func testWatch() {
	conn1 := connect()
	defer conn1.Close()

	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	found, _, ech, err := conn1.ExistsW("/watch")
	must(err)
	fmt.Printf("found: %t\n", found)

	conn2 := connect()
	must(err)

	go func() {
		time.Sleep(time.Second * 3)
		fmt.Println("creating znode")
		_, err = conn2.Create("/watch", []byte("here"), flags, acl)
		must(err)
	}()

	evt := <-ech
	fmt.Println("watch fired")
	must(evt.Err)

	found, _, err = conn1.Exists("/watch")
	must(err)
	fmt.Printf("found: %t\n", found)
}

func testEphemeral() {
	conn1 := connect()

	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	_, err := conn1.Create("/ephemeral", []byte("here"), flags, acl)
	must(err)

	conn2 := connect()
	defer conn2.Close()

	exists, _, err := conn2.Exists("/ephemeral")
	must(err)
	fmt.Printf("before disconnect: %+v\n", exists)

	conn1.Close()
	time.Sleep(time.Second * 2)

	exists, _, err = conn2.Exists("/ephemeral")
	must(err)
	fmt.Printf("after disconnect: %+v\n", exists)
}

func testConn() {
	conn := connect()
	defer conn.Close()

	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	path, err := conn.Create("/01", []byte("data"), flags, acl)
	must(err)
	fmt.Printf("create: %+v\n", path)

	data, stat, err := conn.Get("/01")
	must(err)
	fmt.Printf("get:    %+v %+v\n", string(data), stat)

	stat, err = conn.Set("/01", []byte("newdata"), stat.Version)
	must(err)
	fmt.Printf("set:    %+v\n", stat)

	err = conn.Delete("/01", -1)
	must(err)
	fmt.Printf("delete: ok\n")

	exists, stat, err := conn.Exists("/01")
	must(err)
	fmt.Printf("exists: %+v %+v\n", exists, stat)
}

func mirror(conn *zk.Conn, path string) (chan []string, chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)
	go func() {
		for {
			snapshot, _, events, err := conn.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}
			snapshots <- snapshot
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()
	return snapshots, errors
}

func watchMirror() {
	conn1 := connect()
	defer conn1.Close()

	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	snapshots, errors := mirror(conn1, "/mirror")
	go func() {
		for {
			select {
			case snapshot := <-snapshots:
				fmt.Printf("%+v\n", snapshot)
			case err := <-errors:
				panic(err)
			}
		}
	}()

	conn2 := connect()
	time.Sleep(time.Second)

	_, err := conn2.Create("/mirror/one", []byte("one"), flags, acl)
	must(err)
	time.Sleep(time.Second)

	_, err = conn2.Create("/mirror/two", []byte("two"), flags, acl)
	must(err)
	time.Sleep(time.Second)

	err = conn2.Delete("/mirror/two", 0)
	must(err)
	time.Sleep(time.Second)

	_, err = conn2.Create("/mirror/three", []byte("three"), flags, acl)
	must(err)
	time.Sleep(time.Second)

	conn2.Close()
	time.Sleep(time.Second)
}
