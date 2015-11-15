package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "errors"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view 	   viewservice.View
	content    map[string]string
	client     map[string]string
}

func (pb *PBServer) IsPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) IsBackup() bool {
	return pb.view.Backup == pb.me
}

func (pb *PBServer) ForwardTo(args *ForwardArgs, name string) error {
	if name == "" {
		return nil
	}
	var reply ForwardReply
	ok := call(name, "PBServer.FromForward", args, &reply)
	if !ok {
		return errors.New("[ForwardTo] failed to forward put")
	}
	return nil
}

func (pb *PBServer) FromForward(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()

	if !pb.IsBackup() {
		pb.mu.Unlock()
		return errors.New("[FromForward]I am not a backup")
	}
	for key, value := range args.Content {
		pb.content[key] = value
	}
	for key, value := range args.Client {
		pb.client[key] = value
	}

	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()

	if !pb.IsPrimary() {
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("[Get]I am not a primary")
	}
	reply.Value = pb.content[args.Key]

	pb.mu.Unlock()
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()

	if !pb.IsPrimary() {
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("[PutAppend]I am not a primary")
	}
	key, value, op := args.Key, args.Value, args.Op
	client, uid := args.Me, args.UID
	if pb.client[client] == uid {
		pb.mu.Unlock()
		return nil
	}
	forwardArgs := ForwardArgs{map[string]string{key:value}, map[string]string{client:uid}}
	err := pb.ForwardTo(&forwardArgs, pb.view.Backup)
	if err != nil {
		pb.mu.Unlock()
		return errors.New("[PutAppend]Forward failed")
	}
	if op == "Put" {
		pb.content[key] = value
	} else {
		pb.content[key] = pb.content[key] + value
	}
	pb.client[client] = uid

	pb.mu.Unlock()
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()

	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		if view.Backup != "" && view.Backup != pb.view.Backup && pb.IsPrimary() {
			pb.ForwardTo(&ForwardArgs{Content: pb.content}, view.Backup)
		}
		pb.view = view
	} 

	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{}
	pb.content = make(map[string]string)
	pb.client = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
