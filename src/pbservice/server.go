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

func (pb *PBServer) isPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
	return pb.view.Backup == pb.me
}

func (pb *PBServer) ForwardToBackup(args *ForwardArgs) error {
	var reply ForwardReply
	reply.Err = ErrNoReply
	for i := 0; i < viewservice.DeadPings; i++ {
		if pb.view.Backup == "" || !pb.isPrimary() {
			return nil
		}
		ok := call(pb.view.Backup, "PBServer.FromForward", args, &reply)
		if ok {
			return nil
		}
		time.Sleep(viewservice.PingInterval / 5)
	}
	return nil
}

func (pb *PBServer) FromForward(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isBackup() {
		return errors.New("[FromForward]" + pb.me + " is not a backup")
	}
	if pb.view.Primary != args.Me {
		return errors.New("[FromForward]" + args.Me + " is not a primary")
	}
	for key, value := range args.Content {
		pb.content[key] = value
	}
	for key, value := range args.Client {
		pb.client[key] = value
	}
	
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return errors.New("[Get]I am not a primary")
	}
	reply.Value = pb.content[args.Key]

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return errors.New("[PutAppend]I am not a primary")
	}
	key, value, op := args.Key, args.Value, args.Op
	client, uuid := args.Me, args.UUID
	if pb.client[client] == uuid {
		return nil
	}
	if op == "Put" {
		pb.content[key] = value
	} else {
		pb.content[key] = pb.content[key] + value
	}
	pb.client[client] = uuid
	forwardArgs := ForwardArgs{pb.me, map[string]string{key:pb.content[key]}, map[string]string{client:uuid}}
	err := pb.ForwardToBackup(&forwardArgs)
	if err != nil {
		return errors.New("[PutAppend]Forward failed")
	}

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
	defer pb.mu.Unlock()

	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		var oldBackup = pb.view.Backup
		pb.view = view
		if view.Backup != oldBackup && pb.isPrimary() {
			pb.ForwardToBackup(&ForwardArgs{Me: pb.me, Content: pb.content, Client: pb.client})
		}
	} else {
		pb.view = view
	}
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
