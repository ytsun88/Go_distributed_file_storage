package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/ytsun88/distributed-file-storage/p2p"
)

type Message struct {
	Payload	any
}

type MessageStoreFile struct {
	Key		string
	Size	int64
	ID		string
}

type MessageGetFile struct {
	Key	string
	ID	string
}

type MessageDeleteFile struct {
	Key	string
	ID	string
}

type FileServerOpts struct {
	ID					string
	EncKey				[]byte
	StorageRoot			string
	PathTransformFunc	PathTransformFunc
	Transport			p2p.Transport
	BootstrapNodes		[]string
}

type FileServer struct {
	FileServerOpts

	peerLock	sync.Mutex
	peers		map[string]p2p.Peer

	store		*Store
	quitch		chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root: opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = generateID()
	}

	return &FileServer{
		FileServerOpts: opts,
		store: NewStore(storeOpts),
		quitch: make(chan struct{}),
		peers: make(map[string]p2p.Peer),
	}
}

func (s *FileServer) Start() error {
	fmt.Printf("[%s] starting fileserver...\n", s.Transport.Addr())
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()

	s.loop()

	return nil
}

func (s *FileServer) Delete(key string) error {
	if s.store.Has(s.ID, key) {
		if err := s.store.Delete(s.ID, key); err != nil {
			return err
		}
	}

	fmt.Printf("[%s] has deleted file (%s) locally, broadcast to peers\n", s.Transport.Addr(), key)
	
	msg := Message{
		Payload: MessageDeleteFile{
			Key: hashKey(key),
			ID: s.ID,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 500)
	
	return nil
}

func (s *FileServer) Get(key string) (io.Reader, error) {
	if s.store.Has(s.ID, key) {
		_, r, err := s.store.Read(s.ID, key)
		return r, err
	}
	
	fmt.Printf("[%s] don't have file (%s) locally\n", s.Transport.Addr(), key)

	msg := Message{
		Payload: MessageGetFile{
			Key: hashKey(key),
			ID: s.ID,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return nil, err
	}

	time.Sleep(time.Millisecond * 500)

	for _, peer := range s.peers {
		var fileSize int64 
		binary.Read(peer, binary.LittleEndian, &fileSize)

		n, err := s.store.WriteDecrypt(s.ID, s.EncKey, key, io.LimitReader(peer, fileSize))

		// n, err := s.store.Write(key,io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}

		fmt.Println("received bytes over the network:", n)
		peer.CloseStream()
	}

	_, r, err := s.store.Read(s.ID, key)
	return r, err
}

func (s *FileServer) Store(key string, r io.Reader) error {
	
	fileBuf := new(bytes.Buffer)
	tee := io.TeeReader(r, fileBuf)
	size, err := s.store.Write(s.ID, key, tee)
	if err != nil {
		return err
	}

	msg := Message {
		Payload: MessageStoreFile{
			Key: hashKey(key),
			Size: size + 16,
			ID: s.ID,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(5 * time.Millisecond)

	peers := []io.Writer{}

	for _, peer := range s.peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)
	mw.Write([]byte{p2p.IncomingStream})
	n, err := copyEncrypt(s.EncKey, fileBuf, mw)
	if err != nil {
		return err
	}

	fmt.Println("received and wrote bytes to disk:", n)

	return nil
}

func (s *FileServer) OnPeer(p p2p.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()
	s.peers[p.RemoteAddr().String()] = p
	
	log.Printf("connected with the remote %s", p.RemoteAddr())

	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) broadcast(msg *Message) error {
	msgBuf := new(bytes.Buffer)
	if err := gob.NewEncoder(msgBuf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range s.peers {
		peer.Send([]byte{p2p.IncomingMessage})
		if err := peer.Send(msgBuf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileServer) loop() {
	defer func () {
		s.Transport.Close()
		log.Println("file server stopped due to user quit action")
	} ()

	for {
		select {
		case rpc := <- s.Transport.Consume():
			var m Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&m); err != nil {
				log.Println("decoding error:", err)
			}
			if err := s.handleMessage(rpc.From, &m); err != nil {
				log.Println("handle message error:", err)
			}
		case <- s.quitch:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return s.handleMessageGetFile(from, v)
	case MessageDeleteFile:
		return s.handleMessageDeleteFile(v)
	}
	return nil
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	if !s.store.Has(msg.ID, msg.Key) {
		return fmt.Errorf("file (%s) does not exists on disk", msg.Key)
	}

	fileSize, r, err := s.store.Read(msg.ID, msg.Key)
	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		defer rc.Close()
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer, binary.LittleEndian, fileSize)
	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("wrote %d bytes over the network to %s\n", n, from)

	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	n, err := s.store.Write(msg.ID, msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err 
	}


	log.Printf("written %d bytes to disk\n", n)

	peer.CloseStream()

	return nil
}

func (s *FileServer) handleMessageDeleteFile(msg MessageDeleteFile) error {
	if !s.store.Has(msg.ID, msg.Key) {
		fmt.Printf("file (%s) does not exists on disk", msg.Key)
		return nil
	}

	if err := s.store.Delete(msg.ID, msg.Key); err != nil {
		return err
	}
	
	return nil
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}
		go func (addr string) {
			if err := s.Transport.Dial(addr); err != nil {
				log.Println("dial error: ", err)
			}
		} (addr)
		
	}
	return nil
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageDeleteFile{})
}