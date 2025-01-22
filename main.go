package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/ytsun88/distributed-file-storage/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr: listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder: p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcpTransportOpts)

	fileServerOpts := FileServerOpts {
		EncKey: newEncryptionKey(),
		StorageRoot: listenAddr + "_db",
		PathTransformFunc: CASPathTransformFunc,
		Transport: tcpTransport,
		BootstrapNodes: nodes,
	}

	s := NewFileServer(fileServerOpts)

	tcpTransport.OnPeer = s.OnPeer

	return s
}

func main() {
	s1 := makeServer(":3000", "")
	s2 := makeServer(":3001", ":3000")
	s3 := makeServer(":3002", ":3000", ":3001")

	go func() {
		log.Fatal(s1.Start())
	} ()
	time.Sleep(500 * time.Millisecond)
	go func() {
		log.Fatal(s2.Start())
	} ()
	time.Sleep(500 * time.Millisecond)
	go s3.Start()
	time.Sleep(500 * time.Millisecond)

	for {
		var (
			cmd		 string
			key      string
			pathName string
		)
		fmt.Println("Please enter command:")
		fmt.Scanln(&cmd)
		if cmd =="Store" {
			fmt.Println("Please enter the path name of the file:")
			fmt.Scanln(&pathName)
			fmt.Println("Please enter a key:")
			fmt.Scanln(&key)
			file, err := os.Open(pathName)
			if err != nil {
				fmt.Println("No such a file")
				continue
			}
			defer file.Close()

			s3.Store(key, bufio.NewReader(file))
		} else if cmd == "Get" {
			fmt.Println("Please enter the key:")
			fmt.Scanln(&key)
			r, err := s3.Get(key)
			if err != nil {
				log.Fatal(err)
			}

			b, err := io.ReadAll(r)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(string(b))
		} else if cmd == "Delete" {
			fmt.Println("Please enter the key:")
			fmt.Scanln(&key)
			if err := s3.Delete(key); err != nil {
				log.Fatal(err)
			}
		} else if cmd == "Quit" {
			return
		} else {
			fmt.Println("No such a command")
		}
	}
}