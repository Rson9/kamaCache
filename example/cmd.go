package example

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rson9/kamaCache/group"
	"github.com/rson9/kamaCache/peer"
	"github.com/rson9/kamaCache/server"
	"github.com/rson9/kamaCache/utils"
)

// Configuration flags
var (
	flagPort   = flag.Int("port", 8001, "节点端口")
	flagNodeID = flag.String("node", "A", "节点标识符 (例如 A, B, C)")
	// ETCD相关flags
	flagEtcdEndpoints = flag.String("etcd.endpoints", "localhost:2379", "etcd服务器端点列表, 逗号分隔")
	flagEtcdUsername  = flag.String("etcd.username", "", "etcd用户名")
	flagEtcdPassword  = flag.String("etcd.password", "", "etcd密码")
	flagDialTimeout   = flag.Duration("etcd.dial-timeout", 5*time.Second, "etcd连接超时")
	flagRegisterEtcd  = flag.Bool("etcd.register", true, "是否向etcd注册服务")
	// 服务/组相关flags
	flagServiceName = flag.String("service.name", "kama-cache", "注册到etcd的服务名称") // This name is used by peer discovery
	flagGroupName   = flag.String("group.name", "my_cache_group", "缓存组名称")
	flagGroupSize   = flag.Int64("group.size", 2<<20, "缓存组最大字节数 (例如 2MB)") // 2MB default
	flagServiceAddr = flag.String("service.addr", "", "服务监听地址 (例如 :8001). 如果未提供, 将使用 --port.")
)

func example() {
	flag.Parse()

	// 1. address 配置
	var listenAddr string
	if *flagServiceAddr != "" {
		listenAddr = *flagServiceAddr
	} else {
		listenAddr = fmt.Sprintf(":%d", *flagPort)
	}
	listenAddr, _ = utils.CompleteAddress(listenAddr)
	nodeID := *flagNodeID
	log.Printf("[节点%s] 启动中: %v", nodeID, flagServiceAddr)

	// 2. etcd 端点处理
	etcdEndpoints := parseEndpoints(*flagEtcdEndpoints)
	if len(etcdEndpoints) == 0 && *flagRegisterEtcd {
		log.Fatalf("[节点%s] ETCD已启用但未提供endpoint", nodeID)
	}

	// 3. Server创建
	serverOptions := []server.ServerOption{
		server.WithDialTimeout(*flagDialTimeout),
		server.WithMaxMsgSize(10 * (1 << 20)),
	}
	if *flagRegisterEtcd {
		serverOptions = append(serverOptions,
			server.WithEtcdEndpoints(etcdEndpoints),
			server.WithEtcdUsername(*flagEtcdUsername),
			server.WithEtcdPassword(*flagEtcdPassword),
		)
	}
	cacheServer, err := server.NewServer(listenAddr, *flagServiceName, serverOptions...)
	if err != nil {
		log.Fatalf("[节点%s] 启动失败: %v", nodeID, err)
	}

	// 4. Peer discovery
	picker, err := peer.NewClientPicker(listenAddr)
	if err != nil {
		log.Fatalf("[节点%s] 创建 peer picker 失败: %v", nodeID, err)
	}

	// 5. 创建 Group
	cacheGroup := group.NewGroup(*flagGroupName, *flagGroupSize, group.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			return nil, nil
		}),
	)

	//  注册PeerPicker到Group
	cacheGroup.RegisterPeers(picker)

	//  注册Group到Server的groups里
	cacheServer.RegisterGroup(cacheGroup)

	// 6. 启动服务
	serverCtx, serverCancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := cacheServer.Start(); err != nil {
			log.Fatalf("[节点%s] 启动失败: %v", nodeID, err)
		}
	}()

	time.Sleep(3 * time.Second)

	// 7. CLI Loop
	cliCtx := context.Background()

	for {
		fmt.Println("\n--- KamaCache CLI ---")
		fmt.Printf("节点: %s | 地址: %s | Group: %s\n", nodeID, listenAddr, *flagGroupName)
		fmt.Println("1. Set")
		fmt.Println("2. Get (含存在性判断)")
		fmt.Println("3. List Groups")
		fmt.Println("4. 触发 Getter Fallback")
		fmt.Println("5. Exit")
		fmt.Println("6. List Peers")
		fmt.Println("7. Print Stats")
		fmt.Print("选择: ")

		var choice int
		if _, err := fmt.Scanln(&choice); err != nil {
			clearInputBuffer(os.Stdin)
			continue
		}

		switch choice {
		case 1:
			var key, val string
			fmt.Print("请输入键: ")
			fmt.Scanln(&key)
			fmt.Print("请输入值: ")
			fmt.Scanln(&val)

			if err := cacheGroup.Set(cliCtx, key, []byte(val)); err != nil {
				fmt.Printf("Set 错误: %v\n", err)
			} else {
				fmt.Println("Set 成功")
			}
		case 2:
			var key string
			fmt.Print("请输入键: ")
			fmt.Scanln(&key)

			val, ok := cacheGroup.Get(cliCtx, key)
			if ok {
				fmt.Printf("键 '%s' 的值为: %s\n", key, val.String())
			} else {
				fmt.Printf("键 '%s'的值获取失败 \n", key)
			}
		case 3:
			fmt.Println("已注册的 Group:")
			for _, name := range group.ListGroups() {
				fmt.Println("-", name)
			}
		case 4:
			var key string
			fmt.Print("请输入键（模拟 fallback）: ")
			fmt.Scanln(&key)
			val, ok := cacheGroup.Get(cliCtx, key)
			if !ok {
				fmt.Printf("Fallback 错误\n")
			} else {
				fmt.Printf("fallback 返回: %s\n", val.String())
			}
		case 5:
			serverCancel()
			goto shutdown
		case 6: // 列出 peers
			fmt.Println("--- 当前 Peer 列表 ---")
			if picker != nil {
				peers := picker.Peers()
				if len(peers) == 0 {
					fmt.Println("无可用 peers.")
				} else {
					for _, p := range peers {
						fmt.Println("- " + p)
					}
				}
			} else {
				fmt.Println("Peer picker 未初始化.")
			}
			fmt.Println("----------------------")

		case 7: // 打印缓存统计
			fmt.Println("--- 缓存命中统计信息 ---")
			stats := cacheGroup.Stats()
			for k, v := range stats {
				fmt.Printf("%-20s : %v\n", k, v)
			}
			fmt.Println("------------------------")

		default:
			fmt.Println("无效选项")
		}
		select {
		case <-serverCtx.Done():
			goto shutdown
		case sig := <-sigChan:
			log.Printf("收到退出信号: %v", sig)
			serverCancel()
			goto shutdown
		default:
		}
	}

shutdown:
	log.Printf("节点 %s 正在关闭服务...", nodeID)
	cacheServer.Stop()
	log.Println("已退出。")
}

func parseEndpoints(input string) []string {
	if input == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	var eps []string
	for _, ep := range parts {
		if trimmed := strings.TrimSpace(ep); trimmed != "" {
			eps = append(eps, trimmed)
		}
	}
	return eps
}

func clearInputBuffer(r *os.File) {
	var buf [1024]byte
	for {
		n, err := r.Read(buf[:])
		if n > 0 || err != nil {
			break
		}
	}
}
