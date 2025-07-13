package utils

import (
	"net"
	"strings"
)

func ValidPeerAddr(addr string) bool {
	t1 := strings.Split(addr, ":")
	if len(t1) != 2 {
		return false
	}
	// TODO: more selections
	t2 := strings.Split(t1[0], ".")
	if t1[0] != "localhost" && len(t2) != 4 {
		return false
	}
	return true
}

// func getLocalIP() (string, error) {
// 	ifaces, err := net.Interfaces()
// 	if err != nil {
// 		return "", err
// 	}

// 	for _, iface := range ifaces {
// 		// 忽略未启用的网卡
// 		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
// 			continue
// 		}
// 		addrs, err := iface.Addrs()
// 		if err != nil {
// 			continue
// 		}
// 		for _, addr := range addrs {
// 			var ip net.IP
// 			switch v := addr.(type) {
// 			case *net.IPNet:
// 				ip = v.IP
// 			case *net.IPAddr:
// 				ip = v.IP
// 			}
// 			if ip == nil || ip.IsLoopback() {
// 				continue
// 			}
// 			if ip = ip.To4(); ip != nil {
// 				return ip.String(), nil
// 			}
// 		}
// 	}
// 	return "", fmt.Errorf("没有找到有效的本地IP")
// }

func CompleteAddress(port string) (string, error) {
	ip := "127.0.0.1"

	// 处理前导冒号
	if len(port) > 0 && port[0] == ':' {
		port = port[1:]
	}
	return net.JoinHostPort(ip, port), nil
}
