package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const configFile = "redis.conf"

func fileExists(filename string) bool {
	stat, err := os.Stat(filename)
	return err == nil && !stat.IsDir()
}

// serverProperties 是服务器的配置
type serverProperties struct {
	Bind                string `cfg:"bind"`                   // 绑定的ip, 默认127.0.0.1
	Port                int    `cfg:"port"`                   // 端口, 默认6379
	AppendOnly          bool   `cfg:"append-only"`            // 是否启动aof, 默认不启动
	AppendFilename      string `cfg:"append-filename"`        // aof文件名
	MaxClients          int    `cfg:"max-clients"`            // 最大客户端数
	RequirePass         string `cfg:"require-pass"`           // 是否需要密码
	Databases           int    `cfg:"databases"`              // 数据库量,  默认16
	Cycle               int    `cfg:"cycle"`                  // 清理过期数据的周期, 单位是s, 默认1s
	Buckets             int    `cfg:"buckets"`                // 放数据的桶的数量, 默认65536
	ListMaxShardSize    int    `cfg:"list-max-shard-size"`    // quicklist中每一个分片所存储的数据最大容量, 默认512
	SetMaxIntSetEntries int    `cfg:"set-max-intset-entries"` // intset中可以存储的最大元素个数, 默认为512
	// cluster
	Peers []string `cfg:"peers"` // 所有集群节点的地址
	Self  string   `cfg:"self"`  // 本身的地址
	// dev
	Dev bool // 是否在测试状态, 如果在测试会开启Debug输出, 否则关闭Debug, 默认开启
}

func (properties *serverProperties) String() string {
	return fmt.Sprintf("%#v", *properties)
}

// Properties holds global config properties
var Properties *serverProperties

func init() {
	// default config
	Properties = &serverProperties{
		Bind:                "127.0.0.1",
		Port:                6379,
		AppendOnly:          false,
		Cycle:               1,
		Buckets:             1 << 16,
		ListMaxShardSize:    1 << 9,
		Databases:           1 << 4,
		SetMaxIntSetEntries: 512,
		Dev:                 true,
		MaxClients:          -1,
	}
	// read config file to rewrite `Properties`
	if fileExists(configFile) {
		SetupConfig(configFile)
	}
}

func parse(src io.Reader) *serverProperties {
	config := Properties

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value
				fieldVal.SetBool(boolValue)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")

					for j := range slice {
						slice[j] = strings.TrimSpace(slice[j])
					}

					fieldVal.Set(reflect.ValueOf(slice))
				}
			default:
				panic("unhandled default case")
			}
		}
	}
	return config
}

// SetupConfig read config file and store properties into Properties
func SetupConfig(configFilename string) {
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parse(file)
}
