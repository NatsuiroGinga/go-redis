package enum

// Command 命令枚举
type Command struct {
	name       string // 命令名称
	paramCount int    // 除去命令本身后的参数数量
}

// Name 返回命令名称
func (cmd *Command) Name() string {
	return cmd.name
}

// String equals to Name().
func (cmd *Command) String() string {
	return cmd.name
}

// ParamCount 返回命令参数数量
func (cmd *Command) ParamCount() int {
	return cmd.paramCount
}

// Arity 返回命令带命令本身的参数数量, 即 ParamCount() + 1
func (cmd *Command) Arity() int {
	return cmd.paramCount + 1
}

var (
	DEL      = &Command{name: "DEL", paramCount: Variable}
	PING     = &Command{name: "PING", paramCount: ZERO}
	EXISTS   = &Command{name: "EXISTS", paramCount: Variable}
	FLUSHDB  = &Command{name: "FLUSHDB", paramCount: ZERO}
	TYPE     = &Command{name: "TYPE", paramCount: Single}
	RENAME   = &Command{name: "RENAME", paramCount: DOUBLE}
	RENAMENX = &Command{name: "RENAMENX", paramCount: DOUBLE}
	KEYS     = &Command{name: "KEYS", paramCount: Single}
	GET      = &Command{name: "GET", paramCount: Single}
	SET      = &Command{name: "SET", paramCount: DOUBLE}
	SETNX    = &Command{name: "SETNX", paramCount: DOUBLE}
	STRLEN   = &Command{name: "STRLEN", paramCount: Single}
	GETSET   = &Command{name: "GETSET", paramCount: DOUBLE}
	SELECT   = &Command{name: "SELECT", paramCount: Single}
	INCR     = &Command{name: "INCR", paramCount: Single}
)

const (
	ZERO     = iota
	Single        // 单个参数
	DOUBLE        // 两个参数
	Variable = -2 // 可变长参数
)
