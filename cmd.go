package dlm

const (
	CmdSet    = "SET"
	CmdRemove = "REM"
)

type cmd struct {
	Name  string
	Key   string
	Value []byte
}
