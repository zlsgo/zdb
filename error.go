package zdb

type errType uint

const (
	ErrException errType = iota + 1
	// ErrNotFound no records found
	ErrNotFound
	ErrModuleAlreadyExists
	ErrNotMigration
	errCount
)

var errDescriptions = [...]string{
	ErrException:           "异常",
	ErrNotFound:            "找不到记录",
	ErrModuleAlreadyExists: "模型已存在",
	ErrNotMigration:        "不支持表迁移",
}

var _ = [1]int{}[len(errDescriptions)-int(errCount)]

func (e errType) Error() string {
	if int(e) > len(errDescriptions) {
		return "未知错误"
	}

	return errDescriptions[e]
}
